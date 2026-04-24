"""
Endpoint pour récupérer la géométrie d'une parcelle cadastrale
via WFS IGN.
"""

import csv
import json
import os
from typing import List

import psycopg2
import requests
from fastapi import APIRouter, HTTPException

from .ssl_utils import ssl_verify_for_requests
from pydantic import BaseModel
from shapely.geometry import shape
from shapely.ops import transform, unary_union
from shapely.geometry import mapping
import pyproj

router = APIRouter(prefix="/parcelle", tags=["Cadastre"])

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

CSV_COMMUNES = os.path.join("CONFIG", "v_commune_2025.csv")
WFS_URL = "https://data.geopf.fr/wfs"
CAD_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

proj_2154 = pyproj.CRS("EPSG:2154")
proj_4326 = pyproj.CRS("EPSG:4326")
to_4326 = pyproj.Transformer.from_crs(
    proj_2154, proj_4326, always_xy=True
).transform

# ------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------

class UFParcelle(BaseModel):
    section: str
    numero: str

class UFRequest(BaseModel):
    parcelles: List[UFParcelle]
    commune: str = "LATRESNE"
    code_insee: str | None = None  # Code INSEE optionnel (prioritaire sur commune)

# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------

def load_commune_to_insee():
    mapping_dict = {}
    with open(CSV_COMMUNES, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mapping_dict[row["LIBELLE"].upper()] = row["COM"]
    return mapping_dict

COMMUNE_TO_INSEE = load_commune_to_insee()

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"


def get_db_connection():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )


def wfs_get(params):
    r = requests.get(
        WFS_URL,
        params=params,
        timeout=20,
        verify=ssl_verify_for_requests(),
    )
    r.raise_for_status()
    return r.json()


def resolve_insee_and_commune(code_insee: str | None, commune: str) -> tuple[str, str]:
    """Résout (insee, libellé commune) depuis code_insee prioritaire ou nom de commune."""
    if code_insee:
        insee = code_insee.strip()
        commune_name = None
        for nom, code in COMMUNE_TO_INSEE.items():
            if code == insee:
                commune_name = nom.title()
                break
        if not commune_name:
            commune_name = commune.title()
        return insee, commune_name

    commune_upper = commune.upper().strip()
    if commune_upper not in COMMUNE_TO_INSEE:
        raise HTTPException(404, f"Commune inconnue : {commune}")
    return COMMUNE_TO_INSEE[commune_upper], commune.title()

# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------

@router.get("/geometrie")
def parcelle_geometrie(
    section: str,
    numero: str,
    commune: str = "LATRESNE",
    code_insee: str | None = None  # Code INSEE optionnel (prioritaire sur commune)
):
    """Retourne la géométrie d'une parcelle (sans voisins) avec sa contenance."""
    section = section.upper().strip()
    numero = numero.zfill(4)
    insee, commune_name = resolve_insee_and_commune(code_insee, commune)

    cql = (
        f"code_insee='{insee}' AND "
        f"section='{section}' AND "
        f"numero='{numero}'"
    )

    target = wfs_get({
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": CAD_LAYER,
        "outputFormat": "application/json",
        "srsName": "EPSG:2154",
        "cql_filter": cql
    })

    if not target["features"]:
        raise HTTPException(404, "Parcelle introuvable")

    feature = target["features"][0]
    props = feature["properties"]
    geom_2154 = shape(feature["geometry"])
    geom_4326 = transform(to_4326, geom_2154)

    # Récupérer la contenance depuis le WFS (peut avoir différents noms)
    contenance = None
    for key in props.keys():
        if 'contenance' in key.lower() or 'contain' in key.lower():
            val = props.get(key)
            if val is not None:
                try:
                    # Convertir en nombre si c'est une chaîne
                    if isinstance(val, str):
                        val = float(val.replace(',', '.').replace(' ', ''))
                    contenance = float(val)
                    break
                except (ValueError, TypeError):
                    continue

    return {
        "type": "Feature",
        "geometry": mapping(geom_4326),
        "properties": {
            "section": props.get("section", ""),
            "numero": props.get("numero", ""),
            "insee": insee,
            "commune": commune_name,
            "contenance": contenance  # Surface cadastrale indicative en m²
        }
    }


@router.get("/et-voisins")
def parcelle_et_voisins(
    section: str,
    numero: str,
    commune: str = "LATRESNE",
    code_insee: str | None = None,
):
    """
    Retourne la parcelle cible et ses voisines depuis PostGIS (latresne.parcelles_latresne).
    """
    section_norm = section.upper().strip()
    numero_norm = numero.zfill(4)
    insee, commune_name = resolve_insee_and_commune(code_insee, commune)

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            WITH target AS (
                SELECT p.geom_2154
                FROM latresne.parcelles_latresne p
                WHERE p.code_insee = %s
                  AND UPPER(TRIM(p.section)) = %s
                  AND LPAD(TRIM(p.numero), 4, '0') = %s
                  AND p.geom_2154 IS NOT NULL
                LIMIT 1
            ),
            selected AS (
                SELECT
                    p.section,
                    p.numero,
                    p.nom_com,
                    p.code_insee,
                    p.contenance,
                    p.geom_2154,
                    (UPPER(TRIM(p.section)) = %s AND LPAD(TRIM(p.numero), 4, '0') = %s) AS is_target
                FROM latresne.parcelles_latresne p, target t
                WHERE p.code_insee = %s
                  AND p.geom_2154 IS NOT NULL
                  AND (
                      (UPPER(TRIM(p.section)) = %s AND LPAD(TRIM(p.numero), 4, '0') = %s)
                      OR ST_Touches(p.geom_2154, t.geom_2154)
                  )
            )
            SELECT
                section,
                numero,
                nom_com,
                code_insee,
                contenance,
                is_target,
                ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geom_json
            FROM selected
            ORDER BY is_target DESC, UPPER(TRIM(section)), LPAD(TRIM(numero), 4, '0')
            """,
            (
                insee,
                section_norm,
                numero_norm,
                section_norm,
                numero_norm,
                insee,
                section_norm,
                numero_norm,
            ),
        )
        rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(500, f"Erreur lecture base parcelles_latresne: {exc}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if not rows:
        raise HTTPException(
            404,
            f"Parcelle introuvable en base : {commune_name} {section_norm} {numero_norm} (INSEE: {insee})",
        )

    features = []
    for row in rows:
        sec, num, nom_com, code, contenance, is_target, geom_json = row
        if not geom_json:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": json.loads(geom_json),
                "properties": {
                    "section": (sec or "").strip().upper(),
                    "numero": str(num or "").strip().zfill(4),
                    "commune": (nom_com or commune_name),
                    "insee": (code or insee),
                    "contenance": float(contenance) if contenance is not None else None,
                    "is_target": bool(is_target),
                },
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
    }

@router.post("/uf-geometrie")
def uf_geometrie(payload: UFRequest):
    """Retourne la géométrie de l'union d'une unité foncière (sans voisins)."""
    if not payload.parcelles:
        raise HTTPException(400, "Aucune parcelle fournie pour l'unité foncière")

    if len(payload.parcelles) > 20:
        raise HTTPException(400, "Une unité foncière ne peut pas dépasser 20 parcelles.")

    insee, commune = resolve_insee_and_commune(payload.code_insee, payload.commune)
    print(f"[uf-geometrie] Utilisation de la base locale pour INSEE={insee}, commune={commune}")
    
    print(f"[uf-geometrie] Traitement de {len(payload.parcelles)} parcelles avec INSEE={insee}")
    unique_parcelles = []
    seen = set()
    for p in payload.parcelles:
        section = p.section.upper().strip()
        numero = p.numero.zfill(4)
        key = (section, numero)
        if key in seen:
            continue
        seen.add(key)
        unique_parcelles.append(key)

    if not unique_parcelles:
        raise HTTPException(400, "Aucune parcelle valide fournie pour l'unité foncière")

    values_sql = ", ".join(["(%s, %s)"] * len(unique_parcelles))
    sql_params = []
    for section, numero in unique_parcelles:
        sql_params.extend([section, numero])
    sql_params.append(insee)

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            WITH requested(section, numero) AS (
                VALUES {values_sql}
            ),
            matched AS (
                SELECT
                    r.section,
                    r.numero,
                    p.geom_2154
                FROM requested r
                JOIN latresne.parcelles_latresne p
                  ON UPPER(TRIM(p.section)) = r.section
                 AND LPAD(TRIM(p.numero), 4, '0') = r.numero
                 AND p.code_insee = %s
                 AND p.geom_2154 IS NOT NULL
            )
            SELECT
                ST_AsGeoJSON(ST_Transform(ST_UnaryUnion(ST_Collect(geom_2154)), 4326)) AS union_geojson,
                ARRAY_AGG(DISTINCT section || '-' || numero) AS matched_keys
            FROM matched
            """,
            tuple(sql_params),
        )
        row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(500, f"Erreur lecture base parcelles_latresne: {exc}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    union_geojson = row[0] if row else None
    matched_keys = set(row[1] or []) if row else set()
    requested_keys = {f"{section}-{numero}" for section, numero in unique_parcelles}
    missing_keys = sorted(requested_keys - matched_keys)

    if missing_keys:
        missing_readable = ", ".join(key.replace("-", " ") for key in missing_keys[:5])
        if len(missing_keys) > 5:
            missing_readable += ", ..."
        raise HTTPException(
            404,
            f"Parcelle(s) introuvable(s) en base pour l'UF ({insee}) : {missing_readable}",
        )

    if not union_geojson:
        raise HTTPException(404, f"Aucune géométrie trouvée en base pour l'UF (INSEE: {insee})")

    union_geom_4326 = shape(json.loads(union_geojson))

    return {
        "type": "Feature",
        "geometry": mapping(union_geom_4326),
        "properties": {
            "commune": commune.title(),
            "insee": insee,
            "parcelles": [
                {
                    "section": p.section.upper().strip(),
                    "numero": p.numero.zfill(4)
                }
                for p in payload.parcelles
            ]
        }
    }
