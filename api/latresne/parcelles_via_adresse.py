import os
import json

import psycopg2
from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/latresne", tags=["latresne-parcelles"])

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


@router.get("/parcelles-via-adresse")
def get_parcelles_via_adresse(adresse: str = Query(..., min_length=3)):
    adresse_clean = (adresse or "").strip()
    if not adresse_clean:
        raise HTTPException(status_code=400, detail="Paramètre adresse requis.")

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            WITH ad AS (
                SELECT
                    a.id,
                    a.numero,
                    a.rep,
                    a.nom_voie,
                    a.code_postal,
                    a.code_insee,
                    a.nom_commune,
                    a.lon,
                    a.lat,
                    a.raw_json,
                    TRIM(
                        CONCAT_WS(' ',
                            NULLIF(TRIM(a.numero), ''),
                            NULLIF(TRIM(a.rep), ''),
                            NULLIF(TRIM(a.nom_voie), ''),
                            NULLIF(TRIM(a.code_postal), ''),
                            NULLIF(TRIM(a.nom_commune), '')
                        )
                    ) AS adresse_label
                FROM latresne.adresses_latresne a
                WHERE TRIM(
                        CONCAT_WS(' ',
                            NULLIF(TRIM(a.numero), ''),
                            NULLIF(TRIM(a.rep), ''),
                            NULLIF(TRIM(a.nom_voie), ''),
                            NULLIF(TRIM(a.code_postal), ''),
                            NULLIF(TRIM(a.nom_commune), '')
                        )
                    ) ILIKE %s
                ORDER BY
                    CASE
                        WHEN UPPER(
                            TRIM(
                                CONCAT_WS(' ',
                                    NULLIF(TRIM(a.numero), ''),
                                    NULLIF(TRIM(a.rep), ''),
                                    NULLIF(TRIM(a.nom_voie), ''),
                                    NULLIF(TRIM(a.code_postal), ''),
                                    NULLIF(TRIM(a.nom_commune), '')
                                )
                            )
                        ) = UPPER(%s) THEN 0
                        ELSE 1
                    END,
                    a.id
                LIMIT 1
            )
            SELECT
                p.section,
                p.numero,
                p.nom_com,
                p.code_insee,
                ST_AsGeoJSON(ST_Transform(p.geom_2154, 4326)) AS geom_json,
                ad.adresse_label,
                ad.lon,
                ad.lat
            FROM ad
            JOIN latresne.liens_adresses_parcelles lap
              ON lap.id_adresse = COALESCE(NULLIF(ad.raw_json->>'id_adr', ''), ad.id)
            JOIN latresne.parcelles_latresne p ON p.idu = lap.id_parcelle
            WHERE p.geom_2154 IS NOT NULL
            ORDER BY UPPER(TRIM(p.section)), LPAD(TRIM(p.numero), 4, '0');
            """,
            (f"%{adresse_clean}%", adresse_clean),
        )
        rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur recherche adresse: {exc}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail="Aucune parcelle trouvée pour cette adresse.")

    features = []
    parcelles = []
    address_point = None
    matched_adresse = None

    for row in rows:
        section, numero, commune, insee, geom_json, adresse_label, lon, lat = row
        if geom_json:
            features.append(
                {
                    "type": "Feature",
                    "geometry": json.loads(geom_json),
                    "properties": {
                        "section": (section or "").strip().upper(),
                        "numero": str(numero or "").strip().zfill(4),
                        "commune": commune,
                        "insee": insee,
                    },
                }
            )
        parcelles.append(
            {
                "section": (section or "").strip().upper(),
                "numero": str(numero or "").strip().zfill(4),
                "label": f"{(section or '').strip().upper()} {str(numero or '').strip().zfill(4)}".strip(),
            }
        )
        if matched_adresse is None and adresse_label:
            matched_adresse = adresse_label
        if address_point is None and lon is not None and lat is not None:
            address_point = [float(lon), float(lat)]

    # Déduplication simple des parcelles sur section+numero.
    uniq = {}
    for p in parcelles:
        key = (p["section"], p["numero"])
        uniq[key] = p

    return {
        "type": "FeatureCollection",
        "features": features,
        "address_point": address_point,
        "matched_address": matched_adresse,
        "parcelles": list(uniq.values()),
    }
