import csv
import os
from typing import List

import requests
from fastapi import APIRouter, HTTPException
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

def wfs_get(params):
    r = requests.get(WFS_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

# ------------------------------------------------------------
# Endpoints
# ------------------------------------------------------------

@router.get("/geometrie")
def parcelle_geometrie(
    section: str,
    numero: str,
    commune: str = "LATRESNE"
):
    """Retourne la géométrie d'une parcelle (sans voisins)."""
    commune = commune.upper().strip()
    section = section.upper().strip()
    numero = numero.zfill(4)

    if commune not in COMMUNE_TO_INSEE:
        raise HTTPException(404, f"Commune inconnue : {commune}")

    insee = COMMUNE_TO_INSEE[commune]

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
    geom_2154 = shape(feature["geometry"])
    geom_4326 = transform(to_4326, geom_2154)

    return {
        "type": "Feature",
        "geometry": mapping(geom_4326),
        "properties": {
            "section": feature["properties"].get("section", ""),
            "numero": feature["properties"].get("numero", ""),
            "insee": insee,
            "commune": commune.title()
        }
    }

@router.post("/uf-geometrie")
def uf_geometrie(payload: UFRequest):
    """Retourne la géométrie de l'union d'une unité foncière (sans voisins)."""
    commune = payload.commune.upper().strip()

    if commune not in COMMUNE_TO_INSEE:
        raise HTTPException(404, f"Commune inconnue : {commune}")

    if not payload.parcelles:
        raise HTTPException(400, "Aucune parcelle fournie pour l'unité foncière")

    if len(payload.parcelles) > 5:
        raise HTTPException(400, "Une unité foncière ne peut pas dépasser 5 parcelles.")

    insee = COMMUNE_TO_INSEE[commune]
    uf_geoms = []
    uf_keys = set()

    for p in payload.parcelles:
        section = p.section.upper().strip()
        numero = p.numero.zfill(4)
        key = (section, numero)
        
        if key in uf_keys:
            continue

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
            raise HTTPException(
                404,
                f"Parcelle introuvable pour l'UF : {commune} {section} {numero}"
            )

        geom = shape(target["features"][0]["geometry"])
        uf_geoms.append(geom)
        uf_keys.add(key)

    union_geom = unary_union(uf_geoms)

    if union_geom.geom_type != "Polygon":
        raise HTTPException(
            400,
            "Les parcelles de l'unité foncière ne sont pas toutes contiguës "
            "et forment plusieurs polygones distincts."
        )

    union_geom_4326 = transform(to_4326, union_geom)

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
