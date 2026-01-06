import csv
import os
from typing import List

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shapely.geometry import shape, mapping, Point
from shapely.ops import transform, unary_union
import pyproj

router = APIRouter(prefix="/parcelle", tags=["Cadastre"])

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

CSV_COMMUNES = os.path.join("CONFIG", "v_commune_2025.csv")

WFS_URL = "https://data.geopf.fr/wfs"
CAD_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
BAN_URL = "https://api-adresse.data.gouv.fr/search/"

proj_2154 = pyproj.CRS("EPSG:2154")
proj_4326 = pyproj.CRS("EPSG:4326")
to_4326 = pyproj.Transformer.from_crs(
    proj_2154, proj_4326, always_xy=True
).transform
to_2154 = pyproj.Transformer.from_crs(
    proj_4326, proj_2154, always_xy=True
).transform

# ------------------------------------------------------------
# Pydantic models
# ------------------------------------------------------------


class UFParcelle(BaseModel):
    section: str
    numero: str


class UFRequest(BaseModel):
    commune: str
    parcelles: List[UFParcelle]
    buffer: int = 100


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


def get_parcelles_dans_buffer_2154(
    center_geom_2154,
    buffer_m: int,
    commune: str | None = None,
    insee: str | None = None,
    target_section: str | None = None,
    target_numero: str | None = None
):
    buffer_geom = center_geom_2154.buffer(buffer_m)
    minx, miny, maxx, maxy = buffer_geom.bounds

    voisins = wfs_get({
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": CAD_LAYER,
        "outputFormat": "application/json",
        "srsName": "EPSG:2154",
        "bbox": f"{minx},{miny},{maxx},{maxy},EPSG:2154"
    })

    features = []
    commune_formatted = commune.title() if commune else ""

    for f in voisins["features"]:
        geom_2154 = shape(f["geometry"])
        geom_4326 = transform(to_4326, geom_2154)

        is_target = (
            target_section is not None
            and target_numero is not None
            and f["properties"].get("section") == target_section
            and f["properties"].get("numero") == target_numero
        )

        features.append({
            "type": "Feature",
            "geometry": mapping(geom_4326),
            "properties": {
                "section": f["properties"].get("section", ""),
                "numero": f["properties"].get("numero", ""),
                "insee": insee,
                "commune": commune_formatted,
                "is_target": is_target
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }

# ------------------------------------------------------------
# Endpoint 1 : par parcelle
# ------------------------------------------------------------


@router.get("/et-voisins")
def parcelle_et_voisins(
    commune: str,
    section: str,
    numero: str,
    buffer: int = 100
):
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

    target_geom = shape(target["features"][0]["geometry"])
    center = target_geom.centroid

    return get_parcelles_dans_buffer_2154(
        center_geom_2154=center,
        buffer_m=buffer,
        commune=commune,
        insee=insee,
        target_section=section,
        target_numero=numero
    )


# ------------------------------------------------------------
# Endpoint 1bis : unité foncière (plusieurs parcelles) + voisins
# ------------------------------------------------------------


@router.post("/uf-et-voisins")
def uf_et_voisins(payload: UFRequest):
    """
    Construit une unité foncière à partir de plusieurs parcelles
    (même commune) et retourne toutes les parcelles dans un buffer
    autour du centroïde de l'union, avec marquage des parcelles de l'UF.
    """
    commune = payload.commune.upper().strip()

    if commune not in COMMUNE_TO_INSEE:
        raise HTTPException(404, f"Commune inconnue : {commune}")

    if not payload.parcelles:
        raise HTTPException(400, "Aucune parcelle fournie pour l'unité foncière")

    # Limitation : au maximum 5 parcelles dans une UF
    if len(payload.parcelles) > 5:
        raise HTTPException(
            400,
            "Une unité foncière ne peut pas dépasser 5 parcelles."
        )

    insee = COMMUNE_TO_INSEE[commune]

    # Récupérer chaque parcelle composant l'UF
    uf_geoms = []
    uf_keys = set()

    for p in payload.parcelles:
        section = p.section.upper().strip()
        numero = p.numero.zfill(4)

        key = (section, numero)
        if key in uf_keys:
            # Éviter de charger plusieurs fois la même parcelle
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

    # Union des géométries pour calculer un centroïde global
    union_geom = unary_union(uf_geoms)

    # Condition UF : les parcelles doivent être contiguës et former un seul polygone
    if union_geom.geom_type != "Polygon":
        raise HTTPException(
            400,
            "Les parcelles de l'unité foncière ne sont pas toutes contiguës "
            "et forment plusieurs polygones distincts."
        )

    center = union_geom.centroid

    # Récupérer toutes les parcelles dans le buffer autour du centroïde
    result = get_parcelles_dans_buffer_2154(
        center_geom_2154=center,
        buffer_m=payload.buffer,
        commune=commune,
        insee=insee
    )

    # Marquer toutes les parcelles composant l'UF comme cibles
    for f in result["features"]:
        props = f.get("properties", {})
        key = (
            str(props.get("section", "")).upper(),
            str(props.get("numero", "")).zfill(4)
        )
        props["is_target"] = key in uf_keys
        f["properties"] = props

    return result

# ------------------------------------------------------------
# Endpoint 2 : par adresse
# ------------------------------------------------------------

@router.get("/et-voisins-adresse")
def parcelle_et_voisins_adresse(
    adresse: str,
    buffer: int = 100
):
    r = requests.get(
        BAN_URL,
        params={"q": adresse, "limit": 1},
        timeout=10
    )
    r.raise_for_status()
    data = r.json()

    if not data["features"]:
        raise HTTPException(404, "Adresse introuvable")

    lon, lat = data["features"][0]["geometry"]["coordinates"]
    commune = data["features"][0]["properties"].get("city", "")
    insee = data["features"][0]["properties"].get("citycode", "")

    point_4326 = Point(lon, lat)
    point_2154 = transform(to_2154, point_4326)

    result = get_parcelles_dans_buffer_2154(
        center_geom_2154=point_2154,
        buffer_m=buffer,
        commune=commune,
        insee=insee
    )
    
    # Ajouter le point d'adresse dans la réponse
    result["address_point"] = [lon, lat]
    
    return result

# ------------------------------------------------------------
# Endpoint 3 : par coordonnées (clic sur la carte)
# ------------------------------------------------------------

@router.get("/par-coordonnees")
def parcelle_par_point(
    lon: float,
    lat: float,
    buffer: int = 100
):
    """
    Trouve la parcelle au point cliqué et retourne cette parcelle
    ainsi que ses voisines dans un buffer.
    
    Utilise une micro-bbox au lieu d'un filtre CQL INTERSECTS
    (le WFS IGN n'accepte pas INTERSECTS avec POINT en EPSG:4326).
    """
    # Créer une micro-bbox autour du point (±0.00001° ≈ 1m)
    delta = 0.00001
    bbox = f"{lon-delta},{lat-delta},{lon+delta},{lat+delta}"
    
    target = wfs_get({
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": CAD_LAYER,
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "bbox": f"{bbox},EPSG:4326"
    })
    
    if not target["features"]:
        raise HTTPException(404, "Aucune parcelle à ces coordonnées")
    
    # Si plusieurs parcelles dans la bbox, prendre celle qui contient vraiment le point
    point = Point(lon, lat)
    parcelle = None
    
    for f in target["features"]:
        geom = shape(f["geometry"])
        if geom.contains(point):
            parcelle = f
            break
    
    if not parcelle:
        parcelle = target["features"][0]  # Fallback : la plus proche
    
    props = parcelle["properties"]
    geom_4326 = shape(parcelle["geometry"])
    geom_2154 = transform(to_2154, geom_4326)
    
    return get_parcelles_dans_buffer_2154(
        center_geom_2154=geom_2154.centroid,
        buffer_m=buffer,
        commune=None,
        insee=props.get("code_insee"),
        target_section=props.get("section"),
        target_numero=props.get("numero")
    )
