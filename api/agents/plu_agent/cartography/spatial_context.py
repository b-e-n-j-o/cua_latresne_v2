"""
Orchestration contexte LLM + carto selon profile.catalog (JSON).

Appelé par contexte_parcelle.py et carto.py lorsque le profil commune est actif.
"""

from __future__ import annotations

import json
import logging

from ..commune_context import q
from ..layer_catalog import LayerCatalog
from ..tools.utils import (
    MIN_PARCEL_INTERSECTION_M2,
    build_map_infos,
    build_map_prescriptions,
    build_map_servitudes,
    fetch_infos_rows,
    fetch_prescriptions_rows,
    fetch_servitudes_rows,
    get_infos,
    get_prescriptions,
    get_servitudes,
    get_zonage_et_reglements,
    resolve_unite_fonciere,
)
from ..tools.utils.catalog_bridge import (
    active_catalog,
    group_enabled,
    prescription_config,
)
from ..tools.utils.fetch_layer import fetch_extra_layers_carto, fetch_extra_layers_llm

logger = logging.getLogger("plu_tools")


def _parcel_kwargs(
    parcelles, idus, section, numero, idu,
) -> dict:
    return {
        "parcelles": parcelles,
        "idus": idus,
        "section": section,
        "numero": numero,
        "idu": idu,
    }


def build_contexte_from_catalog(
    db_config: dict,
    catalog: LayerCatalog | None = None,
    *,
    parcelles=None,
    idus=None,
    section=None,
    numero=None,
    idu=None,
    buffer_m: float = 0.0,
) -> dict:
    cat = catalog or active_catalog()
    kw = _parcel_kwargs(parcelles, idus, section, numero, idu)

    zones: list = []
    zones_count = 0
    zonage_meta: dict = {}

    if group_enabled(cat, "zonage"):
        zonage = get_zonage_et_reglements(db_config, **kw)
        if zonage.get("error"):
            return _empty_error(zonage["error"])
        zones = zonage.get("zones", [])
        zones_count = zonage.get("count", 0)
        zonage_meta = zonage
    else:
        resolved = resolve_unite_fonciere(db_config, **kw)
        if resolved.get("error"):
            return _empty_error(resolved["error"])
        zonage_meta = resolved

    presc = _empty_prescriptions()
    if group_enabled(cat, "prescriptions") and prescription_config(cat):
        presc = get_prescriptions(db_config, buffer_m=buffer_m, **kw)
    serv = {"servitudes": [], "count": 0, "error": None}
    if group_enabled(cat, "servitudes"):
        serv = get_servitudes(db_config, buffer_m=buffer_m, **kw)
    infos = _empty_infos()
    if group_enabled(cat, "informations"):
        infos = get_infos(db_config, buffer_m=buffer_m, **kw)

    presc_error = presc.get("error")
    serv_error = serv.get("error")
    infos_error = infos.get("error")

    extra_block = {"couches_supplementaires": {}, "couches_supplementaires_count": 0}
    resolved = resolve_unite_fonciere(db_config, **kw)
    if not resolved.get("error"):
        extra_block = fetch_extra_layers_llm(
            db_config, resolved["geom_wkb"], catalog=cat
        )

    infos_block = _infos_block(infos, infos_error)

    return {
        "zones": zones,
        "zones_count": zones_count,
        "surfaciques": presc.get("surfaciques", []) if not presc_error else [],
        "lineaires": presc.get("lineaires", []) if not presc_error else [],
        "ponctuelles": presc.get("ponctuelles", []) if not presc_error else [],
        "count_surfaciques": presc.get("count_surfaciques", 0) if not presc_error else 0,
        "count_lineaires": presc.get("count_lineaires", 0) if not presc_error else 0,
        "count_ponctuelles": presc.get("count_ponctuelles", 0) if not presc_error else 0,
        "prescriptions_count": presc.get("count", 0) if not presc_error else 0,
        "prescriptions_error": presc_error,
        "servitudes": serv.get("servitudes", []) if not serv_error else [],
        "servitudes_count": serv.get("count", 0) if not serv_error else 0,
        "servitudes_error": serv_error,
        "informations": infos_block,
        "informations_count": infos_block["count"],
        "informations_error": infos_error,
        **extra_block,
        "parcelles": (
            zonage_meta.get("parcelles")
            or presc.get("parcelles")
            or serv.get("parcelles")
            or infos.get("parcelles")
            or resolved.get("parcelles")
            or []
        ),
        "nb_parcelles": (
            zonage_meta.get("nb_parcelles")
            or presc.get("nb_parcelles")
            or serv.get("nb_parcelles")
            or infos.get("nb_parcelles")
            or resolved.get("nb_parcelles")
        ),
        "superficie_unite_m2": (
            zonage_meta.get("superficie_unite_m2")
            or resolved.get("superficie_m2")
        ),
        "error": None,
    }


def _empty_error(error: str) -> dict:
    return {
        "zones": [],
        "zones_count": 0,
        "surfaciques": [],
        "lineaires": [],
        "ponctuelles": [],
        "prescriptions_count": 0,
        "servitudes": [],
        "servitudes_count": 0,
        "informations": _empty_infos(),
        "informations_count": 0,
        "couches_supplementaires": {},
        "couches_supplementaires_count": 0,
        "error": error,
    }


def _empty_prescriptions() -> dict:
    return {
        "surfaciques": [],
        "lineaires": [],
        "ponctuelles": [],
        "count": 0,
        "error": None,
    }


def _empty_infos() -> dict:
    return {
        "surfaciques": [],
        "lineaires": [],
        "ponctuelles": [],
        "count": 0,
        "count_surfaciques": 0,
        "count_lineaires": 0,
        "count_ponctuelles": 0,
    }


def _infos_block(infos: dict, infos_error: str | None) -> dict:
    if infos_error:
        return _empty_infos()
    return {
        "surfaciques": infos.get("surfaciques", []),
        "lineaires": infos.get("lineaires", []),
        "ponctuelles": infos.get("ponctuelles", []),
        "count": infos.get("count", 0),
        "count_surfaciques": infos.get("count_surfaciques", 0),
        "count_lineaires": infos.get("count_lineaires", 0),
        "count_ponctuelles": infos.get("count_ponctuelles", 0),
    }


def build_carto_from_catalog(
    db_config: dict,
    catalog: LayerCatalog | None = None,
    *,
    parcelles=None,
    idus=None,
    section=None,
    numero=None,
    idu=None,
    buffer_m: float = 100.0,
    parcelle_layers: dict | None = None,
    zone_features: list | None = None,
) -> dict:
    """
    Complète le payload carto (prescriptions, servitudes, infos, extra).
    parcelle_layers / zone_features construits par carto.py (zonage buffer).
    """
    from .carto import _build_parcelle_layers, _query, _zone_color, _geometry_has_area

    cat = catalog or active_catalog()
    kw = _parcel_kwargs(parcelles, idus, section, numero, idu)

    resolved = resolve_unite_fonciere(db_config, **kw)
    if resolved.get("error"):
        return {"error": resolved["error"]}

    geom_wkb = resolved["geom_wkb"]
    if parcelle_layers is None:
        parcelle_layers = _build_parcelle_layers(resolved)

    if zone_features is None and group_enabled(cat, "zonage"):
        zone_layer = cat.get("zonage")
        buf = float(
            (zone_layer.buffer_m if zone_layer and zone_layer.buffer_m is not None else buffer_m)
        )
        min_m2 = MIN_PARCEL_INTERSECTION_M2
        sql_zones = f"""
            WITH cible AS (
                SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
            ),
            cible_buffer AS (
                SELECT ST_MakeValid(ST_Buffer(geom, %s)) AS geom FROM cible
            ),
            zones_hits AS (
                SELECT
                    z.zonage_reglement AS code_zone,
                    z.libelle,
                    z.libelong,
                    z.typezone,
                    ST_MakeValid(z.geom_2154) AS geom_zone,
                    c.geom AS geom_parcelle,
                    cb.geom AS geom_buffer
                FROM {q("zonage_plu")} z
                CROSS JOIN cible c
                CROSS JOIN cible_buffer cb
                WHERE ST_Intersects(ST_MakeValid(z.geom_2154), cb.geom)
            )
            SELECT DISTINCT ON (code_zone)
                code_zone, libelle, libelong, typezone,
                ST_AsGeoJSON(
                    ST_Transform(
                        ST_Multi(
                            ST_CollectionExtract(
                                ST_Force2D(ST_Intersection(geom_zone, geom_buffer)),
                                3
                            )
                        ),
                        4326
                    )
                ) AS geojson_zone,
                ROUND(
                    (ST_Area(ST_Intersection(geom_zone, geom_parcelle))
                        / NULLIF(ST_Area(geom_parcelle), 0) * 100)::numeric,
                    1
                ) AS pct_parcelle_couverte
            FROM zones_hits
            WHERE ST_Area(ST_Intersection(geom_zone, geom_parcelle)) > {min_m2}
              AND NOT ST_IsEmpty(ST_Intersection(geom_zone, geom_buffer))
            ORDER BY code_zone,
                     ST_Area(ST_Intersection(geom_zone, geom_parcelle)) DESC;
        """
        rows_zones = _query(db_config, sql_zones, (geom_wkb, buf))
        zone_features = []
        for z in rows_zones:
            geom_z = z.get("geojson_zone")
            if not geom_z:
                continue
            geom_obj = json.loads(geom_z)
            if not _geometry_has_area(geom_obj):
                continue
            code = z["code_zone"]
            tz = z.get("typezone")
            zone_features.append({
                "type": "Feature",
                "geometry": geom_obj,
                "properties": {
                    "code_zone": code,
                    "libelle": z.get("libelle"),
                    "libelong": z.get("libelong"),
                    "typezone": tz,
                    "pct_parcelle_couverte": float(z["pct_parcelle_couverte"])
                    if z.get("pct_parcelle_couverte") is not None
                    else None,
                    "color": _zone_color(tz, code),
                },
            })
    elif zone_features is None:
        zone_features = []

    presc_map = {
        k: {"type": "FeatureCollection", "features": []}
        for k in ("surfaciques", "lineaires", "ponctuelles")
    }
    if group_enabled(cat, "prescriptions") and prescription_config(cat):
        presc_rows = fetch_prescriptions_rows(
            db_config, geom_wkb, with_geojson=True, strict_parcel=True
        )
        presc_map = build_map_prescriptions(presc_rows)

    serv_map = {"type": "FeatureCollection", "features": []}
    if group_enabled(cat, "servitudes"):
        serv_rows = fetch_servitudes_rows(
            db_config, geom_wkb, with_geojson=True, strict_parcel=True
        )
        serv_map = build_map_servitudes(serv_rows)

    infos_map = {
        k: {"type": "FeatureCollection", "features": []}
        for k in ("surfaciques", "lineaires", "ponctuelles")
    }
    if group_enabled(cat, "informations"):
        infos_rows = fetch_infos_rows(
            db_config, geom_wkb, with_geojson=True, strict_parcel=True
        )
        infos_map = build_map_infos(infos_rows)

    extra_carto = fetch_extra_layers_carto(
        db_config, geom_wkb, catalog=cat, buffer_m=buffer_m
    )

    return {
        "parcelle": parcelle_layers["parcelle"],
        "parcelle_union": parcelle_layers.get("parcelle_union"),
        "zones": {"type": "FeatureCollection", "features": zone_features},
        "prescriptions": presc_map,
        "servitudes": serv_map,
        "informations": infos_map,
        "extra": extra_carto,
        "error": None,
    }
