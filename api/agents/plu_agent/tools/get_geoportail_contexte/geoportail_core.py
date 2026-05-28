"""
Coeur métier unifié du fetch Géoportail live.

Ce module regroupe la logique auparavant répartie dans:
- parcelle_wfs.py
- wfs_client.py
- spatial_filter.py
- layer_builder.py

Résolution d'une unité foncière via WFS Parcellaire Express (IGN).

Équivalent live de `resolve_unite_fonciere` (parcel_geom.py) sans BDD :
  - normalise les refs (parcelles / idus / section+numero / idu)
  - fetch chaque parcelle via CQL_FILTER sur le WFS Parcellaire Express
  - union Shapely + contrôle de contiguïté (rejet si N parts > 1 pour N refs > 1)
  - retourne geom_2154 (Shapely), superficie, métadonnées parcelles

Reproduit les mêmes règles de validation que l'ancien code :
  - parcelles introuvables -> erreur explicite
  - parcelles multiples non contiguës -> erreur

  Fetch des couches en parallèle :
  - build_layer_items pour chaque couche du catalogue
  - orchestration parallèle (ThreadPoolExecutor)
  - retourne un payload structuré pour le LLM

  Construction du payload par couche : fetch WFS -> filtre strict -> attributs whitelistés.
  Pour chaque couche du catalogue :
    1. fetch_layer sur la bbox de l'unité foncière (+ buffer si configuré)
    2. filtre spatial strict (ou métriques si zonage)
    3. projection des seuls attributs déclarés dans le catalogue (liste blanche)

  Le résultat est une liste de dicts prêts pour le LLM, sans géométrie.
  Les géométries (pour la carte) sont gérées séparément (cf. note en bas).

"""

from __future__ import annotations

import logging
import re
import time
from io import BytesIO

import geopandas as gpd
import requests
from shapely import make_valid
from shapely.geometry import MultiPolygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

from .catalog import (
    LAYERS,
    MIN_PARCEL_INTERSECTION_M2,
    POINT_BUFFER_TOLERANCE_M,
    SRS_FETCH,
    SRS_METRIC,
    servitude_label,
)

logger = logging.getLogger("geoportail")

WFS_URL = "https://data.geopf.fr/wfs/ows"
LAYER_PARCELLE = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

PARCELLE_TIMEOUT = 45
HITS_TIMEOUT = 30
FETCH_TIMEOUT = 120
DEFAULT_MAX_FEATURES = 5000
TXT_MAX = 800


def ensure_valid(geom: BaseGeometry | None) -> BaseGeometry | None:
    if geom is None or geom.is_empty:
        return None
    if geom.is_valid:
        return geom
    fixed = make_valid(geom)
    return fixed if (fixed is not None and not fixed.is_empty) else None


def passes_strict_filter(
    entity_geom: BaseGeometry,
    parcel_geom: BaseGeometry,
    min_m2: float = MIN_PARCEL_INTERSECTION_M2,
) -> bool:
    entity_geom = ensure_valid(entity_geom)
    parcel_geom = ensure_valid(parcel_geom)
    if entity_geom is None or parcel_geom is None:
        return False

    gtype = entity_geom.geom_type
    if gtype in ("Point", "MultiPoint"):
        shrunk = parcel_geom.buffer(POINT_BUFFER_TOLERANCE_M)
        if shrunk.is_empty:
            shrunk = parcel_geom
        return shrunk.contains(entity_geom)

    if not entity_geom.intersects(parcel_geom):
        return False

    ix = ensure_valid(entity_geom.intersection(parcel_geom))
    if ix is None:
        return False

    if gtype in ("Polygon", "MultiPolygon"):
        return ix.area > min_m2
    if gtype in ("LineString", "MultiLineString", "LinearRing"):
        return ix.length > min_m2
    return ix.area > min_m2 or ix.length > min_m2


def intersection_metrics(entity_geom: BaseGeometry, parcel_geom: BaseGeometry) -> dict:
    entity_geom = ensure_valid(entity_geom)
    parcel_geom = ensure_valid(parcel_geom)
    if entity_geom is None or parcel_geom is None:
        return {"superficie_intersection_m2": 0.0, "pct_parcelle_couverte": 0.0}
    ix = ensure_valid(entity_geom.intersection(parcel_geom))
    if ix is None:
        return {"superficie_intersection_m2": 0.0, "pct_parcelle_couverte": 0.0}
    surf = ix.area
    parcel_area = parcel_geom.area
    pct = (surf / parcel_area * 100.0) if parcel_area > 0 else 0.0
    return {
        "superficie_intersection_m2": round(surf, 1),
        "pct_parcelle_couverte": round(pct, 1),
    }


def _idu_to_sn_fallback(idu: str) -> tuple[str | None, str | None, str | None]:
    i = str(idu or "").strip().upper()
    if len(i) < 10:
        return None, None, None
    insee = i[:5]
    numero = i[-4:]
    section_raw = i[5:-4]
    section = section_raw.lstrip("0") or section_raw
    return insee, section, numero


def normalize_parcel_refs(
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
) -> list[dict]:
    refs: list[dict] = []
    seen: set[str] = set()

    for p in parcelles or []:
        if not isinstance(p, dict):
            continue
        s = (p.get("section") or "").upper().strip()
        n = str(p.get("numero") or "").strip()
        if not s or not n:
            continue
        key = f"sn:{s}:{n.zfill(4)}"
        if key not in seen:
            seen.add(key)
            refs.append({"type": "sn", "section": s, "numero": n})

    for i in idus or []:
        i_norm = str(i).strip().upper()
        if not i_norm:
            continue
        key = f"idu:{i_norm}"
        if key not in seen:
            seen.add(key)
            refs.append({"type": "idu", "idu": i_norm})

    if section and numero:
        s = section.upper().strip()
        n = str(numero).strip()
        key = f"sn:{s}:{n.zfill(4)}"
        if key not in seen:
            seen.add(key)
            refs.append({"type": "sn", "section": s, "numero": n})

    if idu and not idus:
        i_norm = str(idu).strip().upper()
        key = f"idu:{i_norm}"
        if key not in seen:
            refs.append({"type": "idu", "idu": i_norm})

    return refs


def _read_wfs_gdf(params: dict, session: requests.Session | None, timeout: int) -> gpd.GeoDataFrame:
    getter = session.get if session else requests.get
    r = getter(WFS_URL, params=params, timeout=timeout)
    r.raise_for_status()
    gdf = gpd.read_file(BytesIO(r.content))
    if gdf is None or gdf.empty:
        return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)
    if gdf.crs is None:
        gdf.set_crs(SRS_FETCH, inplace=True)
    return gdf.to_crs(SRS_METRIC)


def _fetch_one_parcelle(
    ref: dict,
    insee: str | None,
    session: requests.Session | None = None,
) -> gpd.GeoDataFrame:
    def fetch_with_cql(cql: str, timeout: int = PARCELLE_TIMEOUT, retry: int = 2) -> gpd.GeoDataFrame:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": LAYER_PARCELLE,
            "outputFormat": "application/gml+xml; version=3.2",
            "CQL_FILTER": cql,
            "count": 10,
        }
        for k in range(retry):
            try:
                return _read_wfs_gdf(params, session, timeout=timeout)
            except Exception as e:
                if k < retry - 1:
                    time.sleep(2**k)
                    continue
                logger.warning("fetch parcelle CQL échec (%s): %s", cql, str(e)[:120])
                return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)

    if ref["type"] == "idu":
        idu_val = ref["idu"]
        gdf = fetch_with_cql(f"idu='{idu_val}'")
        if not gdf.empty:
            return gdf

        insee2, section2, numero2 = _idu_to_sn_fallback(idu_val)
        if insee2 and section2 and numero2:
            cql = f"code_insee='{insee2}' AND section='{section2}' AND numero='{numero2}'"
            gdf = fetch_with_cql(cql)
            if not gdf.empty:
                return gdf
            section3 = section2.rjust(3, "0")
            if section3 != section2:
                cql = f"code_insee='{insee2}' AND section='{section3}' AND numero='{numero2}'"
                gdf = fetch_with_cql(cql)
                if not gdf.empty:
                    return gdf
        return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)

    sec = ref["section"]
    num = ref["numero"].zfill(4)
    cql = f"section='{sec}' AND numero='{num}'"
    if insee:
        cql = f"code_insee='{insee}' AND {cql}"
    gdf = fetch_with_cql(cql)
    if not gdf.empty:
        return gdf
    sec_alt = sec.rjust(3, "0")
    if sec_alt != sec:
        cql_alt = f"section='{sec_alt}' AND numero='{num}'"
        if insee:
            cql_alt = f"code_insee='{insee}' AND {cql_alt}"
        return fetch_with_cql(cql_alt)
    return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)


def resolve_unite_fonciere(
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
    insee: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    refs = normalize_parcel_refs(parcelles, idus, section, numero, idu)
    if not refs:
        return {"error": "Fournir parcelles, idus, ou section+numero."}

    rows = []
    for ref in refs:
        gdf = _fetch_one_parcelle(ref, insee, session=session)
        if gdf.empty:
            label = ref.get("idu") or f"{ref.get('section')} {ref.get('numero')}"
            return {"error": f"Parcelle introuvable : {label}."}
        if len(gdf) > 1 and not insee:
            logger.warning(
                "Ref %s ambiguë (%d matchs) — fournir 'insee' pour désambiguïser.",
                ref, len(gdf),
            )
        row = gdf.iloc[0]
        rows.append(
            {
                "idu": row.get("idu"),
                "section": row.get("section"),
                "numero": row.get("numero"),
                "contenance": row.get("contenance"),
                "geom": ensure_valid(row.geometry),
            }
        )

    geoms = [r["geom"] for r in rows if r["geom"] is not None]
    if not geoms:
        return {"error": "Géométries de parcelles invalides."}
    union = ensure_valid(unary_union(geoms))
    if union is None:
        return {"error": "Impossible de construire l'unité foncière."}

    n_parts = len(union.geoms) if isinstance(union, MultiPolygon) else 1
    if len(refs) > 1 and n_parts > 1:
        return {
            "error": (
                f"Les {len(refs)} parcelles ne sont pas contiguës "
                f"({n_parts} parties disjointes après union)."
            )
        }

    parcelles_meta = [
        {
            "idu": r["idu"],
            "section": r["section"],
            "numero": r["numero"],
            "contenance": r["contenance"],
        }
        for r in rows
    ]
    return {
        "geom_2154": union,
        "superficie_m2": round(union.area, 1),
        "parcelles": parcelles_meta,
        "nb_parcelles": len(parcelles_meta),
        "error": None,
    }


def _bbox_param(bbox, srs: str = SRS_FETCH) -> str:
    return f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]},{srs}"


def _bbox_4326_from_geom(geom_2154: BaseGeometry, buffer_m: float = 0.0):
    g = geom_2154.buffer(buffer_m) if buffer_m and buffer_m > 0 else geom_2154
    gs = gpd.GeoSeries([g], crs=SRS_METRIC).to_crs(SRS_FETCH)
    return gs.iloc[0].bounds


def wfs_hits(
    typename: str,
    bbox,
    session: requests.Session | None = None,
    timeout: int = HITS_TIMEOUT,
    retry: int = 3,
) -> int:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": typename,
        "bbox": _bbox_param(bbox),
        "resultType": "hits",
    }
    getter = session.get if session else requests.get
    for k in range(retry):
        try:
            r = getter(WFS_URL, params=params, timeout=timeout)
            r.raise_for_status()
            m = re.search(r'numberMatched="(\d+)"', r.text) or re.search(r'numberOfFeatures="(\d+)"', r.text)
            return int(m.group(1)) if m else 0
        except Exception as e:
            if k < retry - 1:
                time.sleep(2**k)
                continue
            logger.warning("wfs_hits %s échec: %s", typename, str(e)[:120])
            return -1
    return -1


def _wfs_fetch_bbox(typename: str, bbox, session: requests.Session | None = None, timeout: int = FETCH_TIMEOUT) -> bytes:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": typename,
        "bbox": _bbox_param(bbox),
        "outputFormat": "application/gml+xml; version=3.2",
    }
    getter = session.get if session else requests.get
    r = getter(WFS_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.content


def _parse_gml(content: bytes) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(BytesIO(content))
    if gdf is None or gdf.empty:
        return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)
    if gdf.crs is None:
        gdf.set_crs(SRS_FETCH, inplace=True)
    return gdf.to_crs(SRS_METRIC)


def _subdivide(bbox):
    minx, miny, maxx, maxy = bbox
    mx, my = (minx + maxx) / 2.0, (miny + maxy) / 2.0
    return [
        (minx, miny, mx, my),
        (mx, miny, maxx, my),
        (minx, my, mx, maxy),
        (mx, my, maxx, maxy),
    ]


def fetch_layer(
    typename: str,
    bbox_4326,
    session: requests.Session | None = None,
    max_features: int = DEFAULT_MAX_FEATURES,
    _depth: int = 0,
    _seen_ids: set | None = None,
) -> gpd.GeoDataFrame:
    if _seen_ids is None:
        _seen_ids = set()

    n = wfs_hits(typename, bbox_4326, session=session)
    if n <= 0:
        return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)

    if n >= max_features and _depth < 6:
        parts = []
        for sub in _subdivide(bbox_4326):
            gdf_sub = fetch_layer(
                typename,
                sub,
                session=session,
                max_features=max_features,
                _depth=_depth + 1,
                _seen_ids=_seen_ids,
            )
            if not gdf_sub.empty:
                parts.append(gdf_sub)
        if not parts:
            return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)
        import pandas as pd

        return gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), geometry="geometry", crs=SRS_METRIC)

    try:
        content = _wfs_fetch_bbox(typename, bbox_4326, session=session)
        gdf = _parse_gml(content)
    except Exception as e:
        logger.warning("fetch_layer %s échec: %s", typename, str(e)[:120])
        return gpd.GeoDataFrame(geometry=[], crs=SRS_METRIC)

    if gdf.empty:
        return gdf

    id_col = "gml_id" if "gml_id" in gdf.columns else ("gid" if "gid" in gdf.columns else None)
    if id_col and _depth > 0:
        gdf = gdf[~gdf[id_col].astype(str).isin(_seen_ids)]
        _seen_ids.update(gdf[id_col].dropna().astype(str).tolist())
    return gdf


def _project_attributes(props: dict, attributes: list[str], group: str) -> dict:
    out = {a: props.get(a) for a in attributes}
    for txt_field in ("txt", "libelong"):
        if txt_field in out and out[txt_field]:
            s = str(out[txt_field]).strip()
            out[txt_field] = (s[:TXT_MAX] + "...") if len(s) > TXT_MAX else s
    if group == "servitudes":
        out["nom_servitude"] = servitude_label(props)
    return out


def build_layer_items(
    layer_key: str,
    parcel_geom_2154: BaseGeometry,
    session: requests.Session | None = None,
) -> dict:
    cfg = LAYERS[layer_key]
    group = cfg["group"]
    subgroup = cfg.get("subgroup")
    is_zonage = group == "zonage"

    try:
        bbox = _bbox_4326_from_geom(parcel_geom_2154, cfg.get("buffer_m", 0.0))
        gdf = fetch_layer(cfg["layer"], bbox, session=session)
    except Exception as e:
        if cfg.get("optional"):
            logger.warning("Couche %s optionnelle ignorée: %s", layer_key, str(e)[:120])
            return {"items": [], "count": 0, "group": group, "subgroup": subgroup, "error": None}
        return {"items": [], "count": 0, "group": group, "subgroup": subgroup, "error": str(e)}

    if gdf.empty:
        return {"items": [], "count": 0, "group": group, "subgroup": subgroup, "error": None}

    geom_col = gdf.geometry.name
    attributes = cfg["attributes"]
    items = []

    for _, row in gdf.iterrows():
        entity_geom = ensure_valid(row[geom_col])
        if entity_geom is None:
            continue
        if not passes_strict_filter(entity_geom, parcel_geom_2154):
            continue

        props = {k: row[k] for k in gdf.columns if k != geom_col}
        item = _project_attributes(props, attributes, group)

        if is_zonage:
            metrics = intersection_metrics(entity_geom, parcel_geom_2154)
            item["code_zone"] = item.get("libelle")
            item["superficie_intersection_m2"] = metrics["superficie_intersection_m2"]
            item["pct_parcelle_couverte"] = metrics["pct_parcelle_couverte"]

        items.append(item)

    if is_zonage:
        items.sort(key=lambda x: x.get("superficie_intersection_m2") or 0, reverse=True)
    return {"items": items, "count": len(items), "group": group, "subgroup": subgroup, "error": None}
