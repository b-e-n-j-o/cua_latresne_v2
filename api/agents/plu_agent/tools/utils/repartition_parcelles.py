"""Matrice parcelle × couche (UF multi-feuilles) pour get_contexte_parcelle."""

from __future__ import annotations

import logging

from ...commune_context import q
from ...layer_catalog import LayerCatalog
from .catalog_bridge import extra_layers
from .db import db_query
from .fetch_layer import collapse_identical_llm_items, fetch_layer_rows, rows_to_llm_items
from .infos import fetch_infos_rows
from .parcel_ref_parse import official_label
from .prescriptions import fetch_prescriptions_rows
from .repartition_format import parcel_repartition_entry, thin_repartition_attrs
from .servitudes import fetch_servitudes_rows
from .zonage import fetch_zonage_reglement_rows, filter_zonage_rows

logger = logging.getLogger("plu_tools")


def _fetch_parcel_wkbs(db_config: dict, parcelles: list[dict]) -> list[tuple[str, bytes]]:
    idus = [p.get("idu") for p in parcelles if p.get("idu")]
    if not idus:
        return []
    rows = db_query(
        db_config,
        f"""
        SELECT idu, section, numero,
               ST_AsEWKB(ST_MakeValid(geom_2154)) AS geom_wkb
        FROM {q("parcelles")}
        WHERE idu = ANY(%s)
        """,
        (idus,),
    )
    by_idu = {r["idu"]: r for r in rows}
    ordered: list[tuple[str, bytes]] = []
    for parcel in parcelles:
        row = by_idu.get(parcel.get("idu"))
        if not row or not row.get("geom_wkb"):
            continue
        ordered.append((
            official_label(
                parcel.get("section") or row.get("section") or "",
                parcel.get("numero") or row.get("numero") or "",
            ),
            row["geom_wkb"],
        ))
    return ordered


def _uf_extra_titles(extra_block: dict) -> dict[str, str]:
    titles: dict[str, str] = {}
    for items in (extra_block.get("couches_supplementaires") or {}).values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            lid = item.get("layer_id")
            if lid and lid not in titles:
                titles[str(lid)] = str(item.get("couche") or lid)
    return titles


def _zonage_elements(db_config: dict, geom_wkb: bytes) -> list[dict]:
    rows = filter_zonage_rows(fetch_zonage_reglement_rows(db_config, geom_wkb))
    return [
        {
            "code_zone": r.get("code_zone"),
            "pct_parcelle_couverte": r.get("pct_parcelle_couverte"),
        }
        for r in rows
        if r.get("code_zone")
    ]


def _gpu_libelle_elements(rows: list[dict], label_key: str) -> list[dict]:
    items: list[dict] = []
    for row in rows:
        label = row.get(label_key) or row.get("libelle") or row.get("nom_servitude")
        if not label:
            continue
        el: dict = {"libelle": label}
        if row.get("pct_parcelle_couverte") is not None:
            el["pct_parcelle_couverte"] = row["pct_parcelle_couverte"]
        items.append(el)
    return collapse_identical_llm_items(items)


def _extra_elements(db_config: dict, geom_wkb: bytes, spec) -> list[dict]:
    rows = fetch_layer_rows(db_config, geom_wkb, spec, with_geojson=False)
    items = rows_to_llm_items(rows, spec)
    if spec.collapse_identical:
        items = collapse_identical_llm_items(items)
    thinned: list[dict] = []
    for item in items:
        thin = thin_repartition_attrs(item)
        if item.get("nb_entites") and "nb_entites" not in thin:
            thin["nb_entites"] = item["nb_entites"]
        thinned.append(thin)
    return thinned


def build_repartition_parcelles(
    db_config: dict,
    *,
    parcelles: list[dict],
    uf_context: dict,
    catalog: LayerCatalog | None = None,
) -> list[dict] | None:
    """None si 0–1 parcelle. Sinon une entrée par couche qui touche déjà l'UF."""
    if len(parcelles or []) <= 1:
        return None

    wkbs = _fetch_parcel_wkbs(db_config, parcelles)
    if len(wkbs) <= 1:
        return None

    layers: list[tuple[str, str, object]] = []
    if uf_context.get("zones"):
        layers.append(("zonage", "Zonage PLU", "zonage"))
    if (uf_context.get("prescriptions_count") or 0) > 0:
        layers.append(("prescriptions", "Prescriptions PLU", "prescriptions"))
    if (uf_context.get("servitudes_count") or 0) > 0:
        layers.append(("servitudes", "Servitudes", "servitudes"))
    if (uf_context.get("informations_count") or 0) > 0:
        layers.append(("informations", "Informations PLU", "informations"))

    extra_titles = _uf_extra_titles(uf_context)
    extra_specs = {
        spec.id: spec
        for spec in extra_layers(catalog, context_llm=True)
        if spec.id in extra_titles
    }
    for lid, title in extra_titles.items():
        spec = extra_specs.get(lid)
        if spec:
            layers.append((lid, title, spec))

    if not layers:
        return None

    matrix: list[dict] = []
    for layer_id, title, kind in layers:
        parcel_rows: list[dict] = []
        for official, geom_wkb in wkbs:
            try:
                if kind == "zonage":
                    elements = _zonage_elements(db_config, geom_wkb)
                elif kind == "prescriptions":
                    by_kind = fetch_prescriptions_rows(db_config, geom_wkb, with_geojson=False)
                    elements = _gpu_libelle_elements(
                        [r for rows in by_kind.values() for r in rows],
                        "libelle",
                    )
                elif kind == "servitudes":
                    elements = _gpu_libelle_elements(
                        fetch_servitudes_rows(db_config, geom_wkb, with_geojson=False),
                        "nom_servitude",
                    )
                elif kind == "informations":
                    by_kind = fetch_infos_rows(db_config, geom_wkb, with_geojson=False)
                    elements = _gpu_libelle_elements(
                        [r for rows in by_kind.values() for r in rows],
                        "libelle",
                    )
                else:
                    elements = _extra_elements(db_config, geom_wkb, kind)
            except Exception as exc:
                logger.warning(
                    "repartition_parcelles — %s / %s : %s",
                    layer_id,
                    official,
                    exc,
                )
                elements = []
            parcel_rows.append(parcel_repartition_entry(official, elements))
        matrix.append({
            "layer_id": layer_id,
            "couche": title,
            "parcelles": parcel_rows,
        })

    logger.info(
        "repartition_parcelles — %d parcelle(s) × %d couche(s)",
        len(wkbs),
        len(matrix),
    )
    return matrix
