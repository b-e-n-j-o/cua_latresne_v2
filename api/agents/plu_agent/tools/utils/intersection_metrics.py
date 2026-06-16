"""Métriques d'intersection surfacique (m² et % parcelle) pour le contexte LLM."""

from __future__ import annotations

SURFACIC_KINDS = frozenset({"surfacique", "surfaciques"})


def is_surfacic_layer(
    *,
    kind: str | None = None,
    subgroup: str | None = None,
    group: str | None = None,
) -> bool:
    """
    True pour les couches polygonales à enrichir (pas linéaires / ponctuelles).
    Les servitudes (assiettes SUP) sont traitées comme surfaciques.
    """
    if kind in SURFACIC_KINDS:
        return True
    if subgroup == "surfaciques":
        return True
    if group == "servitudes":
        return True
    return False


def surfacic_metrics_select_sql(entity_geom_expr: str) -> str:
    """
    Colonnes SQL calculées sur l'intersection avec l'unité foncière réelle (CTE cible),
    indépendamment d'un éventuel buffer de sélection.
    """
    parcel = "(SELECT geom FROM cible)"
    ix = f"ST_Intersection({entity_geom_expr}, {parcel})"
    return f"""
            ROUND(ST_Area({ix})::numeric, 1) AS superficie_intersection_m2,
            ROUND(
                (ST_Area({ix}) / NULLIF(ST_Area({parcel}), 0) * 100)::numeric,
                1
            ) AS pct_parcelle_couverte"""


def apply_surfacic_metrics_to_item(item: dict, row: dict) -> dict:
    surf = row.get("superficie_intersection_m2")
    pct = row.get("pct_parcelle_couverte")
    if surf is not None:
        item["superficie_intersection_m2"] = float(surf)
    if pct is not None:
        item["pct_parcelle_couverte"] = float(pct)
    return item
