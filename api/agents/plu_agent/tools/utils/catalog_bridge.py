"""Pont entre LayerCatalog (JSON) et les modules fetch prescriptions / infos / servitudes."""

from __future__ import annotations

from ...commune_context import current_schema, get_current_profile_optional
from ...layer_catalog import LayerCatalog, LayerSpec, load_commune_catalog

# Familles gérées par les modules existants (socle GPU)
GPU_LAYER_GROUPS = frozenset({
    "parcelle",
    "zonage",
    "prescriptions",
    "servitudes",
    "informations",
})


def active_catalog() -> LayerCatalog:
    """Catalogue de la requête en cours, ou repli sur le schéma actif (tests CLI)."""
    profile = get_current_profile_optional()
    if profile:
        return profile.catalog
    return load_commune_catalog(current_schema())


def group_enabled(catalog: LayerCatalog, group: str) -> bool:
    return any(
        L.enabled and (L.context_llm or L.context_carto)
        for L in catalog.by_group(group)
    )


def prescription_config(catalog: LayerCatalog | None = None) -> dict[str, dict]:
    """Remplace PRESCRIPTION_CONFIG — clés surfaciques / lineaires / ponctuelles."""
    cat = catalog or active_catalog()
    out: dict[str, dict] = {}
    for L in cat.by_group("prescriptions"):
        if not L.subgroup:
            continue
        out[L.subgroup] = {
            "table": L.table,
            "kind": L.kind or L.subgroup,
            "color": L.color,
            "layer_id": L.id,
            "optional": L.optional,
            "context_llm": L.context_llm,
            "context_carto": L.context_carto,
        }
    return out


def infos_config(catalog: LayerCatalog | None = None) -> dict[str, dict]:
    cat = catalog or active_catalog()
    out: dict[str, dict] = {}
    for L in cat.by_group("informations"):
        if not L.subgroup:
            continue
        out[L.subgroup] = {
            "table": L.table,
            "kind": L.kind or L.subgroup,
            "color": L.color,
            "layer_id": L.id,
            "optional": L.optional,
            "context_llm": L.context_llm,
            "context_carto": L.context_carto,
        }
    return out


def servitudes_spec(catalog: LayerCatalog | None = None) -> LayerSpec | None:
    cat = catalog or active_catalog()
    layers = [L for L in cat.by_group("servitudes") if L.enabled]
    return layers[0] if layers else None


def extra_layers(
    catalog: LayerCatalog | None = None,
    *,
    context_llm: bool = False,
    context_carto: bool = False,
) -> list[LayerSpec]:
    """Couches hors socle GPU (pprt, …) déclarées dans le JSON commune."""
    cat = catalog or active_catalog()
    out: list[LayerSpec] = []
    for L in cat.enabled_layers():
        if L.group in GPU_LAYER_GROUPS:
            continue
        if context_llm and not L.context_llm:
            continue
        if context_carto and not L.context_carto:
            continue
        if not context_llm and not context_carto:
            continue
        out.append(L)
    return out
