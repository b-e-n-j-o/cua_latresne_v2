"""
Utilitaires spatiaux PLU (hors déclarations Gemini).

Couches : zonage, prescriptions, servitudes, informations.
Chaque module expose fetch_* / build_map_* et get_* pour le contexte LLM.
"""

from .infos import build_map_infos, fetch_infos_rows, get_infos
from .parcel_geom import (
    normalize_parcel_refs,
    parcel_tool_properties,
    parcelles_refs_to_json,
    refs_from_session,
    resolve_unite_fonciere,
)
from .prescriptions import (
    build_map_prescriptions,
    fetch_prescriptions_rows,
    get_prescriptions,
)
from .servitudes import build_map_servitudes, fetch_servitudes_rows, get_servitudes
from .zonage import (
    MIN_PARCEL_INTERSECTION_M2,
    fetch_zonage_reglement_rows,
    filter_zonage_rows,
    get_zonage_et_reglements,
)

__all__ = [
    "MIN_PARCEL_INTERSECTION_M2",
    "normalize_parcel_refs",
    "parcel_tool_properties",
    "parcelles_refs_to_json",
    "refs_from_session",
    "resolve_unite_fonciere",
    "get_zonage_et_reglements",
    "fetch_zonage_reglement_rows",
    "filter_zonage_rows",
    "get_prescriptions",
    "fetch_prescriptions_rows",
    "build_map_prescriptions",
    "get_servitudes",
    "fetch_servitudes_rows",
    "build_map_servitudes",
    "get_infos",
    "fetch_infos_rows",
    "build_map_infos",
]
