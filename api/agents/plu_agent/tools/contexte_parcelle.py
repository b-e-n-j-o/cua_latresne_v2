"""Tool get_contexte_parcelle — intersections PLU pour le LLM."""

import logging

from google.genai import types

from ..commune_context import get_current_profile_optional
from .utils import parcel_tool_properties
from ..cartography.spatial_context import build_contexte_from_catalog

logger = logging.getLogger("plu_tools")


def _legacy_contexte(
    db_config: dict,
    parcelles=None,
    idus=None,
    section=None,
    numero=None,
    idu=None,
    buffer_m: float = 0.0,
) -> dict:
    """Chemin historique si aucun profil actif (tests hors HTTP)."""
    from .utils import get_infos, get_prescriptions, get_servitudes, get_zonage_et_reglements

    zonage = get_zonage_et_reglements(
        db_config,
        parcelles=parcelles,
        idus=idus,
        section=section,
        numero=numero,
        idu=idu,
    )
    if zonage.get("error"):
        return {"error": zonage["error"], "zones": [], "zones_count": 0}

    presc = get_prescriptions(
        db_config, parcelles=parcelles, idus=idus,
        section=section, numero=numero, idu=idu, buffer_m=buffer_m,
    )
    serv = get_servitudes(
        db_config, parcelles=parcelles, idus=idus,
        section=section, numero=numero, idu=idu, buffer_m=buffer_m,
    )
    infos = get_infos(
        db_config, parcelles=parcelles, idus=idus,
        section=section, numero=numero, idu=idu, buffer_m=buffer_m,
    )
    infos_block = {
        "surfaciques": infos.get("surfaciques", []),
        "lineaires": infos.get("lineaires", []),
        "ponctuelles": infos.get("ponctuelles", []),
        "count": infos.get("count", 0),
        "count_surfaciques": infos.get("count_surfaciques", 0),
        "count_lineaires": infos.get("count_lineaires", 0),
        "count_ponctuelles": infos.get("count_ponctuelles", 0),
    }
    return {
        "zones": zonage.get("zones", []),
        "zones_count": zonage.get("count", 0),
        "surfaciques": presc.get("surfaciques", []),
        "lineaires": presc.get("lineaires", []),
        "ponctuelles": presc.get("ponctuelles", []),
        "prescriptions_count": presc.get("count", 0),
        "servitudes": serv.get("servitudes", []),
        "servitudes_count": serv.get("count", 0),
        "informations": infos_block,
        "informations_count": infos_block["count"],
        "couches_supplementaires": {},
        "couches_supplementaires_count": 0,
        "parcelles": zonage.get("parcelles") or [],
        "nb_parcelles": zonage.get("nb_parcelles"),
        "superficie_unite_m2": zonage.get("superficie_unite_m2"),
        "error": None,
    }


def get_contexte_parcelle(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    buffer_m: float = 0.0,
) -> dict:
    """
    Contexte spatial intersectant l'unité foncière (catalogue JSON par commune).
    Pas de géométries — la carte via GET /session/{id}/map.
    """
    try:
        profile = get_current_profile_optional()
        if profile:
            return build_contexte_from_catalog(
                db_config,
                profile.catalog,
                parcelles=parcelles,
                idus=idus,
                section=section,
                numero=numero,
                idu=idu,
                buffer_m=buffer_m,
            )
        return _legacy_contexte(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
            buffer_m=buffer_m,
        )
    except Exception as e:
        return {
            "zones": [],
            "zones_count": 0,
            "couches_supplementaires": {},
            "couches_supplementaires_count": 0,
            "error": str(e),
        }


DECL_CONTEXTE_PARCELLE = types.FunctionDeclaration(
    name="get_contexte_parcelle",
    description=(
        "Retourne tout le contexte PLU intersectant une ou plusieurs parcelles contiguës "
        "(unité foncière) : zonage, prescriptions, servitudes, informations, "
        "et couches supplémentaires déclarées dans le catalogue de la commune. "
        "La carte interactive est gérée par l'interface (pas de tool carto)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            **parcel_tool_properties(),
            "buffer_m": types.Schema(
                type=types.Type.NUMBER,
                description="Ignoré pour prescriptions/servitudes/infos (intersection stricte parcelle).",
            ),
        },
    ),
)
