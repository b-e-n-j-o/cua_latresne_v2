"""Tool get_contexte_parcelle — intersections PLU (zonage + prescriptions + servitudes) pour le LLM."""

import logging

from google.genai import types

from .prescriptions import get_prescriptions
from .servitudes import get_servitudes
from .utils.parcel_geom import parcel_tool_properties
from .zonage import get_zonage_et_reglements

logger = logging.getLogger("plu_tools")


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
    Contexte spatial complet intersectant l'unité foncière :
    zonage PLU, prescriptions et servitudes (assiettes surfaciques SUP).
    Pas de géométries — la carte est servie par GET /session/{id}/map.
    """
    try:
        zonage = get_zonage_et_reglements(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
        )
        if zonage.get("error"):
            logger.warning("get_contexte_parcelle — zonage : %s", zonage["error"])
            return {
                "zones": [],
                "zones_count": 0,
                "surfaciques": [],
                "lineaires": [],
                "ponctuelles": [],
                "prescriptions_count": 0,
                "servitudes": [],
                "servitudes_count": 0,
                "error": zonage["error"],
            }

        presc = get_prescriptions(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
            buffer_m=buffer_m,
        )
        serv = get_servitudes(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
            buffer_m=buffer_m,
        )

        presc_error = presc.get("error")
        serv_error = serv.get("error")
        if presc_error:
            logger.warning("get_contexte_parcelle — prescriptions : %s", presc_error)
        if serv_error:
            logger.warning("get_contexte_parcelle — servitudes : %s", serv_error)

        logger.info(
            "get_contexte_parcelle — %d zone(s), %d prescription(s), %d servitude(s)",
            zonage.get("count", 0),
            presc.get("count", 0) if not presc_error else 0,
            serv.get("count", 0) if not serv_error else 0,
        )

        return {
            "zones": zonage.get("zones", []),
            "zones_count": zonage.get("count", 0),
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
            "parcelles": (
                zonage.get("parcelles")
                or presc.get("parcelles")
                or serv.get("parcelles")
                or []
            ),
            "nb_parcelles": (
                zonage.get("nb_parcelles")
                or presc.get("nb_parcelles")
                or serv.get("nb_parcelles")
            ),
            "superficie_unite_m2": zonage.get("superficie_unite_m2"),
            "error": None,
        }

    except Exception as e:
        return {
            "zones": [],
            "zones_count": 0,
            "surfaciques": [],
            "lineaires": [],
            "ponctuelles": [],
            "prescriptions_count": 0,
            "servitudes": [],
            "servitudes_count": 0,
            "error": str(e),
        }


DECL_CONTEXTE_PARCELLE = types.FunctionDeclaration(
    name="get_contexte_parcelle",
    description=(
        "Retourne tout le contexte PLU intersectant une ou plusieurs parcelles contiguës "
        "(unité foncière) : zonage (codes, %, texte réglementaire), prescriptions "
        "(surfaciques, linéaires, ponctuelles) et servitudes d'utilité publique "
        "(assiettes surfaciques — champs suptype, typeass, nomsuplitt). "
        "À appeler pour toute question sur une parcelle d'Argelès-sur-Mer. "
        "La carte interactive est gérée par l'interface (pas besoin d'un tool carto)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            **parcel_tool_properties(),
            "buffer_m": types.Schema(
                type=types.Type.NUMBER,
                description=(
                    "Buffer en mètres pour inclure prescriptions et servitudes proches "
                    "de la parcelle (défaut: 0 = intersection stricte)."
                ),
            ),
        },
    ),
)
