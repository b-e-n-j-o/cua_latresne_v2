"""Servitudes SUP — get_servitudes (usage interne + get_contexte_parcelle)."""

import logging

from .utils.parcel_geom import resolve_unite_fonciere
from .utils.servitudes_query import build_llm_payload, fetch_servitudes_rows

logger = logging.getLogger("plu_tools")


def get_servitudes(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    buffer_m: float = 0.0,
) -> dict:
    """
    Assiettes surfaciques de servitudes (sup_assiette_s) intersectant l'unité foncière.
    Contexte LLM : suptype (type), typeass et nomsuplitt (précisions).
    """
    try:
        resolved = resolve_unite_fonciere(
            db_config,
            parcelles=parcelles,
            idus=idus,
            section=section,
            numero=numero,
            idu=idu,
        )
        if resolved.get("error"):
            logger.warning("get_servitudes — %s", resolved["error"])
            return {"servitudes": [], "count": 0, "error": resolved["error"]}

        rows = fetch_servitudes_rows(
            db_config,
            resolved["geom_wkb"],
            buffer_m=buffer_m,
            with_geojson=False,
        )
        payload = build_llm_payload(rows)

        logger.info("get_servitudes — %d assiette(s)", payload["count"])

        return {
            **payload,
            "parcelles": resolved.get("parcelles") or [],
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": None,
        }

    except Exception as e:
        return {"servitudes": [], "count": 0, "error": str(e)}
