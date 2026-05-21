"""Prescriptions PLU — get_prescriptions (usage interne + get_contexte_parcelle)."""

import logging

from .utils.parcel_geom import resolve_unite_fonciere
from .utils.prescriptions_query import build_llm_payload, fetch_prescriptions_rows

logger = logging.getLogger("plu_tools")


def get_prescriptions(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    buffer_m: float = 0.0,
) -> dict:
    """
    Prescriptions PLU intersectant l'unité foncière (surfacique, linéaire, ponctuelle).
    L'information principale est le champ libelle (+ txt, typepsc, stypepsc).
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
            logger.warning("get_prescriptions — %s", resolved["error"])
            return {
                "surfaciques": [],
                "lineaires": [],
                "ponctuelles": [],
                "count": 0,
                "error": resolved["error"],
            }

        rows_by_kind = fetch_prescriptions_rows(
            db_config,
            resolved["geom_wkb"],
            buffer_m=buffer_m,
            with_geojson=False,
        )
        payload = build_llm_payload(rows_by_kind)

        logger.info(
            "get_prescriptions — %d surf, %d lin, %d pct",
            payload["count_surfaciques"],
            payload["count_lineaires"],
            payload["count_ponctuelles"],
        )

        return {
            **payload,
            "parcelles": resolved.get("parcelles") or [],
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": None,
        }

    except Exception as e:
        return {
            "surfaciques": [],
            "lineaires": [],
            "ponctuelles": [],
            "count": 0,
            "error": str(e),
        }
