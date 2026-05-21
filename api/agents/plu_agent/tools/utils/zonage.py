"""
Zonage PLU — intersection surfacique réelle + règlement.

ST_Intersects seul inclut les contacts bord à bord (0 %) ; on exige une aire > seuil.
"""

from __future__ import annotations

import logging

from .db import db_query
from .parcel_geom import resolve_unite_fonciere

logger = logging.getLogger("plu_tools")

MIN_PARCEL_INTERSECTION_M2 = 1.0


def _zone_geom(alias: str = "z") -> str:
    return f"ST_MakeValid({alias}.geom_2154)"


def _intersection_with_parcel(zone_alias: str = "z", parcel_alias: str = "c") -> str:
    return f"ST_Intersection({_zone_geom(zone_alias)}, {parcel_alias}.geom)"


def parcel_intersection_area_sql(
    zone_alias: str = "z",
    parcel_alias: str = "c",
) -> str:
    return f"ST_Area({_intersection_with_parcel(zone_alias, parcel_alias)})"


def parcel_intersection_filter_sql(
    zone_alias: str = "z",
    parcel_alias: str = "c",
    min_m2: float | None = None,
) -> str:
    threshold = min_m2 if min_m2 is not None else MIN_PARCEL_INTERSECTION_M2
    return f"{parcel_intersection_area_sql(zone_alias, parcel_alias)} > {float(threshold)}"


def fetch_zonage_reglement_rows(
    db_config: dict,
    geom_wkb: bytes,
    min_intersection_m2: float | None = None,
) -> list[dict]:
    min_m2 = float(
        min_intersection_m2
        if min_intersection_m2 is not None
        else MIN_PARCEL_INTERSECTION_M2
    )
    zone_g = _zone_geom("z")
    ix = _intersection_with_parcel("z", "c")

    sql = f"""
        WITH cible AS (
            SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom
        )
        SELECT
            z.zonage_reglement AS code_zone,
            z.libelle,
            z.libelong,
            z.typezone,
            z.destdomi,
            ROUND(ST_Area({ix})::numeric, 1) AS superficie_intersection_m2,
            ROUND(
                (ST_Area({ix}) / NULLIF(ST_Area(c.geom), 0) * 100)::numeric,
                1
            ) AS pct_parcelle_couverte,
            r.nom_zone,
            r.resume_zone,
            r.reglementation
        FROM argeles.zonage_plu z
        CROSS JOIN cible c
        LEFT JOIN argeles.plu_reglement r ON r.code_zone = z.zonage_reglement
        WHERE ST_Intersects({zone_g}, c.geom)
          AND {parcel_intersection_filter_sql("z", "c", min_m2)}
        ORDER BY superficie_intersection_m2 DESC;
    """
    return db_query(db_config, sql, (geom_wkb,))


def filter_zonage_rows(
    rows: list[dict],
    min_intersection_m2: float | None = None,
    min_pct: float = 0.0,
) -> list[dict]:
    min_m2 = (
        float(min_intersection_m2)
        if min_intersection_m2 is not None
        else MIN_PARCEL_INTERSECTION_M2
    )
    out = []
    for r in rows:
        surf = float(r.get("superficie_intersection_m2") or 0)
        pct = float(r.get("pct_parcelle_couverte") or 0)
        if surf >= min_m2 and pct > min_pct:
            out.append(r)
    return out


def get_zonage_et_reglements(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    min_intersection_m2: float | None = None,
) -> dict:
    """Zones PLU avec surface réelle sur la parcelle (exclut contacts 0 m² / 0 %)."""
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
            logger.warning(
                "get_zonage_et_reglements — %s (parcelles=%s, idus=%s, section=%r, numero=%r)",
                resolved["error"],
                parcelles,
                idus,
                section,
                numero,
            )
            return {"zones": [], "count": 0, "error": resolved["error"]}

        parcelles_meta = resolved.get("parcelles") or []
        if parcelles_meta:
            logger.info(
                "get_zonage_et_reglements — %d parcelle(s) : %s",
                len(parcelles_meta),
                ", ".join(
                    f"{p.get('section')} {p.get('numero')} ({p.get('idu')})"
                    for p in parcelles_meta
                ),
            )

        rows = fetch_zonage_reglement_rows(
            db_config,
            resolved["geom_wkb"],
            min_intersection_m2=min_intersection_m2,
        )
        rows = filter_zonage_rows(rows, min_intersection_m2=min_intersection_m2)

        if rows:
            logger.info(
                "get_zonage_et_reglements — %d zone(s) (seuil ≥ %.1f m²) : %s",
                len(rows),
                min_intersection_m2 or MIN_PARCEL_INTERSECTION_M2,
                ", ".join(
                    f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte')}%)"
                    for z in rows
                ),
            )

        return {
            "zones": rows,
            "count": len(rows),
            "parcelles": parcelles_meta,
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "min_intersection_m2": min_intersection_m2 or MIN_PARCEL_INTERSECTION_M2,
            "error": None,
        }

    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}
