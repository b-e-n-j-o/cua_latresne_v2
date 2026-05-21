"""Zonage PLU — get_zonage_et_reglements (usage interne + sessions)."""

import logging

import psycopg2
import psycopg2.extras

from .utils.parcel_geom import resolve_unite_fonciere

logger = logging.getLogger("plu_tools")


def _db_connect(db_config: dict):
    return psycopg2.connect(**db_config)


def _query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    conn = _db_connect(db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def get_zonage_et_reglements(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
) -> dict:
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

        geom_wkb = resolved["geom_wkb"]

        sql = """
            WITH cible AS (SELECT ST_MakeValid(ST_GeomFromEWKB(%s)) AS geom)
            SELECT
                z.zonage_reglement                                            AS code_zone,
                z.libelle, z.libelong, z.typezone, z.destdomi,
                ROUND(ST_Area(ST_Intersection(z.geom_2154, c.geom))::numeric, 1)
                                                                              AS superficie_intersection_m2,
                ROUND((ST_Area(ST_Intersection(z.geom_2154, c.geom))
                       / NULLIF(ST_Area(c.geom), 0) * 100)::numeric, 1)      AS pct_parcelle_couverte,
                r.nom_zone, r.resume_zone, r.reglementation
            FROM argeles.zonage_plu z
            CROSS JOIN cible c
            LEFT JOIN argeles.plu_reglement r ON r.code_zone = z.zonage_reglement
            WHERE ST_Intersects(z.geom_2154, c.geom)
            ORDER BY superficie_intersection_m2 DESC;
        """
        rows = _query(db_config, sql, (geom_wkb,))
        return {
            "zones": rows,
            "count": len(rows),
            "parcelles": parcelles_meta,
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": None,
        }

    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}
