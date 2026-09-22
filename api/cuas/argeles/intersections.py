# -*- coding: utf-8 -*-
"""
intersections.py — Intersection de l'unité foncière avec les couches SIG.

Pour chaque couche du catalogue, on récupère les entités qui intersectent l'UF
(les contacts purement en bordure sont exclus), on ne garde que les attributs 'keep',
et on mesure l'intersection selon geom_type :
  - surfacique : ST_Area(ST_Intersection) par objet + ST_Area(ST_Union) pour le total couche
                 → élimine le double-comptage quand les géométries sources se superposent.
                 Seuil catalogue min_pct_sig (défaut 1 %) : exclut les micro-recouvrements frontaliers.
  - lineaire   : ST_Length(ST_Intersection) par objet, somme pour le total.
  - ponctuel   : présence seule (pas de mesure).

Format de sortie (compatible builder DOCX) :
{
  "parcelles": [{"section": "BR", "numero": "0273"}, ...],
  "n_parcelles": <int>,
  "parcelle": "UF",
  "surface_m2": <surface SIG>,
  "surface_indicative": <contenance>,
  "intersections": {
     "<table>": {"nom", "type", "geom_type", "pct_sig", "objets": [...]},
     ...
  }
}

CLI :
  python intersections.py --catalogue catalogue_cua_argeles.json --refs "AB:0123,AB:0124"
"""

import re
import json
import argparse
import sys
import time
from decimal import Decimal
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection

_ARGELES_DIR = Path(__file__).resolve().parent

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine, logger
except ImportError:
    from db import GEOM_COL, SCHEMA, SRID, get_engine, logger

try:
    from api.cuas.argeles.uf import build_uf
except ImportError:
    from uf import build_uf

try:
    from api.cuas.argeles.intersection_modules.prairies_et_natura_2000 import (
        compute_prairies_natura_reglementation,
    )
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.prairies_et_natura_2000 import (
        compute_prairies_natura_reglementation,
    )

try:
    from api.cuas.argeles.intersection_modules.reseaux_enedis import compute_enedis_raccordement
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.reseaux_enedis import compute_enedis_raccordement

try:
    from api.cuas.argeles.intersection_modules.servitudes import compute_servitudes_reglementation
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.servitudes import compute_servitudes_reglementation

try:
    from api.cuas.argeles.intersection_modules.ppr_et_pprif import compute_ppr_et_pprif_reglementation
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.ppr_et_pprif import compute_ppr_et_pprif_reglementation

try:
    from api.cuas.argeles.intersection_modules.taxes import compute_taxes
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.taxes import compute_taxes

try:
    from api.cuas.argeles.intersection_modules.alea_feu import compute_alea_feu_reglementation
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.alea_feu import compute_alea_feu_reglementation

try:
    from api.cuas.argeles.intersection_modules.zonage_plu import compute_zonage_plu_reglementation
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.zonage_plu import compute_zonage_plu_reglementation

try:
    from api.cuas.argeles.intersection_modules.prescriptions_plu import (
        compute_prescriptions_plu_reglementation,
    )
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.prescriptions_plu import compute_prescriptions_plu_reglementation

try:
    from api.cuas.argeles.intersection_modules.adresses_parcelles import compute_adresses_parcelles
except ImportError:
    if str(_ARGELES_DIR) not in sys.path:
        sys.path.insert(0, str(_ARGELES_DIR))
    from intersection_modules.adresses_parcelles import compute_adresses_parcelles

try:
    from api.modules_communs.intersection_partielle import catalogue_affiche_pct_partiel
except ImportError:
    def catalogue_affiche_pct_partiel(cfg: dict | None) -> bool:
        return bool((cfg or {}).get("afficher_pct_sig_partiel"))


def _layer_catalogue_meta(cfg: dict, table: str) -> dict:
    """Champs communs issus du catalogue (nom, type, options d'affichage)."""
    return {
        "nom": cfg.get("nom", table),
        "type": cfg.get("type"),
        "geom_type": cfg.get("geom_type", "surfacique"),
        "afficher_pct_sig_partiel": catalogue_affiche_pct_partiel(cfg),
        "min_pct_sig": resolve_min_pct_sig(cfg),
    }



# Seuil minimal d'intersection : exclut les contacts en bordure (aire/longueur nulle)
# et les micro-artefacts numériques.
MIN_INTERSECTION_AREA_M2 = 0.01
MIN_INTERSECTION_LENGTH_M = 0.01
# Part minimale de l'emprise UF pour retenir une intersection surfacique
# (le seuil s'applique par objet, pas par parcelle ni par zone agrégée).
DEFAULT_MIN_PCT_SIG = 1.0
STATUTS_KO = frozenset({"erreur", "table_absente"})

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _qualified_ident(expr: str) -> str:
    parts = (expr or "").split(".")
    if len(parts) != 2:
        raise ValueError(f"Identifiant qualifié invalide : {expr!r}")
    return f"{_safe_ident(parts[0])}.{_safe_ident(parts[1])}"


def _jsonable(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def _join_clause(cfg: dict, schema: str) -> tuple[str, list[tuple[str, str]]]:
    """LEFT JOIN catalogue → (clause SQL, [(expr, alias_colonne), ...])."""
    join_cfg = cfg.get("join") or {}
    if not join_cfg:
        return "", []
    jtable = _safe_ident(join_cfg["table"])
    alias = _safe_ident(join_cfg.get("alias") or "j")
    on = join_cfg.get("on") or []
    if not (isinstance(on, (list, tuple)) and len(on) == 2):
        raise ValueError(
            "join.on doit être [gauche, droite] "
            "(ex. [\"c.code_zone\", \"t.zonage_reglement\"])"
        )
    left, right = _qualified_ident(on[0]), _qualified_ident(on[1])
    extras: list[tuple[str, str]] = []
    for out_col, src in (join_cfg.get("select") or {}).items():
        src = str(src)
        expr = _qualified_ident(src) if "." in src else f"{alias}.{_safe_ident(src)}"
        extras.append((expr, _safe_ident(out_col)))
    return f"LEFT JOIN {schema}.{jtable} {alias} ON {left} = {right}", extras


def resolve_min_pct_sig(cfg: dict) -> float:
    """
    Seuil % de surface pour les couches surfaciques (catalogue min_pct_sig).

    Appliqué par objet par rapport à la surface SIG de l'UF (pas de la parcelle) :
    une zone qui couvre 100 % d'une petite parcelle mais moins que le seuil de l'UF
    est exclue. Les fragments d'une même zone ne sont pas sommés à ce stade.

    Défaut 1.0 ; 0 désactive le filtre pourcentage (seul le seuil géométrique 0,01 m² s'applique).
    """
    geom_type = cfg.get("geom_type", "surfacique")
    if geom_type != "surfacique":
        return 0.0
    if "min_pct_sig" in cfg:
        try:
            return float(cfg["min_pct_sig"])
        except (TypeError, ValueError):
            return DEFAULT_MIN_PCT_SIG
    return DEFAULT_MIN_PCT_SIG


def _tables_existantes(conn, schema: str) -> set[str]:
    """Tables / vues / vues matérialisées du schéma, en une requête."""
    return set(conn.execute(text("""
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :s AND c.relkind IN ('r', 'v', 'm', 'p')
    """), {"s": _safe_ident(schema)}).scalars())


def _table_exists(engine, schema: str, table: str) -> bool:
    """Compat carto_context / audit : présence d'une table isolée."""
    table = _safe_ident(table)
    if isinstance(engine, Connection):
        return table in _tables_existantes(engine, schema)
    with engine.connect() as conn:
        return table in _tables_existantes(conn, schema)


def calculate_intersection(uf_wkt, table, cfg, surface_sig, bind, schema=SCHEMA):
    """
    Retourne (objets, total_metric, geom_type).

    Dédoublonnage : un objet n'est retiré que s'il est un clone parfait d'un autre
    (même géométrie d'intersection ST_AsBinary ET mêmes attributs 'keep').
    => deux entités superposées identiques = 1 ligne ; deux entités distinctes
       (géométrie OU attribut différent) = conservées toutes les deux.

    Pour surfacique, total_metric = ST_Area(ST_Union(intersections)) — pas de double-comptage.
    Pour lineaire, total_metric = somme des longueurs.
    Pour ponctuel, total_metric = 0 (pas de mesure).
    """
    table     = _safe_ident(table)
    geom_col  = _safe_ident(cfg.get("geom_col", GEOM_COL))
    join_sql, join_extras = _join_clause(cfg, schema)
    join_out  = {out for _, out in join_extras}
    keep      = [_safe_ident(k) for k in cfg.get("keep", []) if _safe_ident(k) not in join_out]
    keep_all  = keep + [out for _, out in join_extras]
    geom_type = cfg.get("geom_type", "surfacique")
    min_pct_sig = resolve_min_pct_sig(cfg)

    t_cols     = "".join(f"t.{k}, " for k in keep)
    t_cols    += "".join(f"{src} AS {out}, " for src, out in join_extras)
    raw_cols   = "".join(f"{k}, " for k in keep_all)
    dedup_cols = "".join(f", {k}" for k in keep_all)
    from_sql   = (
        f"FROM {schema}.{table} t {join_sql} CROSS JOIN uf"
        if join_sql
        else f"FROM {schema}.{table} t, uf"
    )
    sql_params: dict = {
        "wkt": uf_wkt,
        "surface_sig": float(surface_sig or 0),
        "min_pct_sig": min_pct_sig,
    }

    if geom_type == "surfacique":
        sql = text(f"""
            WITH uf AS (
                SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
            ),
            inter_raw AS (
                SELECT {t_cols}
                       ST_CollectionExtract(
                           ST_MakeValid(
                               ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom)
                           ),
                           3
                       ) AS inter_geom
                {from_sql}
                WHERE ST_Intersects(t.{geom_col}, uf.geom)
            ),
            inter_filtered AS (
                SELECT {raw_cols} inter_geom
                FROM inter_raw
                WHERE ST_Area(inter_geom) > {MIN_INTERSECTION_AREA_M2}
                  AND (
                      :min_pct_sig <= 0
                      OR :surface_sig <= 0
                      OR (ST_Area(inter_geom) / :surface_sig * 100) > :min_pct_sig
                  )
            ),
            inter AS (
                SELECT DISTINCT ON (ST_AsBinary(inter_geom){dedup_cols})
                       {raw_cols} inter_geom
                FROM inter_filtered
            ),
            union_area AS (
                SELECT COALESCE(ST_Area(ST_Union(inter_geom)), 0.0) AS uarea
                FROM inter
            )
            SELECT {raw_cols}
                   ST_Area(inter_geom)  AS metric,
                   union_area.uarea     AS total_area
            FROM inter, union_area
        """)
        metric_label = "surface_inter_m2"

    elif geom_type == "lineaire":
        sql = text(f"""
            WITH uf AS (
                SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
            ),
            inter_raw AS (
                SELECT {t_cols}
                       ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom) AS inter_geom
                {from_sql}
                WHERE ST_Intersects(t.{geom_col}, uf.geom)
                  AND ST_Length(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom))
                      > {MIN_INTERSECTION_LENGTH_M}
            )
            SELECT DISTINCT ON (ST_AsBinary(inter_geom){dedup_cols})
                   {raw_cols} ST_Length(inter_geom) AS metric
            FROM inter_raw
        """)
        metric_label = "longueur_inter_m"

    else:  # ponctuel
        dedup_on_t = "".join(f", t.{k}" for k in keep) + "".join(
            f", {src}" for src, _ in join_extras
        )
        sql = text(f"""
            WITH uf AS (SELECT ST_GeomFromText(:wkt, {SRID}) AS geom)
            SELECT DISTINCT ON (ST_AsBinary(t.{geom_col}){dedup_on_t})
                   {t_cols} NULL::float AS metric
            {from_sql}
            WHERE ST_Within(t.{geom_col}, uf.geom)
        """)
        metric_label = None

    if isinstance(bind, Connection):
        rows = bind.execute(sql, sql_params).mappings().all()
    else:
        with bind.connect() as conn:
            rows = conn.execute(sql, sql_params).mappings().all()

    objets = []
    total  = 0.0

    for i, r in enumerate(rows):
        obj = {k: _jsonable(r[k]) for k in keep_all}
        m   = r["metric"]

        if metric_label and m is not None:
            obj[metric_label] = round(float(m), 2)

            if geom_type == "surfacique" and surface_sig > 0:
                obj["pct_sig"] = round(float(m) / surface_sig * 100, 4)
            elif geom_type == "lineaire":
                total += float(m)

        # surfacique : total = union area (identique sur toutes les lignes, lu sur la 1ère)
        if geom_type == "surfacique" and i == 0:
            total = float(r["total_area"])

        objets.append(obj)

    return objets, total, geom_type


def _parcelles_payload(uf) -> list[dict]:
    """Références cadastrales normalisées (valeurs issues de la base)."""
    return [{"section": s, "numero": n} for s, n in uf.parcelles]


def run_intersections(uf, catalogue, engine=None, schema=SCHEMA) -> dict:
    """Boucle sur toutes les couches du catalogue et assemble le rapport."""
    engine = engine or get_engine()

    rapport = {
        "parcelles": _parcelles_payload(uf),
        "n_parcelles": uf.n_parcelles,
        "parcelle": "UF",
        "surface_m2": round(uf.surface_sig, 2),
        "surface_indicative": round(uf.surface_cadastrale, 2) if uf.surface_cadastrale else round(uf.surface_sig, 2),
        "intersections": {},
    }

    with engine.connect() as conn:
        tables_ok = _tables_existantes(conn, schema)

        for table, cfg in catalogue.items():
            if table == "reseaux_enedis_lineaires":
                try:
                    special = compute_enedis_raccordement(
                        uf.wkt,
                        engine=engine,
                        schema=schema,
                    )
                    rapport["intersections"][table] = {
                        **_layer_catalogue_meta(cfg, table),
                        "pct_sig": 0.0,
                        "objets": [],
                        **special,
                    }
                except Exception as exc:
                    logger.warning(f"  ⚠  {table:<35} {exc}")
                    rapport["intersections"][table] = {
                        **_layer_catalogue_meta(cfg, table),
                        "pct_sig": 0.0,
                        "objets": [],
                        "status": "erreur",
                        "error": str(exc),
                    }
                continue

            if cfg.get("handler") == "servitudes":
                try:
                    special = compute_servitudes_reglementation(
                        uf.wkt,
                        engine=engine,
                        schema=schema,
                        surface_sig=uf.surface_sig,
                        min_pct_sig=resolve_min_pct_sig(cfg),
                    )
                    rapport["intersections"][table] = {
                        **_layer_catalogue_meta(cfg, table),
                        "pct_sig": 0.0,
                        "objets": [],
                        **special,
                    }
                    n = len(special.get("servitudes") or [])
                    if n:
                        logger.info(f"  ✅ {table:<35} {n:>3} servitude(s)")
                    else:
                        logger.info(f"  ·  {table:<35}   —")
                except Exception as exc:
                    logger.warning(f"  ⚠  {table:<35} {exc}")
                    rapport["intersections"][table] = {
                        **_layer_catalogue_meta(cfg, table),
                        "pct_sig": 0.0,
                        "objets": [],
                        "status": "erreur",
                        "error": str(exc),
                        "servitudes": [],
                    }
                continue

            if table not in tables_ok:
                logger.warning(f"  ⏭  {table:<35} table absente en base")
                rapport["intersections"][table] = {
                    **_layer_catalogue_meta(cfg, table),
                    "pct_sig": 0.0,
                    "objets": [],
                    "status": "table_absente",
                }
                continue

            t0 = time.perf_counter()
            try:
                objets, total, geom_type = calculate_intersection(
                    uf.wkt, table, cfg, uf.surface_sig, conn, schema
                )
            except Exception as exc:
                conn.rollback()  # sinon toutes les couches suivantes échouent
                logger.warning(f"  ⚠  {table:<35} {exc}")
                rapport["intersections"][table] = {
                    **_layer_catalogue_meta(cfg, table),
                    "pct_sig": 0.0,
                    "objets": [],
                    "status": "erreur",
                    "error": str(exc),
                }
                continue
            dt = (time.perf_counter() - t0) * 1000

            if geom_type == "surfacique" and uf.surface_sig > 0:
                pct = round(total / uf.surface_sig * 100, 4)
            else:
                pct = 0.0

            rapport["intersections"][table] = {
                **_layer_catalogue_meta(cfg, table),
                "pct_sig": pct,
                "objets": objets,
                "status": "concernee" if objets else "non_concernee",
            }

            if objets:
                logger.info(f"  ✅ {table:<35} {len(objets):>3} objet(s) | {pct:.2f}% | {dt:.0f} ms")
            else:
                logger.info(f"  ·  {table:<35}   —  | {dt:.0f} ms")

    # Bloc métier dédié Natura 2000 / Prairies (sans encombrer la boucle catalogue)
    try:
        special = compute_prairies_natura_reglementation(
            uf.wkt,
            surface_sig=uf.surface_sig,
            engine=engine,
            schema=schema,
        )
        rapport["intersections"]["prairies_et_natura_2000"] = {
            "nom": "Réglementation croisée Natura 2000 / Prairies sensibles",
            "type": "information",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            **special,
        }
    except Exception as exc:
        logger.warning(f"  ⚠  prairies_et_natura_2000          {exc}")
        rapport["intersections"]["prairies_et_natura_2000"] = {
            "nom": "Réglementation croisée Natura 2000 / Prairies sensibles",
            "type": "information",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
        }

    # Bloc métier dédié PPR / PPRIF (attributs ppr + laius pprif)
    ppr_layer = rapport["intersections"].get("ppr", {})
    pprif_layer = rapport["intersections"].get("pprif", {})
    try:
        special = compute_ppr_et_pprif_reglementation(
            ppr_objets=ppr_layer.get("objets") or [],
            pprif_objets=pprif_layer.get("objets") or [],
            parcelles=rapport.get("parcelles") or [],
            ppr_cfg=catalogue.get("ppr"),
            pprif_cfg=catalogue.get("pprif"),
            engine=engine,
            schema=schema,
            min_detail_pct=resolve_min_pct_sig(catalogue.get("ppr") or {}),
        )
        rapport["intersections"]["ppr_et_pprif"] = {
            "nom": "PPR / PPRIF (réglementation)",
            "type": "prescription",
            "geom_type": "surfacique",
            "pct_sig": max(ppr_layer.get("pct_sig") or 0, pprif_layer.get("pct_sig") or 0),
            "objets": [],
            **special,
        }
        n_ppr = len((special.get("ppr") or {}).get("blocs") or [])
        n_pprif = len((special.get("pprif") or {}).get("blocs") or [])
        n_parcelles = len(special.get("detail_parcelles") or [])
        if n_ppr or n_pprif or n_parcelles:
            extra = f" | {n_parcelles} parcelle(s)" if n_parcelles else ""
            logger.info(f"  ✅ ppr_et_pprif                        PPR {n_ppr} | PPRIF {n_pprif}{extra}")
        else:
            logger.info("  ·  ppr_et_pprif                          —")
    except Exception as exc:
        logger.warning(f"  ⚠  ppr_et_pprif                     {exc}")
        rapport["intersections"]["ppr_et_pprif"] = {
            "nom": "PPR / PPRIF (réglementation)",
            "type": "prescription",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
        }

    # Bloc métier dédié taxes (taux communale depuis argeles.taxes)
    try:
        special = compute_taxes(
            uf.wkt,
            surface_sig=uf.surface_sig,
            engine=engine,
            schema=schema,
        )
        rapport["intersections"]["taxes"] = {
            "nom": "Taxe d'aménagement – part communale",
            "type": "fiscalite",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            **special,
        }
        if special.get("status") == "concernee":
            taux = special.get("taux_communale_libelle")
            lib = (special.get("libelle") or "zone fiscale").strip()
            logger.info(f"  ✅ taxes                              {taux} ({lib})")
        else:
            logger.info("  ·  taxes                                —")
    except Exception as exc:
        logger.warning(f"  ⚠  taxes                            {exc}")
        rapport["intersections"]["taxes"] = {
            "nom": "Taxe d'aménagement – part communale",
            "type": "fiscalite",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
        }

    # Bloc métier dédié prescriptions PLU (surf / lin / ponct)
    psc_surf = rapport["intersections"].get("prescriptions_surf", {})
    psc_lin = rapport["intersections"].get("prescriptions_lineaires", {})
    psc_ponct = rapport["intersections"].get("prescriptions_ponctuelles", {})
    try:
        special = compute_prescriptions_plu_reglementation(
            surf_objets=psc_surf.get("objets") or [],
            lineaires_objets=psc_lin.get("objets") or [],
            ponctuelles_objets=psc_ponct.get("objets") or [],
            parcelles=rapport.get("parcelles") or [],
            surf_cfg=catalogue.get("prescriptions_surf"),
            lineaires_cfg=catalogue.get("prescriptions_lineaires"),
            ponctuelles_cfg=catalogue.get("prescriptions_ponctuelles"),
            engine=engine,
            schema=schema,
        )
        rapport["intersections"]["prescriptions_plu"] = {
            "nom": "Prescriptions PLU",
            "type": "prescription",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            **special,
        }
        n_items = sum(len(c.get("items") or []) for c in special.get("couches") or [])
        n_parcelles = len(special.get("detail_parcelles") or [])
        if n_items or n_parcelles:
            extra = f" | {n_parcelles} parcelle(s)" if n_parcelles else ""
            logger.info(f"  ✅ prescriptions_plu (métier)           {n_items} item(s){extra}")
        else:
            logger.info("  ·  prescriptions_plu (métier)            —")
    except Exception as exc:
        logger.warning(f"  ⚠  prescriptions_plu (métier)       {exc}")
        rapport["intersections"]["prescriptions_plu"] = {
            "nom": "Prescriptions PLU",
            "type": "prescription",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
            "couches": [],
            "detail_parcelles": [],
        }

    # Bloc métier dédié zonage PLU (intro UF + détail parcelles + blocs réglementaires)
    zonage_layer = rapport["intersections"].get("zonage_plu", {})
    if zonage_layer.get("status") in STATUTS_KO:
        logger.error("  ⚠  zonage_plu (métier) ignoré : %s", zonage_layer.get("status"))
    else:
        try:
            special = compute_zonage_plu_reglementation(
                zonage_objets=zonage_layer.get("objets") or [],
                parcelles=rapport.get("parcelles") or [],
                zonage_cfg=catalogue.get("zonage_plu"),
                engine=engine,
                schema=schema,
                min_zonage_pct=resolve_min_pct_sig(catalogue.get("zonage_plu") or {}),
            )
            rapport["intersections"]["zonage_plu"] = {
                **zonage_layer,
                **special,
            }
            n_items = len(special.get("items") or [])
            n_parcelles = len(special.get("detail_parcelles") or [])
            if n_items or n_parcelles:
                extra = f" | {n_parcelles} parcelle(s)" if n_parcelles else ""
                logger.info(f"  ✅ zonage_plu (métier)                  {n_items} bloc(s){extra}")
            else:
                logger.info("  ·  zonage_plu (métier)                    —")
        except Exception as exc:
            logger.warning(f"  ⚠  zonage_plu (métier)              {exc}")
            rapport["intersections"]["zonage_plu"] = {
                **zonage_layer,
                "status": "erreur",
                "error": str(exc),
                "intro": None,
                "zones": [],
                "items": [],
                "detail_parcelles": [],
            }

    # Bloc métier dédié aléa feu (PAC — porté à connaissance)
    alea_layer = rapport["intersections"].get("alea_feu", {})
    if alea_layer.get("status") in STATUTS_KO:
        logger.error("  ⚠  alea_feu (métier) ignoré : %s", alea_layer.get("status"))
    else:
        try:
            special = compute_alea_feu_reglementation(
                alea_feu_objets=alea_layer.get("objets") or [],
            )
            rapport["intersections"]["alea_feu"] = {
                **alea_layer,
                **special,
            }
            n_blocs = len(special.get("blocs") or [])
            if n_blocs:
                logger.info(f"  ✅ alea_feu                             {n_blocs} aléa(s)")
            else:
                logger.info("  ·  alea_feu                               —")
        except Exception as exc:
            logger.warning(f"  ⚠  alea_feu                           {exc}")
            rapport["intersections"]["alea_feu"] = {
                **alea_layer,
                "nom": "Risque d'incendie de forêt et de végétation",
                "type": "prescription",
                "geom_type": "surfacique",
                "pct_sig": alea_layer.get("pct_sig") or 0.0,
                "objets": alea_layer.get("objets") or [],
                "status": "erreur",
                "error": str(exc),
                "blocs": [],
            }

    # Adresses BAN liées aux parcelles (header CUA — jointure locale, pas d'intersection SIG)
    try:
        adresses_bloc = compute_adresses_parcelles(
            parcelles=rapport.get("parcelles") or [],
            engine=engine,
            schema=schema,
        )
        rapport["adresses_parcelles"] = adresses_bloc
        if adresses_bloc.get("texte_header"):
            logger.info(f"  ✅ adresses_parcelles                  {adresses_bloc['diagnostic_metier']}")
        elif adresses_bloc.get("status") == "table_absente":
            logger.warning("  ⏭  adresses_parcelles                  tables absentes")
        else:
            logger.info("  ·  adresses_parcelles                    —")
    except Exception as exc:
        logger.warning(f"  ⚠  adresses_parcelles                 {exc}")
        rapport["adresses_parcelles"] = {
            "status": "erreur",
            "diagnostic_metier": str(exc),
            "parcelles": [],
            "adresses_uniques": [],
            "texte_header": None,
        }

    return rapport


def load_catalogue(path: str) -> dict:
    raw = Path(path).read_text(encoding="utf-8")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Catalogue CUA invalide ({path}) : JSON ligne {exc.lineno} col {exc.colno} — {exc.msg}"
        ) from exc


# ============================================================
# CLI DE TEST
# ============================================================
def _parse_refs(raw: str):
    """'AB:0123, AB:0124' -> [{'section':'AB','numero':'0123'}, ...]"""
    refs = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        section, _, numero = tok.partition(":")
        if not numero:
            raise ValueError(f"Référence invalide '{tok}' (format attendu SECTION:NUMERO).")
        refs.append({"section": section.strip(), "numero": numero.strip()})
    return refs


def main():
    ap = argparse.ArgumentParser(description="Test intersections UF → couches SIG (Argelès)")
    ap.add_argument("--catalogue", default="catalogue_cua_argeles.json", help="Chemin du catalogue JSON")
    ap.add_argument("--refs",      required=True, help='Refs parcellaires, ex: "AB:0123,AB:0124"')
    ap.add_argument("--schema",    default=SCHEMA)
    ap.add_argument("--out",       default=None,  help="Chemin de sortie JSON (optionnel)")
    args = ap.parse_args()

    refs      = _parse_refs(args.refs)
    catalogue = load_catalogue(args.catalogue)

    uf = build_uf(refs, schema=args.schema)
    logger.info(f"🔎 Intersection sur {len(catalogue)} couche(s)…")
    rapport = run_intersections(uf, catalogue, schema=args.schema)

    n_touch = sum(1 for v in rapport["intersections"].values() if v.get("objets"))
    logger.info(f"\n🎯 {n_touch}/{len(catalogue)} couche(s) intersectée(s).")

    out = args.out or f"rapport_intersections_{refs[0]['section']}{refs[0]['numero']}.json"
    Path(out).write_text(
        json.dumps(rapport, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
    logger.info(f"💾 Rapport écrit : {out}")


if __name__ == "__main__":
    main()