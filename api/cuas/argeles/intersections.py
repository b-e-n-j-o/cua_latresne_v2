# -*- coding: utf-8 -*-
"""
intersections.py — Intersection de l'unité foncière avec les couches SIG.

Pour chaque couche du catalogue, on récupère les entités qui intersectent l'UF
(les contacts purement en bordure sont exclus), on ne garde que les attributs 'keep',
et on mesure l'intersection selon geom_type :
  - surfacique : ST_Area(ST_Intersection) par objet + ST_Area(ST_Union) pour le total couche
                 → élimine le double-comptage quand les géométries sources se superposent.
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
from pathlib import Path

from sqlalchemy import text

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

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Seuil minimal d'intersection : exclut les contacts en bordure (aire/longueur nulle)
# et les micro-artefacts numériques.
MIN_INTERSECTION_AREA_M2 = 0.01
MIN_INTERSECTION_LENGTH_M = 0.01


def _safe_ident(name: str) -> str:
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return name


def _table_exists(engine, schema: str, table: str) -> bool:
    schema = _safe_ident(schema)
    table = _safe_ident(table)
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )


def calculate_intersection(uf_wkt, table, cfg, surface_sig, engine, schema=SCHEMA):
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
    keep      = [_safe_ident(k) for k in cfg.get("keep", [])]
    geom_type = cfg.get("geom_type", "surfacique")

    t_cols     = "".join(f"t.{k}, " for k in keep)   # SELECT depuis la table   → t.col,
    raw_cols   = "".join(f"{k}, "   for k in keep)   # SELECT depuis un CTE      → col,
    dedup_cols = "".join(f", {k}"   for k in keep)   # clés DISTINCT ON (avec virgule de tête)

    if geom_type == "surfacique":
        sql = text(f"""
            WITH uf AS (
                SELECT ST_GeomFromText(:wkt, {SRID}) AS geom
            ),
            inter_raw AS (
                SELECT {t_cols}
                       ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom) AS inter_geom
                FROM {schema}.{table} t, uf
                WHERE ST_Intersects(t.{geom_col}, uf.geom)
                  AND ST_Area(ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom))
                      > {MIN_INTERSECTION_AREA_M2}
            ),
            inter AS (
                SELECT DISTINCT ON (ST_AsBinary(inter_geom){dedup_cols})
                       {raw_cols} inter_geom
                FROM inter_raw
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
                FROM {schema}.{table} t, uf
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
        sql = text(f"""
            WITH uf AS (SELECT ST_GeomFromText(:wkt, {SRID}) AS geom)
            SELECT DISTINCT ON (ST_AsBinary(t.{geom_col}){dedup_cols})
                   {raw_cols} NULL::float AS metric
            FROM {schema}.{table} t, uf
            WHERE ST_Within(t.{geom_col}, uf.geom)
        """)
        metric_label = None

    with engine.connect() as conn:
        rows = conn.execute(sql, {"wkt": uf_wkt}).mappings().all()

    objets = []
    total  = 0.0

    for i, r in enumerate(rows):
        obj = {k: r[k] for k in keep}
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

    for table, cfg in catalogue.items():
        geom_type_cfg = cfg.get("geom_type", "surfacique")

        if table == "reseaux_enedis_lineaires":
            try:
                special = compute_enedis_raccordement(
                    uf.wkt,
                    engine=engine,
                    schema=schema,
                )
                rapport["intersections"][table] = {
                    "nom": cfg.get("nom", table),
                    "type": cfg.get("type"),
                    "geom_type": geom_type_cfg,
                    "pct_sig": 0.0,
                    "objets": [],
                    **special,
                }
            except Exception as exc:
                logger.warning(f"  ⚠  {table:<35} {exc}")
                rapport["intersections"][table] = {
                    "nom": cfg.get("nom", table),
                    "type": cfg.get("type"),
                    "geom_type": geom_type_cfg,
                    "pct_sig": 0.0,
                    "objets": [],
                    "status": "erreur",
                    "error": str(exc),
                }
            continue

        if not _table_exists(engine, schema, table):
            logger.warning(f"  ⏭  {table:<35} table absente en base")
            rapport["intersections"][table] = {
                "nom": cfg.get("nom", table),
                "type": cfg.get("type"),
                "geom_type": geom_type_cfg,
                "pct_sig": 0.0,
                "objets": [],
                "status": "table_absente",
            }
            continue

        try:
            objets, total, geom_type = calculate_intersection(
                uf.wkt, table, cfg, uf.surface_sig, engine, schema
            )
        except Exception as exc:
            logger.warning(f"  ⚠  {table:<35} {exc}")
            rapport["intersections"][table] = {
                "nom": cfg.get("nom", table),
                "type": cfg.get("type"),
                "geom_type": geom_type_cfg,
                "pct_sig": 0.0,
                "objets": [],
                "status": "erreur",
                "error": str(exc),
            }
            continue

        if geom_type == "surfacique" and uf.surface_sig > 0:
            pct = round(total / uf.surface_sig * 100, 4)
        else:
            pct = 0.0

        rapport["intersections"][table] = {
            "nom":      cfg.get("nom", table),
            "type":     cfg.get("type"),
            "geom_type": geom_type,
            "pct_sig":  pct,
            "objets":   objets,
            "status":   "concernee" if objets else "non_concernee",
        }

        if objets:
            logger.info(f"  ✅ {table:<35} {len(objets):>3} objet(s) | {pct:.2f}%")
        else:
            logger.info(f"  ·  {table:<35}   —")

    # Bloc métier dédié Natura 2000 / Prairies (sans encombrer la boucle catalogue)
    try:
        special = compute_prairies_natura_reglementation(
            uf.wkt,
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

    # Bloc métier dédié SUP (réglementations depuis servitudes_reglements)
    try:
        special = compute_servitudes_reglementation(
            uf.wkt,
            engine=engine,
            schema=schema,
        )
        rapport["intersections"]["servitudes_reglementees"] = {
            "nom": "Servitudes d'utilité publique (réglementation)",
            "type": "servitude",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            **special,
        }
        n = len(special.get("servitudes") or [])
        if n:
            logger.info(f"  ✅ servitudes_reglementees              {n:>3} servitude(s)")
        else:
            logger.info("  ·  servitudes_reglementees                —")
    except Exception as exc:
        logger.warning(f"  ⚠  servitudes_reglementees          {exc}")
        rapport["intersections"]["servitudes_reglementees"] = {
            "nom": "Servitudes d'utilité publique (réglementation)",
            "type": "servitude",
            "geom_type": "surfacique",
            "pct_sig": 0.0,
            "objets": [],
            "status": "erreur",
            "error": str(exc),
        }

    return rapport


def load_catalogue(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


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

    n_touch = sum(1 for v in rapport["intersections"].values() if v["objets"])
    logger.info(f"\n🎯 {n_touch}/{len(catalogue)} couche(s) intersectée(s).")

    out = args.out or f"rapport_intersections_{refs[0]['section']}{refs[0]['numero']}.json"
    Path(out).write_text(
        json.dumps(rapport, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
    logger.info(f"💾 Rapport écrit : {out}")


if __name__ == "__main__":
    main()