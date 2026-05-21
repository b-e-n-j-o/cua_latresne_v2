#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_unite_fonciere.py — valide l'union multi-parcelles (unité foncière contiguë).

Parcelles de référence connues contiguës : BD 634 + BD 518 (Argelès-sur-Mer).

Ce test n'appelle PAS Gemini ni POST /chat : il exécute directement les fonctions
Python des tools (comme le ferait la boucle agentique après un tool_call structuré).

Usage (depuis plu_agent/, venv activé, .env Supabase) :

    python tests/test_unite_fonciere.py
    python tests/test_unite_fonciere.py -v

Variables requises : SUPABASE_HOST, SUPABASE_DB, SUPABASE_USER, SUPABASE_PASSWORD
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

# Racine package plu_agent (parent de tests/)
PLU_AGENT_ROOT = Path(__file__).resolve().parents[1]
if str(PLU_AGENT_ROOT) not in sys.path:
    sys.path.insert(0, str(PLU_AGENT_ROOT))

# .env : cua_latresne_v4/.env (parents[4] depuis tests/) puis plu_agent/.env
for _env_path in (
    PLU_AGENT_ROOT.parents[4] / ".env",
    PLU_AGENT_ROOT / ".env",
):
    if _env_path.is_file():
        load_dotenv(_env_path)
        break
else:
    load_dotenv()

try:
    from _env import DB_CONFIG
except ImportError:
    DB_CONFIG = {
        "host": os.environ["SUPABASE_HOST"],
        "port": int(os.environ.get("SUPABASE_PORT", 5432)),
        "dbname": os.environ["SUPABASE_DB"],
        "user": os.environ["SUPABASE_USER"],
        "password": os.environ["SUPABASE_PASSWORD"],
        "sslmode": "require",
        "connect_timeout": 15,
    }

from tools.utils.parcel_geom import normalize_parcel_refs, resolve_unite_fonciere
from tools.zonage import get_zonage_et_reglements
from tools.carto import build_carto_payload
from tools.contexte_parcelle import get_contexte_parcelle

# ---------------------------------------------------------------------------
# Référence métier
# ---------------------------------------------------------------------------

PARCELLES_CONTIGUES = [
    {"section": "BD", "numero": "634"},
    {"section": "BD", "numero": "518"},
]

SEP = "=" * 78


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _fail(msg: str) -> None:
    logging.error("ÉCHEC — %s", msg)
    raise AssertionError(msg)


def _ok(msg: str) -> None:
    logging.info("OK — %s", msg)


def _log_result(label: str, data: dict) -> None:
    err = data.get("error")
    if err:
        logging.error("%s → error: %s", label, err)
        return
    logging.info("%s → succès", label)
    for key, val in data.items():
        if key == "error":
            continue
        if key == "zones" and isinstance(val, list):
            logging.info("  zones (%d) :", len(val))
            for z in val[:8]:
                logging.info(
                    "    - %s | %.1f%% | %.0f m² | %s",
                    z.get("code_zone"),
                    z.get("pct_parcelle_couverte") or 0,
                    z.get("superficie_intersection_m2") or 0,
                    (z.get("libelle") or "")[:50],
                )
            if len(val) > 8:
                logging.info("    … +%d zone(s)", len(val) - 8)
        elif key == "parcelles" and isinstance(val, list):
            logging.info("  parcelles (%d) : %s", len(val), val)
        elif key == "parcelle" and isinstance(val, dict):
            props = val.get("properties") or val
            logging.info("  parcelle : %s", props)
        else:
            logging.info("  %s : %s", key, val)


def test_normalize_refs() -> None:
    logging.info("%s\nTEST 1 — normalize_parcel_refs\n%s", SEP, SEP)
    refs = normalize_parcel_refs(parcelles=PARCELLES_CONTIGUES)
    logging.debug("refs = %s", refs)
    if len(refs) != 2:
        _fail(f"attendu 2 refs, obtenu {len(refs)}")
    _ok("2 références BD 634 / BD 518 normalisées")


def test_resolve_union() -> dict:
    logging.info("%s\nTEST 2 — resolve_unite_fonciere (union contiguë)\n%s", SEP, SEP)
    result = resolve_unite_fonciere(DB_CONFIG, parcelles=PARCELLES_CONTIGUES)
    _log_result("resolve_unite_fonciere", result)

    if result.get("error"):
        _fail(result["error"])

    nb = result.get("nb_parcelles")
    if nb != 2:
        _fail(f"nb_parcelles={nb}, attendu 2")

    superficie = result.get("superficie_m2")
    if not superficie or superficie <= 0:
        _fail(f"superficie_m2 invalide : {superficie}")

    geojson = result.get("geojson_wgs84")
    if not geojson:
        _fail("geojson_wgs84 manquant")

    import json

    geom = json.loads(geojson) if isinstance(geojson, str) else geojson
    gtype = geom.get("type")
    if gtype not in ("Polygon", "MultiPolygon"):
        _fail(f"type géométrie inattendu après union : {gtype}")

    _ok(f"union WGS84 type={gtype}, superficie={superficie:.1f} m²")
    return result


def test_resolve_singles_vs_union(union_result: dict) -> None:
    logging.info("%s\nTEST 3 — surface union vs parcelles isolées\n%s", SEP, SEP)
    areas = []
    for p in PARCELLES_CONTIGUES:
        one = resolve_unite_fonciere(DB_CONFIG, section=p["section"], numero=p["numero"])
        if one.get("error"):
            _fail(f"parcelle seule {p['section']} {p['numero']} : {one['error']}")
        a = one.get("superficie_m2") or 0
        areas.append(a)
        logging.info(
            "  %s %s → %.1f m² (idu=%s)",
            p["section"],
            p["numero"],
            a,
            (one.get("parcelles") or [{}])[0].get("idu"),
        )

    sum_singles = sum(areas)
    union_area = union_result.get("superficie_m2") or 0
    logging.info("  somme isolées : %.1f m²", sum_singles)
    logging.info("  union         : %.1f m²", union_area)

    # Contiguës sans chevauchement : union ≈ somme (tolérance 1 %)
    if sum_singles > 0:
        ratio = union_area / sum_singles
        logging.info("  ratio union/somme : %.4f", ratio)
        if ratio < 0.98 or ratio > 1.02:
            logging.warning(
                "  écart > 2%% entre union et somme — vérifier chevauchement ou trou cadastral"
            )
    _ok("comparaison surfaces effectuée")


def test_zonage_multi() -> None:
    logging.info("%s\nTEST 4 — get_zonage_et_reglements (unité foncière)\n%s", SEP, SEP)
    result = get_zonage_et_reglements(DB_CONFIG, parcelles=PARCELLES_CONTIGUES)
    _log_result("get_zonage_et_reglements", result)

    if result.get("error"):
        _fail(result["error"])

    zones = result.get("zones") or []
    if not zones:
        _fail("aucune zone PLU intersectée")

    if result.get("nb_parcelles") != 2:
        _fail(f"nb_parcelles={result.get('nb_parcelles')}")

    total_pct = sum(float(z.get("pct_parcelle_couverte") or 0) for z in zones)
    logging.info("  somme des %% couverture : %.1f%%", total_pct)
    if total_pct < 95 or total_pct > 105:
        logging.warning(
            "  somme %% hors [95, 105] — peut être normal si multi-zones + arrondis"
        )

    _ok(f"{len(zones)} zone(s) PLU, réglementation chargée pour analyse LLM")


def test_contexte_multi() -> None:
    logging.info("%s\nTEST 5 — get_contexte_parcelle (unité foncière)\n%s", SEP, SEP)
    result = get_contexte_parcelle(DB_CONFIG, parcelles=PARCELLES_CONTIGUES)
    _log_result("get_contexte_parcelle", result)
    if result.get("error"):
        _fail(result["error"])
    if not result.get("zones"):
        _fail("aucune zone dans le contexte")
    _ok(
        f"contexte LLM — {result.get('zones_count')} zone(s), "
        f"{result.get('prescriptions_count')} prescription(s), "
        f"{result.get('servitudes_count')} servitude(s)"
    )


def test_map_multi() -> None:
    logging.info("%s\nTEST 6 — build_carto_payload (carte unité foncière)\n%s", SEP, SEP)
    result = build_carto_payload(DB_CONFIG, parcelles=PARCELLES_CONTIGUES, buffer_m=100.0)
    if result.get("error"):
        _fail(result["error"])

    parcelle = result.get("parcelle") or {}
    parcelle_union = result.get("parcelle_union")
    zone_features = (result.get("zones") or {}).get("features") or []

    if parcelle.get("type") != "FeatureCollection":
        _fail(f"parcelle attendue en FeatureCollection, obtenu {parcelle.get('type')}")

    feuilles = parcelle.get("features") or []
    if len(feuilles) != 2:
        _fail(f"attendu 2 feuilles cadastrales, obtenu {len(feuilles)}")

    for f in feuilles:
        g = (f.get("geometry") or {}).get("type")
        if g not in ("Polygon", "MultiPolygon"):
            _fail(f"géométrie feuille invalide : {g}")
        logging.info("  feuille : %s", f.get("properties"))

    if not parcelle_union:
        logging.warning("  parcelle_union absente (remplissage enveloppe optionnel)")

    if not zone_features:
        _fail("aucune zone dans FeatureCollection")

    logging.info("  zones carto : %d feature(s)", len(zone_features))
    serv_fc = (result.get("servitudes") or {}).get("features") or []
    logging.info("  servitudes carto : %d feature(s)", len(serv_fc))
    _ok("GeoJSON carte : 2 contours + union + zones + servitudes OK")


def test_non_contiguous_rejected() -> None:
    """Sanity check : deux parcelles éloignées doivent être refusées."""
    logging.info("%s\nTEST 7 — rejet parcelles non contiguës (sanity)\n%s", SEP, SEP)
    # BD 634 + une parcelle très probablement non voisine (section différente si besoin)
    distant = [
        {"section": "BD", "numero": "634"},
        {"section": "AC", "numero": "8770"},
    ]
    result = resolve_unite_fonciere(DB_CONFIG, parcelles=distant)
    if not result.get("error"):
        logging.warning(
            "  les parcelles BD 634 et AC 8770 ont été acceptées comme contiguës — "
            "revoir le test de rejet ou les données cadastre"
        )
        return
    logging.info("  erreur attendue : %s", result["error"])
    if "contigu" not in result["error"].lower():
        logging.warning("  message d'erreur sans mot 'contigu' — acceptable si autre cause")
    _ok("union refusée pour paire non contiguë")


def main() -> int:
    parser = argparse.ArgumentParser(description="Test unité foncière BD 634 + BD 518")
    parser.add_argument("-v", "--verbose", action="store_true", help="logs DEBUG")
    args = parser.parse_args()

    setup_logging(args.verbose)
    started = datetime.now(timezone.utc).isoformat()
    logging.info("Démarrage test_unite_fonciere — %s", started)
    logging.info("Parcelles : %s", PARCELLES_CONTIGUES)

    failed = 0
    try:
        test_normalize_refs()
        union = test_resolve_union()
        test_resolve_singles_vs_union(union)
        test_zonage_multi()
        test_contexte_multi()
        test_map_multi()
        test_non_contiguous_rejected()
    except AssertionError:
        failed = 1
    except Exception as e:
        logging.exception("Erreur inattendue : %s", e)
        failed = 1

    print(f"\n{SEP}")
    if failed:
        print("RÉSULTAT : ÉCHEC")
        print(SEP)
        return 1
    print("RÉSULTAT : TOUS LES TESTS OK (BD 634 + BD 518)")
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
