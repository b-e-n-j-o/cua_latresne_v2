# -*- coding: utf-8 -*-
"""Dédoublonnage des extras LLM (import fichier, sans tools/__init__)."""

from __future__ import annotations

import sys
from pathlib import Path

_UTILS = Path(__file__).resolve().parents[1] / "api/agents/plu_agent/tools/utils"
if str(_UTILS) not in sys.path:
    sys.path.insert(0, str(_UTILS))

from llm_item_collapse import collapse_identical_llm_items  # noqa: E402


def test_galeries_identiques_deviennent_une_entree() -> None:
    item = {
        "kind": "lineaire",
        "group": "information",
        "couche": "Galeries et piliers souterrains",
        "layer_id": "galerie_cheminement_et_pilier",
        "Réglementation": "carrières souterraines",
    }
    collapsed = collapse_identical_llm_items([item] * 35)
    assert len(collapsed) == 1
    assert collapsed[0]["nb_entites"] == 35
    assert collapsed[0]["Réglementation"] == "carrières souterraines"


def test_pprmvt_codes_differents_restent_separes() -> None:
    a = {
        "layer_id": "pprmvt_latresne",
        "Code zone": "RF",
        "pct_parcelle_couverte": 37.6,
        "superficie_intersection_m2": 1784.8,
    }
    b = {
        "layer_id": "pprmvt_latresne",
        "Code zone": "BF1",
        "pct_parcelle_couverte": 0.5,
        "superficie_intersection_m2": 22.3,
    }
    collapsed = collapse_identical_llm_items([a, b])
    assert len(collapsed) == 2
    assert "nb_entites" not in collapsed[0]
    assert collapsed[0]["Code zone"] == "RF"


def test_meme_zone_deux_polygones_agreges() -> None:
    a = {
        "layer_id": "pprmvt_latresne",
        "Code zone": "BF1",
        "pct_parcelle_couverte": 0.5,
        "superficie_intersection_m2": 22.3,
    }
    b = {
        "layer_id": "pprmvt_latresne",
        "Code zone": "BF1",
        "pct_parcelle_couverte": 0.1,
        "superficie_intersection_m2": 2.6,
    }
    collapsed = collapse_identical_llm_items([a, b])
    assert len(collapsed) == 1
    assert collapsed[0]["nb_entites"] == 2
    assert collapsed[0]["superficie_intersection_m2"] == 24.9
    assert collapsed[0]["pct_parcelle_couverte"] == 0.6
