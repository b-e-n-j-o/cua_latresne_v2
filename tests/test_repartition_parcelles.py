# -*- coding: utf-8 -*-
"""Matrice parcelle × couche (import fichier, sans tools/__init__)."""

from __future__ import annotations

import sys
from pathlib import Path

_UTILS = Path(__file__).resolve().parents[1] / "api/agents/plu_agent/tools/utils"
if str(_UTILS) not in sys.path:
    sys.path.insert(0, str(_UTILS))

from repartition_format import parcel_repartition_entry, thin_repartition_attrs  # noqa: E402


def test_thin_attrs_retire_le_reglement() -> None:
    thin = thin_repartition_attrs({
        "layer_id": "pprmvt_latresne",
        "couche": "PPRMVT",
        "kind": "surfacique",
        "Code zone": "RF",
        "Nom": "Zone d'interdiction",
        "Réglementation": "texte long recopié",
        "pct_parcelle_couverte": 40.0,
    })
    assert thin == {
        "Code zone": "RF",
        "Nom": "Zone d'interdiction",
        "pct_parcelle_couverte": 40.0,
    }


def test_parcel_entry_sans_hit() -> None:
    assert parcel_repartition_entry("AL 0074", []) == {
        "official": "AL 0074",
        "intersecte": False,
    }


def test_parcel_entry_galerie_sans_attributs() -> None:
    assert parcel_repartition_entry("AL 0417", [{}]) == {
        "official": "AL 0417",
        "intersecte": True,
    }
