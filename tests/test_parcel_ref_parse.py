# -*- coding: utf-8 -*-
"""Parseur déterministe section/numéro cadastral (import fichier, sans tools/__init__)."""

from __future__ import annotations

import sys
from pathlib import Path

_UTILS = Path(__file__).resolve().parents[1] / "api/agents/plu_agent/tools/utils"
if str(_UTILS) not in sys.path:
    sys.path.insert(0, str(_UTILS))

from parcel_ref_parse import (  # noqa: E402
    extract_parcel_pairs,
    format_parcel_identity_prompt,
    official_label,
    parse_parcel_refs_from_text,
)


def _pairs(text: str) -> list[tuple[str, str]]:
    return [(p["section"], p["numero"]) for p in extract_parcel_pairs(text)]


def test_exemples_saisie() -> None:
    assert _pairs("AL 74") == [("AL", "74")]
    assert _pairs("AL74") == [("AL", "74")]
    assert _pairs("AL 741") == [("AL", "741")]
    assert _pairs("A 7") == [("A", "7")]
    assert _pairs("A7") == [("A", "7")]
    assert _pairs("AN 0076") == [("AN", "0076")]


def test_separateurs_et_explicit() -> None:
    assert _pairs("AL-74") == [("AL", "74")]
    assert _pairs("AL/74") == [("AL", "74")]
    assert _pairs("section AL n°74") == [("AL", "74")]
    assert _pairs("parcelle AL numéro 74") == [("AL", "74")]
    assert _pairs("la parcelle a 7") == [("A", "7")]


def test_minuscules() -> None:
    assert _pairs("al 74") == [("AL", "74")]
    assert _pairs("infos sur al74 svp") == [("AL", "74")]


def test_officiel_padde() -> None:
    assert official_label("AL", "74") == "AL 0074"
    assert official_label("A", "7") == "A 0007"
    assert official_label("AN", "0076") == "AN 0076"


def test_multi_parcelles() -> None:
    parsed = parse_parcel_refs_from_text("parcelles AL 74 et AL 81")
    assert parsed["parcelles"] == [
        {"section": "AL", "numero": "74"},
        {"section": "AL", "numero": "81"},
    ]
    parsed2 = parse_parcel_refs_from_text("AL 74 et 75")
    assert parsed2["parcelles"] == [
        {"section": "AL", "numero": "74"},
        {"section": "AL", "numero": "75"},
    ]


def test_idu() -> None:
    assert parse_parcel_refs_from_text("idu 66008000AL0074") == {
        "idu": "66008000AL0074"
    }


def test_faux_positifs_francais() -> None:
    assert _pairs("à 5 mètres de la route") == []
    assert _pairs("de 12 m de recul") == []
    assert _pairs("en 2024 le PLU") == []
    assert _pairs("il y a 12 bâtiments") == []
    assert _pairs("il a 12 ans") == []
    assert _pairs("a 5 % d'emprise") == []
    assert _pairs("article L421-6") == []
    assert _pairs("selon R151-1") == []
    assert _pairs("zone A constructible") == []
    assert _pairs("secteur AU 1") == []
    assert _pairs("an 2023") == []


def test_phrase_utile() -> None:
    parsed = parse_parcel_refs_from_text(
        "Quelles règles d'urbanisme pour la parcelle AL 74 ?"
    )
    assert parsed == {"section": "AL", "numero": "74"}
    parsed2 = parse_parcel_refs_from_text("infos sur A7")
    assert parsed2 == {"section": "A", "numero": "7"}


def test_numeros_juxtaposes_sans_virgule() -> None:
    parsed = parse_parcel_refs_from_text("AM 1106, AL74, AL 418 417")
    labels = [official_label(p["section"], p["numero"]) for p in parsed["parcelles"]]
    assert labels == ["AM 1106", "AL 0074", "AL 0418", "AL 0417"]

    parsed2 = parse_parcel_refs_from_text("AM 1106 AL74 AL 418 417")
    labels2 = [official_label(p["section"], p["numero"]) for p in parsed2["parcelles"]]
    assert labels2 == ["AM 1106", "AL 0074", "AL 0418", "AL 0417"]


def test_et_418_n_est_pas_une_section() -> None:
    parsed = parse_parcel_refs_from_text(
        "superficie parcelle par parcelle des AL74 AL 416, 417 et 418"
    )
    labels = [official_label(p["section"], p["numero"]) for p in parsed["parcelles"]]
    assert labels == ["AL 0074", "AL 0416", "AL 0417", "AL 0418"]
    assert all(p["section"] != "ET" for p in parsed["parcelles"])


def test_unite_fonciere_trois_feuilles() -> None:
    text = (
        "je souhaite connaitre le zonage parcelle par parcelle "
        "de l'unité fonciere AL74 AL 417 et AL416"
    )
    parsed = parse_parcel_refs_from_text(text)
    assert parsed["parcelles"] == [
        {"section": "AL", "numero": "74"},
        {"section": "AL", "numero": "417"},
        {"section": "AL", "numero": "416"},
    ]
    assert [official_label(p["section"], p["numero"]) for p in parsed["parcelles"]] == [
        "AL 0074",
        "AL 0417",
        "AL 0416",
    ]


def test_a_minuscule_sans_contexte_rejete() -> None:
    assert _pairs("regarde a 7 reprises") == []


def test_identity_prompt_contient_le_format_officiel() -> None:
    text = format_parcel_identity_prompt({
        "error": None,
        "parcelle": {
            "idu": "66008000AL0074",
            "section": "AL",
            "numero": "74",
            "contenance": 5460,
            "superficie_m2": 5459.47,
        },
        "parcelles": [{
            "idu": "66008000AL0074",
            "section": "AL",
            "numero": "74",
            "contenance": 5460,
            "superficie_m2": 5459.47,
        }],
    })
    assert "AL 0074" in text
    assert "66008000AL0074" in text
    assert "get_contexte_parcelle" in text
