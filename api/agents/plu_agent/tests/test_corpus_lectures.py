"""Tests unitaires lectures corpus (sans base)."""

from api.agents.plu_agent.corpus import concatener_reglements


def test_concatener_un_seul_texte_sans_titre():
    out = concatener_reglements([
        {"zone_code": "UB", "reglementation": "Hauteur 9 m.", "titre": None},
    ])
    assert out == "Hauteur 9 m."


def test_concatener_ua_deux_secteurs():
    out = concatener_reglements([
        {"zone_code": "UAa", "titre": "Secteur a", "reglementation": "Texte A"},
        {"zone_code": "UAb", "titre": "Secteur b", "reglementation": "Texte B"},
    ])
    assert "### UAa — Secteur a" in out
    assert "Texte A" in out
    assert "Texte B" in out
    assert out.count("---") == 1


def test_concatener_vide():
    assert concatener_reglements([]) is None
    assert concatener_reglements([{"reglementation": "  "}]) is None
