# -*- coding: utf-8 -*-
"""
Notes d'intersection partielle (option C1) — commune-agnostique.

Règles :
  - ≤ 1 % : filtré en amont (min_pct_sig), pas de note ;
  - ]1 % ; 5 %] : « Intersection partielle de X % de l'unité foncière. » ;
  - > 5 % : pas de note, sauf si plusieurs entités sur l'UF (multi-zones).
"""

from __future__ import annotations

from typing import Any, Optional

# Bornes affichage (le filtre SQL utilise min_pct_sig > 1 %).
PARTIAL_PCT_MIN = 1.0
PARTIAL_PCT_MAX = 5.0


def catalogue_affiche_pct_partiel(cfg: dict | None) -> bool:
    """True si le catalogue active la note d'intersection partielle pour la couche."""
    return bool((cfg or {}).get("afficher_pct_sig_partiel"))


def pct_sig_objet(obj: dict | None) -> float:
    try:
        return float((obj or {}).get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def est_multi_entites(objets: list | None) -> bool:
    """Plusieurs entités distinctes intersectent l'UF (multi-zones)."""
    return len(objets or []) > 1


def texte_note_intersection_partielle(
    pct: float,
    *,
    multi_entites: bool = False,
    enabled: bool = True,
) -> Optional[str]:
    """
    Texte de note pour une intersection surfacique, ou None si pas d'affichage.

    multi_entites : plusieurs entités sur l'UF — autorise la note au-delà de 5 %.
    """
    if not enabled:
        return None
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return None
    if p <= PARTIAL_PCT_MIN:
        return None
    if PARTIAL_PCT_MIN < p <= PARTIAL_PCT_MAX:
        return f"Intersection partielle de {p:.2f} % de l'unité foncière."
    if multi_entites and p > PARTIAL_PCT_MAX:
        return f"Intersection de {p:.2f} % de l'unité foncière."
    return None


def note_pour_objet(
    obj: dict,
    *,
    multi_entites: bool = False,
    enabled: bool = True,
) -> Optional[str]:
    """Note d'intersection partielle pour un objet intersecté (pct_sig sur l'objet)."""
    return texte_note_intersection_partielle(
        pct_sig_objet(obj),
        multi_entites=multi_entites,
        enabled=enabled,
    )


def pct_sig_servitude(servitude: dict, *, surface_sig: float = 0.0) -> float:
    """pct_sig direct, ou dérivé de metric / surface UF."""
    pct = pct_sig_objet(servitude)
    if pct > 0:
        return pct
    metric = servitude.get("metric")
    if metric is not None and surface_sig > 0:
        try:
            return float(metric) / float(surface_sig) * 100
        except (TypeError, ValueError):
            return 0.0
    return 0.0


def note_pour_servitude(
    servitude: dict,
    *,
    multi_entites_couche: bool = False,
    enabled: bool = True,
    surface_sig: float = 0.0,
) -> Optional[str]:
    """
    Note pour une servitude agrégée.

    multi_entites sur la servitude (nb_fragments > 1) ou sur la couche SUP.
    """
    multi = multi_entites_couche or int(servitude.get("nb_fragments") or 0) > 1
    return texte_note_intersection_partielle(
        pct_sig_servitude(servitude, surface_sig=surface_sig),
        multi_entites=multi,
        enabled=enabled,
    )
