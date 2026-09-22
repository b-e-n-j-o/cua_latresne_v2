"""Fusionne les extras LLM aux attributs identiques (galeries fragmentées, etc.)."""

from __future__ import annotations

_METRIC_KEYS = frozenset({
    "pct_parcelle_couverte",
    "superficie_intersection_m2",
    "longueur_intersection_m",
    "nb_entites",
})


def _identity_key(item: dict) -> tuple:
    return tuple(
        (k, item[k])
        for k in sorted(item)
        if k not in _METRIC_KEYS
    )


def collapse_identical_llm_items(items: list[dict]) -> list[dict]:
    """Une entrée par jeu d'attributs identiques + nb_entites (contexte LLM)."""
    if len(items) <= 1:
        return items
    buckets: dict[tuple, list[dict]] = {}
    order: list[tuple] = []
    for item in items:
        key = _identity_key(item)
        if key not in buckets:
            order.append(key)
            buckets[key] = []
        buckets[key].append(item)
    out: list[dict] = []
    for key in order:
        group = buckets[key]
        merged = dict(group[0])
        n = len(group)
        if n > 1:
            merged["nb_entites"] = n
            if any(g.get("superficie_intersection_m2") is not None for g in group):
                merged["superficie_intersection_m2"] = round(
                    sum(float(g.get("superficie_intersection_m2") or 0) for g in group),
                    1,
                )
            if any(g.get("longueur_intersection_m") is not None for g in group):
                merged["longueur_intersection_m"] = round(
                    sum(float(g.get("longueur_intersection_m") or 0) for g in group),
                    1,
                )
            pcts = [
                float(g["pct_parcelle_couverte"])
                for g in group
                if g.get("pct_parcelle_couverte") is not None
            ]
            if pcts:
                merged["pct_parcelle_couverte"] = round(min(100.0, sum(pcts)), 1)
        out.append(merged)
    return out
