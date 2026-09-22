"""Mise en forme de la matrice parcelle × couche (sans I/O)."""

from __future__ import annotations

_SKIP_ATTR = frozenset({
    "layer_id",
    "group",
    "couche",
    "kind",
    "subgroup",
    "réglementation",
    "reglementation",
    "txt",
    "laius_reglement",
    "legende",
    "légende",
})


def thin_repartition_attrs(item: dict) -> dict:
    out: dict = {}
    for key, val in item.items():
        if key in _SKIP_ATTR or key.casefold() in _SKIP_ATTR:
            continue
        if val is None or val == "":
            continue
        out[key] = val
    return out


def parcel_repartition_entry(official: str, elements: list[dict]) -> dict:
    if not elements:
        return {"official": official, "intersecte": False}
    cleaned = [item for item in elements if item]
    if cleaned:
        return {"official": official, "intersecte": True, "elements": cleaned}
    return {"official": official, "intersecte": True}
