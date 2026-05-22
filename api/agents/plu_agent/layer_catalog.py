"""
layer_catalog.py — catalogues de couches spatiales par commune (JSON).

Responsabilité
--------------
Charger et fusionner les définitions de couches :
  - `communes/catalogs/default.json` — socle GPU (toutes communes) ;
  - `communes/catalogs/<slug>.json` — ajouts / désactivations / renommages.

Chaque couche (`LayerSpec`) décrit comment faire l'intersection géométrique
et quoi exposer au LLM / à la carte. Les modules `cartography/spatial_context.py`,
    `cartography/carto.py` et `contexte_parcelle.py` s'appuient sur ce catalogue ;
    les modules `prescriptions.py` / `servitudes.py` / `infos.py` lisent la config via
    `catalog_bridge.py` ; les couches hors GPU passent par `fetch_layer.py`.

Modifier une commune = éditer un JSON, redémarrer l'API (ou reload uvicorn).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_CATALOGS_DIR = Path(__file__).resolve().parent / "communes" / "catalogs"


@dataclass(frozen=True)
class LayerSpec:
    """Une couche PostGIS intersectable avec l'unité foncière."""

    id: str
    enabled: bool = True
    table: str = ""
    """Nom de table sans schéma (qualifié via `CommuneProfile.schema`)."""

    geom_column: str = "geom_2154"
    """Colonne géométrie (`geom_2154` ou `geometry` pour certaines SUP)."""

    geom_transform: str = "make_valid_2154"
    """
    make_valid_2154 — ST_MakeValid(p.geom_2154)
    sup_geometry    — logique servitudes (SRID 2154 / transform)
    """

    group: str = ""
    """Famille : zonage, prescriptions, servitudes, informations, parcelle, …"""

    subgroup: str | None = None
    """Sous-clé carte / LLM (ex. surfaciques, lineaires)."""

    kind: str | None = None
    color: str | None = None

    strict_parcel: bool = True
    """True = intersection stricte parcelle (hors bordure) ; False = buffer zonage."""

    inclu_buffer: bool = False
    """
    Carte : si True, sélection des entités dans le buffer (ex. fossés le long de la parcelle).
    Si False (défaut), sélection = intersection stricte parcelle ; buffer sert uniquement au clip GeoJSON.
    Le zonage GPU utilise buffer_m + strict_parcel:false (hors fetch_layer).
    """

    buffer_m: float | None = None
    """Buffer affichage carto (zonage uniquement, ex. 100)."""

    context_llm: bool = True
    context_carto: bool = True

    attributes: tuple[str, ...] = ()
    """Colonnes SQL à sélectionner (en plus de la géométrie)."""

    attribute_labels: tuple[str, ...] = ()
    """Libellés lisibles pour le LLM (même ordre que `attributes` / ancien clean_attributes)."""

    title: str | None = None
    """Nom affiché de la couche (ancien champ `nom` du catalogue identité)."""

    reglement_table: str | None = None
    """Jointure zonage → plu_reglement (optionnel)."""

    optional: bool = False
    """Si True, erreur SQL table absente → couche ignorée (comme infos_*)."""


@dataclass(frozen=True)
class LayerCatalog:
    """Catalogue résolu pour une commune (défaut + surcharges)."""

    slug: str
    layers: dict[str, LayerSpec] = field(default_factory=dict)

    def enabled_layers(self) -> list[LayerSpec]:
        return [L for L in self.layers.values() if L.enabled]

    def by_group(self, group: str) -> list[LayerSpec]:
        return [L for L in self.enabled_layers() if L.group == group]

    def get(self, layer_id: str) -> LayerSpec | None:
        return self.layers.get(layer_id)

    def is_enabled(self, layer_id: str) -> bool:
        L = self.layers.get(layer_id)
        return bool(L and L.enabled)


def _deep_merge_layer(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in patch.items():
        if k == "_comment":
            continue
        out[k] = v
    return out


def _parse_layer(layer_id: str, raw: dict[str, Any]) -> LayerSpec:
    # Rétrocompat : keep = colonnes SQL, clean_attributes = libellés LLM
    attrs = raw.get("attributes") or raw.get("keep") or []
    labels = raw.get("attribute_labels") or raw.get("clean_attributes") or []
    if isinstance(attrs, list) and isinstance(labels, list) and labels and len(labels) != len(attrs):
        raise ValueError(
            f"Couche {layer_id!r} : attributes/keep ({len(attrs)}) et "
            f"clean_attributes ({len(labels)}) doivent avoir la même longueur."
        )
    title = raw.get("title") or raw.get("nom")
    return LayerSpec(
        id=layer_id,
        enabled=bool(raw.get("enabled", True)),
        table=str(raw.get("table", layer_id)),
        geom_column=str(raw.get("geom_column", "geom_2154")),
        geom_transform=str(raw.get("geom_transform", "make_valid_2154")),
        group=str(raw.get("group", "")),
        subgroup=raw.get("subgroup"),
        kind=raw.get("kind"),
        color=raw.get("color"),
        strict_parcel=bool(raw.get("strict_parcel", True)),
        buffer_m=raw.get("buffer_m"),
        context_llm=bool(raw.get("context_llm", True)),
        context_carto=bool(raw.get("context_carto", True)),
        attributes=tuple(attrs) if isinstance(attrs, list) else (),
        attribute_labels=tuple(labels) if isinstance(labels, list) else (),
        title=str(title) if title else None,
        reglement_table=raw.get("reglement_table"),
        optional=bool(raw.get("optional", False)),
        inclu_buffer=bool(raw.get("inclu_buffer", False)),
    )


def load_catalog_file(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    layers = data.get("layers") or {}
    if not isinstance(layers, dict):
        raise ValueError(f"{path}: clé 'layers' invalide")
    return layers


def load_commune_catalog(slug: str) -> LayerCatalog:
    """
    Fusionne default.json + <slug>.json.

    Dans <slug>.json :
      - `"layers": { "id": { ... } }` — merge champ par champ sur le défaut ;
      - `"layers": { "id": { "enabled": false } }` — désactive une couche ;
      - nouvelle clé `"id"` — ajoute une couche (ex. pprt_surf).
    """
    default_raw = load_catalog_file(_CATALOGS_DIR / "default.json")
    commune_raw = load_catalog_file(_CATALOGS_DIR / f"{slug}.json")

    merged: dict[str, dict[str, Any]] = {}
    for layer_id, spec in default_raw.items():
        merged[layer_id] = dict(spec)

    for layer_id, patch in commune_raw.items():
        if layer_id.startswith("_"):
            continue
        if layer_id in merged:
            merged[layer_id] = _deep_merge_layer(merged[layer_id], patch)
        else:
            merged[layer_id] = dict(patch)

    layers = {
        lid: _parse_layer(lid, raw)
        for lid, raw in merged.items()
    }
    return LayerCatalog(slug=slug, layers=layers)
