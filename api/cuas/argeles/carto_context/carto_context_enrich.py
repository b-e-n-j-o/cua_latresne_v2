# -*- coding: utf-8 -*-
"""Enrichissement features / légende — aligné studyZone (frontend Argelès)."""

from __future__ import annotations

import copy
import hashlib
from typing import Any

EMPTY_GROUP_KEY = "__empty__"

ZONAGE_COLORS = {
    "U": "#d64545",
    "AU": "#efa59c",
    "A": "#f2c14e",
    "N": "#5a9367",
}
ZONAGE_LABELS = {
    "U": "Zones U (urbaines)",
    "AU": "AU (à urbaniser)",
    "A": "A (agricole)",
    "N": "N (naturelle)",
}

PRESCRIPTION_PALETTE = [
    "#9D4EDD", "#7B2CBF", "#5A189A", "#C77DFF", "#B388EB",
    "#E0AAFF", "#7209B7", "#560BAD", "#480CA8", "#3F37C9",
]
INFOS_PALETTE = [
    "#0D9488", "#14B8A6", "#2DD4BF", "#5EEAD4", "#134E4A",
    "#115E59", "#0F766E", "#047857", "#059669", "#10B981",
]
SERVITUDE_PALETTE = [
    "#457B9D", "#1D3557", "#2A6F97", "#468FAF", "#61A5C2",
    "#89C2D9", "#33658A", "#264653", "#2C699A", "#048BA8",
]
RISQUE_PALETTE = [
    "#E76F51", "#F4A261", "#E9C46A", "#D62828", "#BC4749",
    "#9B2226", "#AE2012", "#BB3E03", "#CA6702", "#EE9B00",
]
ENV_PALETTE = [
    "#1B4332", "#2D6A4F", "#40916C", "#52B788", "#74C69D",
    "#95D5B2", "#344E41", "#588157", "#3A5A40", "#A7C957",
]
RESEAU_PALETTE = [
    "#0077B6", "#0096C7", "#00B4D8", "#48CAE4", "#023E8A",
    "#03045E", "#0077B6", "#1D3557", "#457B9D", "#2A6F97",
]

FAMILY_FALLBACK = {
    "zonages_plu": "#bdbdbd",
    "prescriptions": "#9D4EDD",
    "informations": "#0D9488",
    "servitudes": "#457B9D",
    "risques": "#E76F51",
    "environnement": "#2D6A4F",
    "reseaux": "#0077B6",
    "cadastre": "#6b7280",
    "_other": "#9ca3af",
}

FAMILY_PALETTE = {
    "prescriptions": PRESCRIPTION_PALETTE,
    "informations": INFOS_PALETTE,
    "servitudes": SERVITUDE_PALETTE,
    "risques": RISQUE_PALETTE,
    "environnement": ENV_PALETTE,
    "reseaux": RESEAU_PALETTE,
}


def _norm_key(raw: Any) -> str:
    s = str(raw or "").strip()
    if not s or s.lower() in ("none", "null", "nan"):
        return EMPTY_GROUP_KEY
    return s.upper()


def _is_zonage(layer_id: str, carto_meta: dict) -> bool:
    return carto_meta.get("legend") == "zonage" or layer_id == "zonage_plu"


def _discriminant_field(carto_meta: dict) -> str | None:
    g = carto_meta.get("group")
    if g:
        return str(g)
    t = carto_meta.get("tip")
    return str(t) if t else None


def _zonage_group_key(props: dict) -> str:
    code = _norm_key(
        props.get("libelle") or props.get("zonage_reglement") or props.get("typezone")
    )
    if code == EMPTY_GROUP_KEY:
        return EMPTY_GROUP_KEY
    if code.startswith("AU") or code.startswith("1AU") or code.startswith("2AU"):
        return "AU"
    if code.startswith("N"):
        return "N"
    if code.startswith("A"):
        return "A"
    return "U"


def _group_key(layer_id: str, carto_meta: dict, props: dict) -> str:
    if _is_zonage(layer_id, carto_meta):
        return _zonage_group_key(props)
    field = _discriminant_field(carto_meta)
    if not field:
        return "_all"
    return _group_key_from_raw(props.get(field))


def _group_key_from_raw(raw: Any) -> str:
    return _norm_key(raw)


def _color_for_key(key: str, palette: list[str], fallback: str) -> str:
    if key == EMPTY_GROUP_KEY:
        return fallback
    if key in ("U", "AU", "A", "N"):
        return ZONAGE_COLORS.get(key, fallback)
    idx = int(hashlib.md5(key.encode()).hexdigest(), 16) % len(palette)
    return palette[idx]


def _group_color(layer_id: str, carto_meta: dict, key: str, props: dict) -> str:
    family = carto_meta.get("family") or "_other"
    fallback = FAMILY_FALLBACK.get(family, FAMILY_FALLBACK["_other"])
    if _is_zonage(layer_id, carto_meta):
        if props:
            return ZONAGE_COLORS.get(_zonage_group_key(props), fallback)
        return ZONAGE_COLORS.get(key, fallback)
    palette = FAMILY_PALETTE.get(family, [fallback])
    return _color_for_key(key, palette, fallback)


def _group_label(
    layer_id: str,
    carto_meta: dict,
    key: str,
    props: dict,
    field: str | None,
) -> str:
    if key == EMPTY_GROUP_KEY:
        return "Non renseigné"
    if _is_zonage(layer_id, carto_meta):
        return ZONAGE_LABELS.get(key, key)
    if field and props.get(field) not in (None, ""):
        return str(props[field]).strip()
    tip = carto_meta.get("tip")
    if tip and props.get(tip) not in (None, ""):
        return str(props[tip]).strip()
    return key


def _layer_filterable(layer_id: str, carto_meta: dict) -> bool:
    if carto_meta.get("legend") == "simple":
        return False
    return bool(_discriminant_field(carto_meta)) or _is_zonage(layer_id, carto_meta)


def _field_label(layer_id: str, carto_meta: dict, field: str | None) -> str:
    if _is_zonage(layer_id, carto_meta):
        return "Type de zone (U / AU / A / N)"
    if carto_meta.get("group"):
        return str(carto_meta["group"])
    return field or "—"


def build_layer_legends(
    layer_id: str,
    layer: dict,
    carto_meta: dict,
) -> dict[str, Any]:
    """Construit métadonnées légende + features enrichies pour une couche."""
    features = copy.deepcopy((layer.get("features") or {}).get("features") or [])
    field = _discriminant_field(carto_meta)
    filterable = _layer_filterable(layer_id, carto_meta)

    counts: dict[str, int] = {}
    samples: dict[str, dict] = {}

    for feat in features:
        props = feat.setdefault("properties", {})
        gkey = _group_key(layer_id, carto_meta, props)
        props["_studyKey"] = gkey
        props["_studyColor"] = _group_color(layer_id, carto_meta, gkey, props)
        props["_studyLabel"] = _group_label(layer_id, carto_meta, gkey, props, field)
        props["_layerId"] = layer_id
        counts[gkey] = counts.get(gkey, 0) + 1
        samples.setdefault(gkey, props)

    legend_items: list[dict[str, Any]] = []
    if _is_zonage(layer_id, carto_meta):
        for zkey in ("U", "AU", "A", "N"):
            legend_items.append({
                "key": zkey,
                "label": ZONAGE_LABELS[zkey],
                "color": ZONAGE_COLORS[zkey],
                "count": counts.get(zkey, 0),
            })
    elif filterable and field:
        for gkey in sorted(counts.keys(), key=lambda k: (k == EMPTY_GROUP_KEY, k)):
            props = samples[gkey]
            legend_items.append({
                "key": gkey,
                "label": _group_label(layer_id, carto_meta, gkey, props, field),
                "color": _group_color(layer_id, carto_meta, gkey, props),
                "count": counts[gkey],
            })
    elif features:
        fam = carto_meta.get("family") or "_other"
        legend_items.append({
            "key": "_all",
            "label": layer.get("title") or layer_id,
            "color": FAMILY_FALLBACK.get(fam, "#888888"),
            "count": len(features),
        })

    return {
        "layer_id": layer_id,
        "title": layer.get("title") or layer_id,
        "family": carto_meta.get("family") or "_other",
        "family_title": layer.get("family_title") or carto_meta.get("family") or "Autres",
        "geom_type": layer.get("geom_type") or carto_meta.get("geom") or "surfacique",
        "tip": carto_meta.get("tip"),
        "group": carto_meta.get("group"),
        "legend": carto_meta.get("legend"),
        "filterable": filterable and len(legend_items) > 1,
        "field_label": _field_label(layer_id, carto_meta, field),
        "legend_items": legend_items,
        "features": features,
        "count": len(features),
    }


def prepare_layers_payload(
    context: dict,
    carto_catalogue: dict | None = None,
) -> list[dict]:
    carto_layers = (carto_catalogue or {}).get("layers") or {}
    families = {
        f["id"]: f["title"]
        for f in (carto_catalogue or {}).get("families") or []
    }
    out: list[dict] = []
    for layer_id, layer in (context.get("layers") or {}).items():
        raw_features = (layer.get("features") or {}).get("features") or []
        if not raw_features:
            continue
        carto_meta = carto_layers.get(layer_id) or {}
        layer = dict(layer)
        layer["family_title"] = families.get(
            layer.get("family") or carto_meta.get("family") or "_other", "Autres"
        )
        out.append(build_layer_legends(layer_id, layer, carto_meta))
    out.sort(key=lambda x: (x["family_title"], x["title"]))
    return out
