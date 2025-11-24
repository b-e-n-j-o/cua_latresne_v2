#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
carte2d_metier.py
----------------------------------------------------
Responsable de :
- transformer les entités brutes SQL (features)
  en objets prêts à être rendus sur la carte Folium
- gérer les deux modes :
    • mode ENTIRE (pas d’attribut_map)
    • mode GROUPED (attribut_map → groupes)
- gérer attribut_split pour sous-groupes
- créer les GeoJson & tooltips / popups
- remplir registry["layers"] dans le format attendu
"""

import json
import logging
import folium

from CUA.map_utils import clean_properties

logger = logging.getLogger("carte2d.metier")
logger.setLevel(logging.INFO)


# ============================================================
# 🔧 Construction d'un popup harmonisé (type PPRI + PLU)
# ============================================================

def construire_popup_html(nom_couche, props_full, couleur_bordure, reglement=None):
    """Construit un popup cohérent avec ton ancien système."""
    if reglement:
        contenu = f"""
        <div style="width:450px;max-height:400px;overflow-y:auto;
                    padding:10px;border-left:4px solid {couleur_bordure};">
            <h4 style="margin-top:0;color:#003366;">{nom_couche}</h4>
            <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                {reglement.strip()}
            </p>
        </div>
        """
    else:
        # Construire un listing HTML propre
        liste = ""
        for k, v in props_full.items():
            if k == "__layer_name__":
                continue
            label = k.replace("_", " ").title()
            liste += f"<b>{label}:</b> {v}<br>"

        contenu = f"""
        <div style="width:450px;max-height:400px;overflow-y:auto;
                    padding:10px;border-left:4px solid {couleur_bordure};">
            <h4 style="margin-top:0;color:#003366;">{nom_couche}</h4>
            <p style="font-size:13px;color:#333;white-space:pre-wrap;line-height:1.4;">
                {liste}
            </p>
        </div>
        """
    return contenu


# ============================================================
# 🔧 Tooltip (résumé)
# ============================================================

def construire_tooltip_html(props_clean):
    html = (
        '<div style="background:white;color:#111;font-size:12px;'
        'border-radius:4px;padding:8px;border:1px solid #ccc;">'
    )
    for k, v in props_clean.items():
        if k == "__layer_name__":
            continue
        alias = f"{k}:"
        valeur = str(v)
        if len(valeur) > 50:
            valeur = valeur[:50] + "..."
        html += f"<div><strong>{alias}</strong> {valeur}</div>"
    html += '<div class="tooltip-footer">👆 Cliquer pour afficher plus</div></div>'
    return html


# ============================================================
# 🧩 Construction d'une couche métier
# ============================================================

def construire_couche_metier(
    table: str,
    config: dict,
    rows: list,
    keys: list,
    registry: dict,
    catalogue: dict,
    random_color_fn,
    *,
    map_obj,   # 🔥 obligatoire et keyword-only
):
    """
    Reprend fidèlement ton ancien add_layer(), mais en mode modulaire.
    On ajoute ici directement les GeoJson à la carte Folium.
    """

    nom = config.get("nom", table)
    keep = config.get("keep", [])
    attribut_map = config.get("attribut_map", None)
    attribut_split = config.get("attribut_split", None)
    color = random_color_fn()

    logger.info(f"🎨 Construction couche métier : {nom}")

    # type de couche (pour couleur bordure popup)
    type_couche = catalogue.get(table, {}).get("type", "")
    couleur_bordure = {
        "Zonage PLU": "#27ae60",
        "Servitudes": "#2980b9",
        "Prescriptions": "#8e44ad",
        "Informations": "#e67e22",
    }.get(type_couche, "#7f8c8d")

    mode_entire = (
        attribut_map is None
        or attribut_map == ""
        or str(attribut_map).lower() == "none"
    )

    if mode_entire:
        logger.info("   → Mode ENTIRE")
    else:
        logger.info(f"   → Mode GROUPED par '{attribut_map}'")
        if attribut_split:
            logger.info(f"   → Sous-groupes via attribut_split = '{attribut_split}'")

    # ============================================================
    # 📌 MODE ENTIRE (toute la couche = un seul groupe)
    # ============================================================

    if mode_entire:
        all_feat_vars = []
        count_entites = 0

        for i, row in enumerate(rows):
            geom = json.loads(row[0])
            props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys)-1)}

            props_clean = clean_properties(props_raw, nom)

            # props_full sans IDs
            ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
            props_full = {
                k: v for k, v in props_raw.items()
                if not any(p in k.lower() for p in ignore_patterns)
            }
            props_full = {"__layer_name__": nom, **props_full}

            # Détecter un éventuel champ "reglementation"
            reglement = None
            for k, v in props_full.items():
                if "reglementation" in k.lower():
                    reglement = v
                    break

            # Tooltip
            tooltip_html = construire_tooltip_html(props_clean)

            # Popup
            popup_html = construire_popup_html(
                nom_couche=nom,
                props_full=props_full,
                couleur_bordure=couleur_bordure,
                reglement=reglement
            )

            # Feature Folium
            feature = {"type": "Feature", "geometry": geom, "properties": props_clean}

            gj = folium.GeoJson(
                {"type": "FeatureCollection", "features": [feature]},
                style_function=lambda x, c=color: {"color": c, "weight": 2, "fillOpacity": 0.35},
                highlight_function=lambda x, c=color: {"weight": 4, "fillOpacity": 0.65},
                tooltip=folium.Tooltip(tooltip_html),
                name=f"{table}_ent_{i}",  # identifiant unique
                show=False,
            )
            gj.add_to(map_obj)
            folium.Popup(popup_html, max_width=480).add_to(gj)

            all_feat_vars.append(gj.get_name())
            count_entites += 1

        # Ajouter au registry
        registry["layers"].append({
            "name": nom,
            "color": color,
            "mode": "entire",
            "attribut_map": None,
            "nom_attribut_map": "",
            "attribut_split": None,
            "entities": [
                {
                    "name": nom,
                    "vars": all_feat_vars,
                    "count": count_entites,
                }
            ],
        })

        return


    # ============================================================
    # 📌 MODE GROUPED (groupement par attribut_map)
    # ============================================================

    grouped = {}
    split_values = {}

    for idx, row in enumerate(rows, start=1):
        geom = json.loads(row[0])
        props_raw = {keys[j + 1]: str(row[j + 1]) for j in range(len(keys)-1)}

        props_clean = clean_properties(props_raw, nom)

        ignore_patterns = ["id", "uuid", "gid", "fid", "globalid"]
        props_full = {
            k: v for k, v in props_raw.items()
            if not any(p in k.lower() for p in ignore_patterns)
        }
        props_full = {"__layer_name__": nom, **props_full}

        # détecter reglementation
        reglement = None
        for k, v in props_full.items():
            if "reglementation" in k.lower():
                reglement = v
                break

        # valeur groupement
        if attribut_map in props_clean:
            group_value = props_clean[attribut_map]
            if not group_value or group_value.lower() in ["none", "null", ""]:
                group_value = f"Entité #{idx}"
        else:
            # fallback intelligent
            group_value = next(
                (
                    v for k, v in props_clean.items()
                    if v and v.lower() not in ["none", "null"]
                    and k.lower() not in {"id", "gid", "uuid", "fid"}
                    and k != "__layer_name__"
                ),
                f"Entité #{idx}",
            )

        # attribut_split (sous-groupes)
        split_value = None
        if attribut_split and attribut_split in props_clean:
            split_value = props_clean[attribut_split]
            if not split_value or split_value.lower() in ["none", "null", ""]:
                split_value = "Autres"

        # Tooltip
        tooltip_html = construire_tooltip_html(props_clean)

        # Popup
        popup_html = construire_popup_html(
            nom_couche=nom,
            props_full=props_full,
            couleur_bordure=couleur_bordure,
            reglement=reglement
        )

        # Feature Folium
        feature = {"type": "Feature", "geometry": geom, "properties": props_clean}

        entity_id = f"{table}_grp_{idx}"

        gj = folium.GeoJson(
            {"type": "FeatureCollection", "features": [feature]},
            name=entity_id,
            style_function=lambda x, c=color: {"color": c, "weight": 2, "fillOpacity": 0.35},
            highlight_function=lambda x, c=color: {"weight": 4, "fillOpacity": 0.65},
            tooltip=folium.Tooltip(tooltip_html),
            show=False,
        )
        gj.add_to(map_obj)
        folium.Popup(popup_html, max_width=480).add_to(gj)

        if group_value not in grouped:
            grouped[group_value] = []
        grouped[group_value].append(gj.get_name())

        split_values[gj.get_name()] = split_value

    # assembler final
    entities = []
    for group_value, var_list in grouped.items():
        if attribut_split:
            first_var = var_list[0]
            split_val = split_values.get(first_var, "Autres")
        else:
            split_val = None

        entities.append({
            "name": group_value,
            "vars": var_list,
            "count": len(var_list),
            "split_value": split_val,
        })

    registry["layers"].append({
        "name": nom,
        "color": color,
        "mode": "grouped",
        "attribut_map": attribut_map,
        "nom_attribut_map": config.get("nom_attribut_map", ""),
        "attribut_split": attribut_split,
        "entities": entities,
    })
