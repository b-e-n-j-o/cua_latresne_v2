# -*- coding: utf-8 -*-
"""
ppri_module.py
Module d'intégration PPRI pour cartes Folium externes,
avec sous-couches :
 - Avant réglementation
 - Après réglementation
 - Zones d’absorption réglementaires
"""

import folium
import geopandas as gpd
import json
from sqlalchemy import text
from CUA.ppri_analyse_tolerance import analyser_ppri_tolerance

# ============================================================
# 🎨 Couleurs PPRI
# ============================================================
COULEURS_PPRI = {
    "Grenat": "#7f0000",
    "Rouge": "#ff4c4c",
    "Rouge urbanisé": "#ff8080",
    "Rouge non urbanisé": "#ff3333",
    "Bleu": "#1e90ff",
    "Bleu clair": "#87cefa",
    "Byzantin": "#4682b4",
}


def couleur_ppri(nom: str):
    for k, v in COULEURS_PPRI.items():
        if k.lower() in nom.lower():
            return v
    return "#cccccc"


def ajouter_ppri_a_carte(
    map_folium,
    section=None,
    numero=None,
    code_insee=None,
    ppri_table=None,
    engine=None,
    show=True,
    nom_groupe=None,
    registry=None,
    geom_wkt=None  # ✅ ajouté
):
    """
    Ajoute l'analyse PPRI à la carte Folium existante, sous forme
    de 3 sous-couches contrôlables séparément dans la légende custom :

    1. Avant réglementation        (ppri_initial brut)
    2. Après réglementation        (ppri_conservees = carte finale après absorption)
    3. Zones d’absorption          (zones_remplacement issues des buffers ±2.5 m)

    Args:
        map_folium: objet folium.Map
        section, numero, code_insee: identifiants de la parcelle
        ppri_table: nom de la table PPRI dans la base
        engine: moteur SQLAlchemy
        show: bool -> affiche la couche "Après réglementation" par défaut
        nom_groupe: nom du groupe à afficher dans la légende
        registry: le registry global utilisé pour générer la légende interactive

    Returns:
        dict: métadonnées utiles (stats, succès, etc.)
    """

    print("\n🌊 Intégration du PPRI (avec sous-couches)…")

    # ============================================================
    # 🛡️ FALLBACK AUTOMATIQUE SI ppri_table MANQUANT
    # ============================================================
    if not ppri_table:
        ppri_table = "latresne.pm1_detaillee_gironde"
        print("⚠️ Aucun ppri_table fourni — utilisation du PPRI Latresne par défaut.")

    # ============================================================
    # 1️⃣ Lancer l'analyse PPRI complète
    # ============================================================
    resultats = analyser_ppri_tolerance(
        section=section,
        numero=numero,
        code_insee=code_insee,
        ppri_table=ppri_table,
        engine=engine,
        geom_wkt=geom_wkt  # ✅ nouveau
    )

    # Récupération des jeux intermédiaires
    ppri_initial = resultats["zones"]["ppri_initial"]          # zones brutes intersectées
    ppri_filtre = resultats["zones"]["ppri_filtre"]            # filtrées + nettoyage
    cons = resultats["zones"]["ppri_conservees"]               # zones conservées finales
    abs_data = resultats["zones"]["ppri_absorbees"]            # zones absorbées
    buf = resultats["zones"]["ppri_buffer"]                    # buffers 2.5m autour des zones
    stats = resultats["statistiques"]

    # Les réglementations sont déjà présentes dans les données depuis analyser_ppri_tolerance()
    print("   ✅ Réglementations déjà présentes dans les zones PPRI")
    if not cons.empty and "reglementation" in cons.columns:
        print(f"   📋 Colonnes disponibles : {cons.columns.tolist()}")

    # ============================================================
    # 2️⃣ Calcul des zones de remplacement (zones absorbées,
    #     réattribuées aux zones absorbantes via les buffers)
    # ============================================================
    zones_remplacement = []
    if not abs_data.empty:
        # noms des zones finales (conservées)
        zones_conservees_names = set(cons["codezone"].unique())

        for idx, zone_abs in abs_data.iterrows():
            geom_abs = zone_abs.geometry
            absorbeurs = zone_abs.get("absorbeurs", [])
            contributions = zone_abs.get("contributions", {})

            if not absorbeurs:
                continue

            for absorbeur in absorbeurs:
                if absorbeur not in zones_conservees_names:
                    continue

                # retrouver dans le ppri_filtre les zones absorbantes correspondantes
                zone_absorbante_idx = ppri_filtre[ppri_filtre["codezone"] == absorbeur].index

                for idx_abs in zone_absorbante_idx:
                    # buffer autour de la zone absorbante
                    buffer_absorbante = buf.loc[idx_abs, "geometry"]

                    # on garde juste l'intersection entre la petite zone absorbée
                    # et la "zone d'influence" de l'absorbeur
                    zone_replace = geom_abs.intersection(buffer_absorbante)

                    if not zone_replace.is_empty and zone_replace.area >= 1.0:
                        contrib_pct = contributions.get(absorbeur, 0)
                        zones_remplacement.append({
                            "codezone_origine": zone_abs.codezone,
                            "codezone": absorbeur,
                            "geometry": zone_replace,
                            "contribution_pct": contrib_pct
                        })
                        break  # on a déjà affecté cette zone_abs à un absorbeur valable

    # ============================================================
    # 3️⃣ Passage en WGS84 de chaque couche géo
    # ============================================================
    init_wgs = ppri_initial.to_crs("EPSG:4326") if not ppri_initial.empty else gpd.GeoDataFrame()
    cons_wgs = cons.to_crs("EPSG:4326") if not cons.empty else gpd.GeoDataFrame()

    if zones_remplacement:
        gdf_remplacement = gpd.GeoDataFrame(
            zones_remplacement,
            geometry="geometry",
            crs=2154
        )
        remp_wgs = gdf_remplacement.to_crs("EPSG:4326")
    else:
        remp_wgs = gpd.GeoDataFrame()

    # Note: buf (les buffers 2.5m) existe, mais pour l’instant on ne l’affiche pas
    # comme couche séparée, car le besoin métier exprimé est:
    # "Zones d’absorption réglementaires" = zones_remplacement.
    # Si tu veux voir les buffers bruts visuellement aussi, on pourrait
    # en faire une 4e sous-couche plus tard.

    # ============================================================
    # 4️⃣ Création des FeatureGroups Folium pour les 3 sous-couches
    # ============================================================
    # On choisit quelle sous-couche est visible par défaut.
    # show=True -> "Après réglementation" visible au départ.
    group_avant = folium.FeatureGroup(
        name="PPRI – Avant réglementation",
        show=False  # masquée au chargement
    )
    group_apres = folium.FeatureGroup(
        name="PPRI – Après réglementation",
        show=bool(show)  # visible par défaut
    )
    group_absorption = folium.FeatureGroup(
        name="Zones d’absorption réglementaires",
        show=False  # masquée au chargement
    )

    # ------------------------------------------------------------
    # (a) Couche AVANT : ppri_initial brut
    # ------------------------------------------------------------
    for _, row in init_wgs.iterrows():
        code = row["codezone"]
        color = couleur_ppri(code)
        reglement = row.get("reglementation", "").strip() if "reglementation" in row else ""

        popup_html = f"""
        <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;">
            <h4 style="margin-top:0;color:#003366;">Zone {code}</h4>
            <p style="font-size:13px;color:#333;">
                {reglement if reglement else '<em>Aucune réglementation associée.</em>'}
            </p>
        </div>
        """

        folium.GeoJson(
            row.geometry,
            style_function=lambda x, color=color: {
                "fillColor": color,
                "color": "#000000",
                "weight": 0.8,
                "fillOpacity": 0.45
            },
            tooltip=f"Zone {code} (avant réglementation)",
            popup=folium.Popup(popup_html, max_width=480)
        ).add_to(group_avant)

    # ------------------------------------------------------------
    # (b) Couche APRES : zones conservées
    # ------------------------------------------------------------
    for _, row in cons_wgs.iterrows():
        code = row["codezone"]
        color = couleur_ppri(code)
        reglement = row.get("reglementation", "").strip() if "reglementation" in row else ""

        popup_html = f"""
        <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;">
            <h4 style="margin-top:0;color:#003366;">Zone {code}</h4>
            <p style="font-size:13px;color:#333;">
                {reglement if reglement else '<em>Aucune réglementation associée.</em>'}
            </p>
        </div>
        """

        folium.GeoJson(
            row.geometry,
            style_function=lambda x, color=color: {
                "fillColor": color,
                "color": "#000000",
                "weight": 1.2,
                "fillOpacity": 0.6
            },
            tooltip=f"Zone {code} (après réglementation)",
            popup=folium.Popup(popup_html, max_width=480)
        ).add_to(group_apres)

    # ------------------------------------------------------------
    # (c) Couche ABSORPTION : zones_remplacement
    #     (les morceaux absorbés/reclassés via buffers ±2.5 m)
    # ------------------------------------------------------------
    for _, row in remp_wgs.iterrows():
        code_abs = row["codezone"]
        code_orig = row["codezone_origine"]
        color = couleur_ppri(code_abs)
        reglement = row.get("reglementation", "").strip() if "reglementation" in row else ""

        popup_html = f"""
        <div style="width:450px;max-height:400px;overflow-y:auto;padding:10px;">
            <h4 style="margin-top:0;color:#003366;">Zone absorbée {code_orig} → {code_abs}</h4>
            <p style="font-size:13px;color:#333;">
                {reglement if reglement else '<em>Aucune réglementation associée.</em>'}
            </p>
        </div>
        """

        folium.GeoJson(
            row.geometry,
            style_function=lambda x, color=color: {
                "fillColor": color,
                "color": "#000000",
                "weight": 1,
                "fillOpacity": 0.4,
                "dashArray": "6,3"
            },
            tooltip=f"Zone absorbée {code_orig} → {code_abs}",
            popup=folium.Popup(popup_html, max_width=480)
        ).add_to(group_absorption)

    # ============================================================
    # 4️⃣ bis : COTES DE SEUIL PPRI
    # ============================================================
    print("   ➕ Intégration des cotes de seuil PPRI...")
    
    # Récupérer la géométrie de la parcelle depuis les résultats
    parcelle_wkt = resultats.get("parcelle", {}).get("wkt", "")
    if not parcelle_wkt:
        # Fallback : reconstruire la géométrie de la parcelle
        from CUA.map_utils import get_parcelle_geometry
        geom_parcelle = get_parcelle_geometry(engine, section, numero)
        parcelle_wkt = geom_parcelle.wkt
    
    sql_cote = f"""
        WITH parcelle AS (SELECT ST_GeomFromText(:wkt, 2154) AS geom)
        SELECT
            c.codezone AS cote_ngf,
            ST_AsGeoJSON(ST_Transform(c.geom_2154, 4326)) AS geom_json
        FROM latresne.cote_de_seuil_ppri c, parcelle p
        WHERE ST_Intersects(c.geom_2154, p.geom);
    """
    
    cote_rows = []
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql_cote), {"wkt": parcelle_wkt})
            for r in res.fetchall():
                cote_rows.append({"cote_ngf": r[0], "geometry": json.loads(r[1])})
    except Exception as e:
        print(f"   ⚠️ Erreur chargement cotes PPRI : {e}")
        cote_rows = []

    group_cotes = folium.FeatureGroup(
        name="Cotes de seuil PPRI",
        show=False  # masquée par défaut
    )

    if cote_rows:
        for feat in cote_rows:
            geom = feat["geometry"]
            cote = feat["cote_ngf"]
            folium.GeoJson(
                geom,
                style_function=lambda x: {
                    "color": "#2c7bb6",
                    "weight": 2,
                    "fillOpacity": 0.5
                },
                tooltip=f"Cote de seuil : {cote} m NGF"
            ).add_to(group_cotes)
        print(f"   ✅ {len(cote_rows)} cotes ajoutées à la couche 'Cotes de seuil PPRI'")
    else:
        print("   ⚠️ Aucune cote de seuil trouvée pour cette parcelle")

    # ============================================================
    # 5️⃣ Ajouter les 4 groupes à la carte Folium
    # ============================================================
    group_avant.add_to(map_folium)
    group_apres.add_to(map_folium)
    group_absorption.add_to(map_folium)
    group_cotes.add_to(map_folium)

    # Récupérer les noms JS (variables Leaflet créées par Folium)
    avant_var = group_avant.get_name()
    apres_var = group_apres.get_name()
    absorp_var = group_absorption.get_name()
    cotes_var = group_cotes.get_name()

    # ============================================================
    # 6️⃣ Intégrer ces 3 sous-couches PPRI dans le registry
    #     → pour la légende custom que tu génères après
    # ============================================================
    # nom du bloc parent dans la légende
    if nom_groupe is None:
        nom_groupe = "🌊 PPRI"

    if registry is not None:
        registry["layers"].append({
            "name": nom_groupe,
            "color": "#1e90ff",
            "mode": "grouped",  # très important : on veut un groupe dépliant
            "attribut_map": "type",
            "nom_attribut_map": "Avant / Après / Absorption / Cotes de seuil",
            "entities": [
                {
                    "name": "Avant réglementation",
                    "vars": [avant_var],
                    "count": len(init_wgs)
                },
                {
                    "name": "Après réglementation",
                    "vars": [apres_var],
                    "count": len(cons_wgs)
                },
                {
                    "name": "Zones d'absorption réglementaires",
                    "vars": [absorp_var],
                    "count": len(remp_wgs)
                },
                {
                    "name": "Cotes de seuil PPRI",
                    "vars": [cotes_var],
                    "count": len(cote_rows)
                }
            ]
        })

    # ============================================================
    # 7️⃣ Préparer un résumé métadonnées cohérent avec avant
    # ============================================================
    # stats de base
    nb_init = len(ppri_initial)
    nb_final = len(cons)
    nb_absorbees = len(abs_data) if not abs_data.empty else 0
    nb_rempl = len(zones_remplacement)
    nb_cotes = len(cote_rows)

    # calcul du taux d'absorption si possible
    if nb_init > 0:
        taux_absorption_pct = (nb_absorbees / nb_init) * 100.0
    else:
        taux_absorption_pct = 0.0

    meta = {
        "success": True,
        "zones_initiales": nb_init,
        "zones_conservees": nb_final,
        "zones_absorbees": nb_absorbees,
        "zones_remplacement": nb_rempl,
        "cotes_seuil": nb_cotes,
        "taux_absorption": taux_absorption_pct,
        # on conserve ces clés pour compatibilité avec ton logging
        "zones_conservees_log": nb_final,
        "zones_absorbees_log": nb_absorbees,
        "zones_remplacement_log": nb_rempl,
        # bonus : noms JS des layers, utile pour debug
        "layers_js": {
            "avant": avant_var,
            "apres": apres_var,
            "absorption": absorp_var,
            "cotes": cotes_var
        }
    }

    print(f"   ✅ PPRI: {nb_init} zones initiales")
    print(f"   ✅ {nb_final} zones conservées (après réglementation)")
    print(f"   ✅ {nb_absorbees} zones absorbées")
    print(f"   ✅ {nb_rempl} zones d'absorption générées (buffers 2.5m)")
    print(f"   ✅ {nb_cotes} cotes de seuil PPRI")
    print("   ✅ PPRI ajouté à la légende globale (4 sous-couches)")

    return meta
