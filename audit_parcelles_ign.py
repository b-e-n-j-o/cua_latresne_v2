#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
audit_parcelles_ign.py
----------------------
Calcule la superficie indicative de l'unité foncière unifiée à partir des parcelles :
1) Récupère les parcelles via WFS (Parcellaire Express)
2) Extrait la surface indicative (contenance) de chaque parcelle
3) Somme les contenances pour obtenir la superficie indicative totale
"""

import io
import requests
import geopandas as gpd

ENDPOINT = "https://data.geopf.fr/wfs/ows"
LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS = "EPSG:2154"

CODE_INSEE = "33234"
COMMUNE_NOM = "Latresne"

# Parcelles de test (à adapter si besoin)
PARCELLES_TEST = [
    {"section": "AL", "numero": "0417"},
    {"section": "AL", "numero": "0418"},
    {"section": "AL", "numero": "0074"},
]


def fetch_parcelles_wfs(parcelles, code_insee):
    """Récupère les géométries des parcelles via WFS et renvoie un GeoDataFrame."""
    parcelle_conditions = [
        f"(section='{p['section']}' AND numero='{p['numero']}')" for p in parcelles
    ]
    cql_filter = f"code_insee='{code_insee}' AND ({' OR '.join(parcelle_conditions)})"

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": cql_filter,
    }

    print("🌐 Requête WFS…")
    print(f"   Endpoint: {ENDPOINT}")
    print(f"   Layer: {LAYER}")
    print(f"   SRS demandé: {SRS}")
    r = requests.get(ENDPOINT, params=params, timeout=30)
    r.raise_for_status()

    gdf = gpd.read_file(io.BytesIO(r.content))
    if gdf.empty:
        raise RuntimeError("Aucune géométrie de parcelle trouvée dans le WFS.")

    crs_received = gdf.crs.to_string() if gdf.crs else "None"
    print(f"   CRS reçu du serveur: {crs_received}")
    
    if gdf.crs is None or gdf.crs.to_string() != SRS:
        print(f"   → Conversion vers {SRS}...")
        gdf = gdf.to_crs(SRS)
    else:
        print(f"   ✓ CRS déjà en {SRS}")

    return gdf


def main():
    print("=== CALCUL SUPERFICIE INDICATIVE UNITÉ FONCIÈRE ===\n")
    print(f"Commune : {COMMUNE_NOM} ({CODE_INSEE})")
    print("Parcelles :", ", ".join(f"{p['section']} {p['numero']}" for p in PARCELLES_TEST))
    print()

    # Récupération des parcelles via WFS
    gdf = fetch_parcelles_wfs(PARCELLES_TEST, CODE_INSEE)
    print(f"✅ {len(gdf)} parcelle(s) récupérée(s)")
    print()
    
    # Affichage des colonnes disponibles pour debug
    print("→ Colonnes disponibles :", list(gdf.columns))
    print()

    # Recherche de la colonne contenance (peut avoir différents noms)
    contenance_col = None
    possible_names = ['contenance', 'contenance_m2', 'contenance_m²', 'CONTAIN', 'contain']
    
    for col in gdf.columns:
        if col.lower() in [name.lower() for name in possible_names]:
            contenance_col = col
            break
    
    if contenance_col is None:
        # Si pas trouvé, chercher une colonne contenant "contenance" ou "contain"
        for col in gdf.columns:
            if 'contenance' in col.lower() or 'contain' in col.lower():
                contenance_col = col
                break
    
    if contenance_col is None:
        raise RuntimeError(
            f"❌ Colonne 'contenance' introuvable dans le GeoDataFrame.\n"
            f"   Colonnes disponibles : {list(gdf.columns)}\n"
            f"   Veuillez vérifier le nom exact de la colonne dans les données WFS."
        )
    
    print(f"✅ Colonne contenance trouvée : '{contenance_col}'")
    print()

    # Extraction et somme des contenances
    print("=== DÉTAIL PAR PARCELLE ===")
    superficie_totale_m2 = 0.0
    
    for idx, row in gdf.iterrows():
        section = row.get('section', '?')
        numero = row.get('numero', '?')
        contenance = row.get(contenance_col)
        
        # Conversion en float si nécessaire
        if contenance is None:
            print(f"⚠️  Parcelle {section} {numero} : contenance manquante")
            continue
        
        try:
            # Gérer différents formats (string avec virgule, float, etc.)
            if isinstance(contenance, str):
                contenance_val = float(contenance.replace(',', '.').replace(' ', ''))
            else:
                contenance_val = float(contenance)
            
            superficie_totale_m2 += contenance_val
            print(f"   {section} {numero} : {contenance_val:.2f} m²")
        except (ValueError, TypeError) as e:
            print(f"⚠️  Parcelle {section} {numero} : erreur conversion contenance ({contenance}): {e}")
    
    print()
    
    print("=== RÉSULTAT ===")
    print(f"Superficie indicative unité foncière (somme contenances) : {superficie_totale_m2:.2f} m²")
    print(f"Superficie indicative unité foncière : {superficie_totale_m2 / 10000:.4f} ha")
    print()


if __name__ == "__main__":
    main()
