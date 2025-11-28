# -*- coding: utf-8 -*-
"""
verif_unite_fonciere.py — Validation géospatiale des unités foncières issues d’un CERFA
Vérifie que les parcelles extraites forment une seule unité foncière contiguë (max 5 parcelles).
Renvoie un rapport JSON clair utilisable par l'orchestrator avant le CUA builder.
"""

import io, json, requests
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.prepared import prep
from pathlib import Path

# ============================================================
# CONFIGURATION WFS
# ============================================================
ENDPOINT = "https://data.geopf.fr/wfs/ows"
LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
SRS = "EPSG:2154"

# ============================================================
# FONCTION PRINCIPALE
# ============================================================
def verifier_unite_fonciere(cerfa_json_path: str, code_insee: str, out_dir: str = ".") -> dict:
    """Analyse les parcelles d'un CERFA et détermine si elles forment une unité foncière valide."""
    cerfa_data = json.load(open(cerfa_json_path, encoding="utf-8"))["data"]
    parcelles = cerfa_data.get("references_cadastrales", [])
    commune = cerfa_data.get("commune_nom", "")
    nb_parcelles = len(parcelles)

    if nb_parcelles == 0:
        return {"success": False, "message": "Aucune parcelle détectée dans le CERFA.", "groupes": []}
    
    if nb_parcelles > 20:
        return {"success": False, "message": f"Trop de parcelles ({nb_parcelles}). Veuillez limiter à 5 par unité foncière.", "groupes": []}

    # Construction du filtre CQL
    parcelle_conditions = [
        f"(section='{p['section']}' AND numero='{p['numero']}')" for p in parcelles
    ]
    cql_filter = f"code_insee='{code_insee}' AND ({' OR '.join(parcelle_conditions)})"

    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": LAYER, "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": cql_filter
    }

    try:
        r = requests.get(ENDPOINT, params=params, timeout=30)
        r.raise_for_status()
        gdf = gpd.read_file(io.BytesIO(r.content))
    except Exception as e:
        return {"success": False, "message": f"Erreur WFS : {e}", "groupes": []}

    if gdf.empty:
        return {"success": False, "message": "Aucune géométrie de parcelle trouvée.", "groupes": []}

    if gdf.crs is None or gdf.crs.to_string() != SRS:
        gdf = gdf.to_crs(SRS)

    gdf = gdf.rename(columns={"geometry": "geom_2154"}).set_geometry("geom_2154")
    
    # === Récupération superficie indicative (contenance) ===
    superficie_indicative = None
    try:
        contenance_col = None
        for col in gdf.columns:
            if 'contenance' in col.lower() or 'contain' in col.lower():
                contenance_col = col
                break
        
        if contenance_col:
            superficie_indicative = 0.0
            for _, row in gdf.iterrows():
                val = row.get(contenance_col)
                if val:
                    try:
                        if isinstance(val, str):
                            val = float(val.replace(',', '.').replace(' ', ''))
                        superficie_indicative += float(val)
                    except (ValueError, TypeError):
                        pass
            superficie_indicative = round(superficie_indicative, 2)
            print(f"✅ Superficie indicative (contenance IGN) : {superficie_indicative} m²")
    except Exception as e:
        print(f"⚠️ Erreur récupération contenance : {e}")

    # Fusion des géométries
    union_geom = gdf["geom_2154"].unary_union

    # ============================================================
    # CAS 1 : Une seule unité foncière
    # ============================================================
    if isinstance(union_geom, Polygon):
        # Sauvegarde du WKT de l'unité foncière pour usage ultérieur
        wkt_path = Path(out_dir) / "geom_unite_fonciere.wkt"
        wkt_path.write_text(union_geom.wkt, encoding="utf-8")

        if nb_parcelles <= 5:
            msg = f"✅ Une seule unité foncière détectée ({nb_parcelles} parcelle(s) contiguë(s))."
            return {
                "success": True,
                "message": msg,
                "groupes": [[f"{p['section']} {p['numero']}" for p in parcelles]],
                "geom_wkt_path": str(wkt_path),
                "superficie_indicative": superficie_indicative
            }
        else:
            # Trop de parcelles contiguës → suggérer des regroupements par 5 max
            groupes = [
                [f"{p['section']} {p['numero']}" for p in parcelles[i:i+5]]
                for i in range(0, nb_parcelles, 5)
            ]
            msg = f"⚠️ {nb_parcelles} parcelles contiguës détectées, mais limité à 5 par analyse. " \
                  f"Séparez-les en {len(groupes)} unités foncières."
            return {
                "success": False,
                "message": msg,
                "groupes": groupes,
                "geom_wkt_path": str(wkt_path),
                "superficie_indicative": superficie_indicative
            }

    # ============================================================
    # CAS 2 : Plusieurs unités foncières distinctes
    # ============================================================
    elif isinstance(union_geom, MultiPolygon):
        groupes_uf = []
        for uf_geom in union_geom.geoms:
            uf_prepared = prep(uf_geom)
            parcelles_du_groupe = []
            for _, row in gdf.iterrows():
                if uf_prepared.contains(row["geom_2154"].representative_point()):
                    parcelles_du_groupe.append(f"{row['section']} {row['numero']}")
            if parcelles_du_groupe:
                groupes_uf.append(parcelles_du_groupe)

        msg = f"❌ {len(groupes_uf)} unités foncières distinctes détectées pour la commune {commune}. " \
              "Veuillez déposer une demande de CERFA par unité foncière."
        return {"success": False, "message": msg, "groupes": groupes_uf}

    # ============================================================
    # CAS 3 : Erreur géométrique
    # ============================================================
    else:
        msg = f"Erreur d’analyse géométrique : type inattendu ({union_geom.geom_type})."
        return {"success": False, "message": msg, "groupes": []}


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Vérifie la cohérence foncière des parcelles d'un CERFA.")
    ap.add_argument("--cerfa-json", required=True, help="Chemin vers le JSON d'analyse CERFA")
    ap.add_argument("--code-insee", required=True, help="Code INSEE de la commune")
    ap.add_argument("--out", default="rapport_unite_fonciere.json", help="Fichier JSON de sortie")
    ap.add_argument("--out-dir", default=".", help="Dossier de sortie")
    args = ap.parse_args()

    result = verifier_unite_fonciere(args.cerfa_json, args.code_insee, args.out_dir)
    Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("\n📊 Résultat de la vérification :")
    print(json.dumps(result, indent=2, ensure_ascii=False))
