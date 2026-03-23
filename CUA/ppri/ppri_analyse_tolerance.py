# -*- coding: utf-8 -*-
"""
ppri_analyse_tolerance.py
Analyse détaillée des zones PPRI avec rapport complet
SANS génération de carte (pour performances optimales)
"""

import io
import os
import geopandas as gpd
import requests
from shapely import wkb
from shapely.ops import unary_union
from shapely.validation import explain_validity
from sqlalchemy import text, create_engine
from dotenv import load_dotenv
from datetime import datetime

# =========================================================
# 🔧 Connexion DB
# =========================================================
load_dotenv()
HOST = os.getenv("SUPABASE_HOST")
DB = os.getenv("SUPABASE_DB")
USER = os.getenv("SUPABASE_USER")
PWD = os.getenv("SUPABASE_PASSWORD")
PORT = str(os.getenv("SUPABASE_PORT", "5432")).strip().strip('"').strip("'")
if HOST and "pooler.supabase.com" in HOST and PORT == "5432":
    print("⚠️ SUPABASE_PORT=5432 détecté sur pooler; bascule auto vers 6543 (transaction mode).")
    PORT = "6543"
engine = create_engine(
    f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}",
    pool_size=1,
    max_overflow=0,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# =========================================================
# 🎨 Hiérarchie de contrainte PPRI
# =========================================================
HIERARCHIE_CONTRAINTE = {
    "Grenat": 1,
    "Rouge foncé": 2,
    "Rouge non urbanisé": 3,
    "Rouge": 4,
    "Rouge centre urbain": 5,
    "Rouge industrialo-portuaire": 6,
    "Rouge urbanisé": 7,
    "Rouge clair": 8,
    "Bleu": 9,
    "Bleu clair": 10,
    "Byzantin": 10,
    "Violette": 10,
    "Jaune": 10,
    "Orange": 10,
    "Marron": 10,
}


def get_niveau_contrainte(codezone: str) -> int:
    """Retourne le niveau de contrainte d'une zone"""
    if codezone in HIERARCHIE_CONTRAINTE:
        return HIERARCHIE_CONTRAINTE[codezone]
    
    codezone_lower = codezone.lower()
    for k, v in HIERARCHIE_CONTRAINTE.items():
        if k.lower() in codezone_lower:
            return v
    return 1


def peut_absorber(zone_absorbante: str, zone_absorbee: str) -> bool:
    """Vérifie si une zone peut en absorber une autre selon la hiérarchie"""
    niveau_absorbante = get_niveau_contrainte(zone_absorbante)
    niveau_absorbee = get_niveau_contrainte(zone_absorbee)
    return niveau_absorbante >= niveau_absorbee


# =========================================================
# 🧮 Fonction d'analyse principale
# =========================================================
def analyser_ppri_tolerance(section=None, numero=None, code_insee=None, ppri_table=None,
                            engine=None, seuil_residuel_m2=0.01, geom_wkt=None):
    """
    Analyse complète des zones PPRI avec rapport détaillé.
    Peut être appelée soit :
      - via section/numero/code_insee (mode cadastral)
      - soit directement avec une géométrie WKT (mode unité foncière)
    """
    
    ENDPOINT = "https://data.geopf.fr/wfs/ows"
    SRS = "EPSG:2154"
    LAYER_PARCELLE = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
    TOLERANCE_M = 2.5
    BUFFER_M = 50
    
    debut_analyse = datetime.now()
    
    print("\n" + "=" * 80)
    print(f"🔍 ANALYSE PPRI - TOLÉRANCE ±{TOLERANCE_M} m")
    print("=" * 80)
    print(f"📅 Date: {debut_analyse.strftime('%Y-%m-%d %H:%M:%S')}")
    if geom_wkt:
        print("📍 Mode WKT direct activé")
    else:
        print(f"📍 Parcelle: {section} {numero} (INSEE: {code_insee})")
    print(f"🎯 Seuil résiduel: {seuil_residuel_m2} m² ({seuil_residuel_m2 * 10000:.0f} cm²)")
    print("=" * 80)

    # =========================================================
    # 1️⃣ Charger la géométrie
    # =========================================================
    print("\n📦 ÉTAPE 1/6 : Chargement de la géométrie")
    print("-" * 80)

    if geom_wkt:
        # Utiliser directement la géométrie fournie
        print("✅ Géométrie WKT reçue, lecture directe")
        geom_parcelle = gpd.GeoSeries.from_wkt([geom_wkt], crs=2154).iloc[0]
        surface_parcelle = geom_parcelle.area
        perimetre_parcelle = geom_parcelle.length
        print(f"   • Surface: {surface_parcelle:.2f} m²")
        print(f"   • Périmètre: {perimetre_parcelle:.2f} m")
        print(f"   • Type: {geom_parcelle.geom_type}")
    else:
        # Mode ancien via WFS cadastre
        params_parcelle = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typename": LAYER_PARCELLE,
            "srsName": SRS,
            "outputFormat": "application/json",
            "CQL_FILTER": f"code_insee='{code_insee}' AND section='{section}' AND numero='{numero}'"
        }
        parcelle = gpd.read_file(io.BytesIO(requests.get(ENDPOINT, params=params_parcelle).content)).to_crs(SRS)

        if parcelle.empty:
            raise ValueError("❌ Parcelle introuvable.")

        geom_parcelle = parcelle.geometry.union_all()
        surface_parcelle = geom_parcelle.area
        perimetre_parcelle = geom_parcelle.length
        print(f"✅ Parcelle chargée")
        print(f"   • Surface: {surface_parcelle:.2f} m² ({surface_parcelle/10000:.4f} ha)")
        print(f"   • Périmètre: {perimetre_parcelle:.2f} m")
        print(f"   • Type géométrie: {geom_parcelle.geom_type}")
    
    buffer_geom = geom_parcelle.buffer(BUFFER_M)

    # =========================================================
    # 2️⃣ Charger les zones PPRI
    # =========================================================
    print("\n📦 ÉTAPE 2/6 : Chargement des zones PPRI")
    print("-" * 80)

    # 🔐 Forcer le schéma si absent
    if ppri_table and "." not in ppri_table:
        ppri_table = f"latresne.{ppri_table}"
    
    sql = f"""
    WITH p AS (SELECT ST_GeomFromText(:wkt, 2154) AS geom)
    SELECT 
        z.codezone,
        z.reglementation,
        ST_AsEWKB(z.geom_2154) AS ewkb
    FROM {ppri_table} z, p
    WHERE ST_Intersects(z.geom_2154, ST_Buffer(p.geom, {BUFFER_M}));
    """
    rows = []
    with engine.connect() as conn:
        for codezone, reglementation, ewkb in conn.execute(text(sql), {"wkt": geom_parcelle.wkt}).fetchall():
            rows.append({
                "codezone": codezone,
                "reglementation": reglementation,
                "geometry": wkb.loads(bytes(ewkb))
            })
    
    ppri_initial = gpd.GeoDataFrame(rows, geometry="geometry", crs=2154)
    
    if ppri_initial.empty:
        raise ValueError("❌ Aucune zone PPRI trouvée autour de la parcelle.")
    
    # ✅ Vérification de la présence de la colonne codezone
    if 'codezone' not in ppri_initial.columns:
        raise ValueError("⚠️ Colonne codezone manquante dans ppri_initial")
    
    print(f"✅ {len(ppri_initial)} zones PPRI chargées depuis la base")
    
    # Statistiques par type de zone
    zones_types = ppri_initial.groupby("codezone").size()
    print(f"   📊 {len(zones_types)} type(s) de zone(s) différentes")

    # =========================================================
    # 3️⃣ Nettoyage et filtrage
    # =========================================================
    print("\n🧹 ÉTAPE 3/6 : Nettoyage et filtrage")
    print("-" * 80)
    
    ppri = ppri_initial.copy()
    ppri["geometry"] = ppri.geometry.intersection(geom_parcelle)
    
    nb_zones_initiales = len(ppri)
    
    # Filtrage surface nulle
    ppri = ppri[ppri.geometry.area > 0.01].copy()
    nb_filtrees_surface = nb_zones_initiales - len(ppri)
    print(f"   🗑️ {nb_filtrees_surface} zones de surface < 100 cm² supprimées")
    
    # Filtrage zones marginales
    centroid_parcelle = geom_parcelle.centroid
    zones_valides = []
    zones_exclues = []
    
    for idx, row in ppri.iterrows():
        zone_geom = row.geometry
        zone_centroid = zone_geom.centroid
        is_centroid_inside = zone_centroid.within(geom_parcelle)
        is_zone_significant = zone_geom.area > 0.5
        
        if is_centroid_inside or is_zone_significant:
            zones_valides.append(row)
        else:
            zones_exclues.append({
                "codezone": row.codezone,
                "surface": zone_geom.area,
                "raison": "Centroïde hors parcelle"
            })
    
    ppri = gpd.GeoDataFrame(zones_valides, crs=2154)
    
    if zones_exclues:
        print(f"   🗑️ {len(zones_exclues)} zones marginales exclues")

    # =========================================================
    # 4️⃣ Éclatement des MultiPolygons
    # =========================================================
    print("\n✂️ ÉTAPE 4/6 : Éclatement des géométries multi-parties")
    print("-" * 80)
    
    nb_avant_eclatement = len(ppri)
    nb_multipolygons = len(ppri[ppri.geometry.geom_type == 'MultiPolygon'])
    
    ppri_exploded = []
    for idx, row in ppri.iterrows():
        geom = row.geometry
        
        if geom.geom_type == 'MultiPolygon':
            nb_parties = len(geom.geoms)
            for i, poly in enumerate(geom.geoms, 1):
                if poly.area > 0.001:
                    ppri_exploded.append({
                        "codezone": row.codezone,
                        "reglementation": row.get("reglementation", ""),
                        "geometry": poly,
                        "zone_originale_idx": idx,
                        "partie_numero": i
                    })
        elif geom.geom_type == 'Polygon':
            ppri_exploded.append({
                "codezone": row.codezone,
                "reglementation": row.get("reglementation", ""),
                "geometry": geom,
                "zone_originale_idx": idx,
                "partie_numero": 1
            })
    
    ppri = gpd.GeoDataFrame(ppri_exploded, geometry="geometry", crs=2154).reset_index(drop=True)
    
    # ⭐ NOUVEAU FILTRE : Suppression des fragments < 1m²
    nb_avant_filtre = len(ppri)
    ppri = ppri[ppri.geometry.area >= 1.0].copy().reset_index(drop=True)
    nb_filtrees_petites = nb_avant_filtre - len(ppri)
    
    surface_totale_zones = ppri.geometry.area.sum()
    print(f"   ✅ Éclatement terminé: {len(ppri)} fragment(s) conservé(s) ({surface_totale_zones:.2f} m², {surface_totale_zones/surface_parcelle*100:.1f}% de la parcelle)")
    
    # Créer les buffers
    ppri_buffer = ppri.copy()
    ppri_buffer["geometry"] = ppri_buffer.geometry.buffer(TOLERANCE_M).intersection(geom_parcelle)

    # =========================================================
    # 5️⃣ Analyse d'absorption
    # =========================================================
    print("\n🔍 ÉTAPE 5/6 : Analyse d'absorption avec hiérarchie de contrainte")
    print("-" * 80)
    
    # Afficher la hiérarchie (version simplifiée)
    print("📏 Hiérarchie de contrainte appliquée")
    
    print("\n" + "-" * 80)
    
    absorbées, conservées, relations = [], [], []
    zones_conservees_force = []  # Zones conservées malgré couverture complète
    
    for i, z in ppri.iterrows():
        other_buffers = ppri_buffer[ppri_buffer.index != i]
        
        if other_buffers.empty:
            conservées.append({
                "codezone": z.codezone,
                "reglementation": z.get("reglementation", ""),
                "geometry": z.geometry
            })
            continue
        
        union_others = unary_union(other_buffers.geometry)
        
        # Tests géométriques
        is_within = z.geometry.within(union_others)
        is_covered = z.geometry.covered_by(union_others)
        difference = z.geometry.difference(union_others)
        surface_residuelle = difference.area
        surface_totale = z.geometry.area
        pct_couverture = ((surface_totale - surface_residuelle) / surface_totale * 100) if surface_totale > 0 else 0
        
        # Décision géométrique
        est_absorbee_geometriquement = (
            is_within or is_covered or
            surface_residuelle <= seuil_residuel_m2 or
            (pct_couverture >= 99.99 and surface_residuelle <= 0.1)
        )
        
        if not est_absorbee_geometriquement:
            conservées.append({
                "codezone": z.codezone,
                "reglementation": z.get("reglementation", ""),
                "geometry": z.geometry
            })
            continue
        
        # Identifier les absorbeurs potentiels
        absorbeurs_autorises = []
        absorbeurs_interdits = []
        contributions = {}
        
        for j, z_other in ppri.iterrows():
            if i != j:
                buffer_j = ppri_buffer.loc[j, "geometry"]
                overlap = z.geometry.intersection(buffer_j)
                
                if not overlap.is_empty:
                    overlap_pct = (overlap.area / z.geometry.area * 100) if z.geometry.area > 0 else 0
                    if overlap_pct > 1:
                        if peut_absorber(z_other.codezone, z.codezone):
                            absorbeurs_autorises.append(z_other.codezone)
                            contributions[z_other.codezone] = overlap_pct
                        else:
                            absorbeurs_interdits.append(z_other.codezone)
        
        # Décision finale
        if not absorbeurs_autorises:
            conservées.append({
                "codezone": z.codezone,
                "reglementation": z.get("reglementation", ""),
                "geometry": z.geometry
            })
            zones_conservees_force.append({
                "codezone": z.codezone,
                "surface": surface_totale,
                "pct_couverture": pct_couverture,
                "zones_interdites": absorbeurs_interdits
            })
        else:
            absorbées.append({
                "codezone": z.codezone,
                "reglementation": z.get("reglementation", ""),
                "geometry": z.geometry,
                "absorbeurs": list(set(absorbeurs_autorises)),
                "contributions": contributions
            })
            
            relations.append({
                "absorbée": z.codezone,
                "absorbeurs": list(set(absorbeurs_autorises)),
                "surface_totale": surface_totale,
                "pct_couverture": pct_couverture,
                "surface_residuelle": surface_residuelle
            })

    ppri_abs = gpd.GeoDataFrame(absorbées, geometry="geometry", crs=ppri.crs) if absorbées else gpd.GeoDataFrame()
    ppri_cons = gpd.GeoDataFrame(conservées, geometry="geometry", crs=ppri.crs)

    # =========================================================
    # 6️⃣ Rapport final
    # =========================================================
    print("\n📊 ÉTAPE 6/6 : Rapport final")
    print("=" * 80)
    
    duree_analyse = (datetime.now() - debut_analyse).total_seconds()
    
    print(f"\n⏱️ Durée de l'analyse: {duree_analyse:.2f} secondes")
    print("\n" + "=" * 80)
    print("📈 STATISTIQUES FINALES")
    print("=" * 80)
    
    print(f"\n🔢 Comptages:")
    print(f"   • Zones initiales (DB): {len(ppri_initial)}")
    print(f"   • Zones après filtrage: {len(ppri)}")
    print(f"   • Zones conservées: {len(ppri_cons)}")
    print(f"   • Zones absorbées: {len(ppri_abs)}")
    print(f"   • Taux d'absorption: {len(ppri_abs)/len(ppri)*100:.1f}%")
    
    if zones_conservees_force:
        print(f"\n⚠️ Zones conservées malgré couverture complète (hiérarchie): {len(zones_conservees_force)}")
        for zcf in zones_conservees_force:
            print(f"   • {zcf['codezone']:<30} ({zcf['surface']:.2f} m², couv. {zcf['pct_couverture']:.1f}%)")
            print(f"     Rejetées: {', '.join(zcf['zones_interdites'])}")
    
    print(f"\n📏 Surfaces:")
    surface_conservees = ppri_cons.geometry.area.sum()
    surface_absorbees = ppri_abs.geometry.area.sum() if not ppri_abs.empty else 0
    
    print(f"   • Surface zones conservées: {surface_conservees:.2f} m²")
    print(f"   • Surface zones absorbées: {surface_absorbees:.2f} m²")
    print(f"   • Couverture parcelle: {(surface_conservees + surface_absorbees)/surface_parcelle*100:.1f}%")
    
    if relations:
        print(f"\n🔄 Détail des absorptions ({len(relations)}):")
        print("=" * 80)
        for r in relations:
            absorbeurs_list = ", ".join(r["absorbeurs"])
            print(f"\n   ➡️ {r['absorbée']}")
            print(f"      • Surface: {r['surface_totale']:.4f} m²")
            print(f"      • Couverture: {r['pct_couverture']:.2f}%")
            print(f"      • Résidu: {r['surface_residuelle']:.6f} m²")
            print(f"      • Absorbée par: {absorbeurs_list}")
    
    print("\n" + "=" * 80)
    print("✅ ANALYSE TERMINÉE")
    print("=" * 80)
    
    # Retourner les résultats pour usage ultérieur
    return {
        "parcelle": {
            "section": section,
            "numero": numero,
            "code_insee": code_insee,
            "surface": surface_parcelle,
            "perimetre": perimetre_parcelle,
            "wkt": geom_parcelle.wkt  # ✅ ajouté pour réutilisation
        },
        "zones": {
            "ppri_initial": ppri_initial,
            "ppri_filtre": ppri,
            "ppri_conservees": ppri_cons,
            "ppri_absorbees": ppri_abs,
            "ppri_buffer": ppri_buffer
        },
        "statistiques": {
            "nb_initial": len(ppri_initial),
            "nb_final": len(ppri),
            "nb_conservees": len(ppri_cons),
            "nb_absorbees": len(ppri_abs),
            "surface_conservees": surface_conservees,
            "surface_absorbees": surface_absorbees
        },
        "relations": relations,
        "zones_conservees_force": zones_conservees_force,
        "parametres": {
            "tolerance_m": TOLERANCE_M,
            "seuil_residuel_m2": seuil_residuel_m2,
            "buffer_m": BUFFER_M
        }
    }


if __name__ == "__main__":
    resultats = analyser_ppri_tolerance(
        section="AC",
        numero="0242",
        code_insee="33234",
        ppri_table="latresne.pm1_detaillee_gironde",
        engine=engine,
        seuil_residuel_m2=0.01
    )