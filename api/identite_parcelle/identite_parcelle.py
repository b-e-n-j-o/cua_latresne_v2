"""
identite_parcelle.py
Service métier pour l'analyse d'identité parcellaire
"""
import os
import json
import requests
import io
import logging
from pathlib import Path
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

try:
    import geopandas as gpd
except ImportError:
    gpd = None

load_dotenv()

logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DB = os.getenv('SUPABASE_DB')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = os.getenv('SUPABASE_PORT')

DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

IGN_WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
IGN_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

# Chargement catalogue
CATALOGUE_PATH = Path(__file__).parent / "catalogue_identite.json"
with open(CATALOGUE_PATH, 'r', encoding='utf-8') as f:
    CATALOGUE = json.load(f)

# ------------------------------------------------------------
# Fonctions métier
# ------------------------------------------------------------

def fetch_parcelle_geometry_ign(section: str, numero: str, insee: str) -> str:
    """
    Récupère la géométrie WKT en EPSG:2154 depuis l'IGN WFS
    
    Args:
        section: Section cadastrale (ex: "AC")
        numero: Numéro de parcelle (ex: "0042")
        insee: Code INSEE commune (ex: "33522")
    
    Returns:
        str: Géométrie au format WKT en EPSG:2154
    
    Raises:
        ValueError: Si geopandas n'est pas disponible
        requests.RequestException: Si erreur réseau
        ValueError: Si parcelle non trouvée
    """
    logger.info(f"🔍 Récupération géométrie IGN pour {section} {numero} (INSEE: {insee})")
    
    if gpd is None:
        raise ValueError("geopandas non disponible")
    
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LAYER,
        "srsName": "EPSG:2154",
        "outputFormat": "application/json",
        "CQL_FILTER": f"code_insee='{insee}' AND section='{section}' AND numero='{numero}'"
    }
    
    logger.info(f"📡 Appel IGN WFS...")
    r = requests.get(IGN_WFS_ENDPOINT, params=params, timeout=30)
    r.raise_for_status()
    logger.info(f"✅ Réponse IGN reçue ({len(r.content)} bytes)")
    
    gdf = gpd.read_file(io.BytesIO(r.content))
    
    if gdf.empty:
        raise ValueError(f"Parcelle {section} {numero} non trouvée (INSEE: {insee})")
    
    logger.info(f"✅ Géométrie extraite : {len(gdf.iloc[0].geometry.wkt)} caractères")
    return gdf.iloc[0].geometry.wkt

def get_carto_tables() -> List[str]:
    """
    Récupère les tables actives depuis layer_registry
    
    Returns:
        List[str]: Liste des noms de tables actives
    """
    logger.info("📊 Récupération des tables depuis layer_registry...")
    
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT table_name 
                FROM carto.layer_registry 
                WHERE is_active = true
                AND table_schema = 'carto'
            """)
            
            result = conn.execute(query)
            tables = [row[0] for row in result]
        
        logger.info(f"✅ {len(tables)} tables actives trouvées")
        return tables
    except Exception as e:
        logger.error(f"   ❌ Erreur lors de la récupération depuis layer_registry : {e}")
        return []

def calculate_intersections_detailed(parcelle_wkt: str, tables: List[str] = None):
    if tables is None:
        tables = get_carto_tables()
    
    if not tables:
        return []
    
    logger.info(f"🧩 Test avec attributs sur {len(tables)} tables...")
    
    def test_table(table_name):
        try:
            config = CATALOGUE.get(table_name)
            if not config:
                return None
            
            attr_disc = config["attribut_disc"]
            
            with engine.connect() as conn:
                # Détecter si c'est un array
                type_check = text("""
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'carto' 
                    AND table_name = :tbl 
                    AND column_name = :col
                """)
                col_type = conn.execute(type_check, {"tbl": table_name, "col": attr_disc}).scalar()
                
                # Construire requête selon type
                if col_type == 'ARRAY':
                    query = text(f"""
                        SELECT DISTINCT unnest({attr_disc}) as valeur
                        FROM carto.{table_name}
                        WHERE geom_2154 && ST_Expand(ST_GeomFromText(:wkt, 2154), 1000)
                        AND ST_Intersects(geom_2154, ST_GeomFromText(:wkt, 2154))
                    """)
                else:
                    query = text(f"""
                        SELECT DISTINCT {attr_disc} as valeur
                        FROM carto.{table_name}
                        WHERE geom_2154 && ST_Expand(ST_GeomFromText(:wkt, 2154), 1000)
                        AND ST_Intersects(geom_2154, ST_GeomFromText(:wkt, 2154))
                    """)
                
                result = conn.execute(query, {"wkt": parcelle_wkt})
                valeurs = [str(row[0]).lower() for row in result if row[0]]
                
                if valeurs:
                    logger.info(f"   ✅ {table_name}: {len(valeurs)} élément(s)")
                    return {
                        "table": table_name,
                        "display_name": config["nom_affiche"],
                        "elements": valeurs
                    }
        except Exception as e:
            logger.error(f"   ❌ {table_name}: {e}")
        return None
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = [r for r in executor.map(test_table, tables) if r]
    
    logger.info(f"🎯 {len(results)} couches intersectées")
    return results

def format_table_name(table_name: str) -> str:
    """
    Formate le nom de table pour affichage UI
    À terme : sera remplacé par un catalogue JSON
    
    Args:
        table_name: Nom technique de la table
    
    Returns:
        str: Nom formaté pour affichage
    """
    name = table_name
    
    # Retirer préfixes de ville
    for prefix in ["plui_", "plu_", "bordeaux_", "talence_", "latresne_", "pessac_"]:
        if name.startswith(prefix):
            name = name[len(prefix):]
    
    # Remplacer underscores et capitaliser
    name = name.replace("_", " ").title()
    
    return name

# ------------------------------------------------------------
# Fonction principale (orchestration)
# ------------------------------------------------------------

def analyser_identite_parcelle(
    section: str,
    numero: str,
    insee: str,
    commune: str
) -> Dict:
    """
    Fonction principale : orchestre l'analyse complète d'identité parcellaire
    
    Args:
        section: Section cadastrale
        numero: Numéro de parcelle
        insee: Code INSEE
        commune: Nom de la commune
    
    Returns:
        Dict: Résultat complet de l'analyse
    
    Raises:
        Exception: En cas d'erreur métier
    """
    logger.info(f"🚀 Début analyse identité parcellaire : {section} {numero} ({commune}, INSEE: {insee})")
    
    # 1. Normalisation
    section = section.upper().strip()
    numero = numero.zfill(4)
    logger.info(f"   → Normalisé : {section} {numero}")
    
    # 2. Récupération géométrie
    parcelle_wkt = fetch_parcelle_geometry_ign(section, numero, insee)
    
    # 3. Calcul intersections avec attributs discriminants
    intersections = calculate_intersections_detailed(parcelle_wkt)
    
    # 4. Tri alphabétique
    intersections.sort(key=lambda x: x["display_name"])
    
    result = {
        "parcelle": f"{section} {numero}",
        "commune": commune,
        "insee": insee,
        "nb_intersections": len(intersections),
        "intersections": intersections
    }
    
    logger.info(f"✅ Analyse terminée : {len(intersections)} intersection(s) trouvée(s)")
    return result