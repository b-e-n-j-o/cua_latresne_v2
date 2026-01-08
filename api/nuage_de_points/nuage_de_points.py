"""
Service métier pour génération nuage de points LIDAR
"""
import requests
import io
import laspy
import numpy as np
import geopandas as gpd
from pathlib import Path
from shapely import contains_xy
from typing import Dict, Optional
import asyncio
import os
from supabase import create_client, Client

# CONFIG
TEMP_DIR = Path("./temp_lidar")  # Temporaire pour downloads Supabase
OUTPUT_DIR = Path("./output_lidar")
CATALOGUE_PATH = Path("./api/nuage_de_points/catalogue_dalles_bordeaux.txt")

TEMP_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

IGN_WFS = "https://data.geopf.fr/wfs/ows"
IGN_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

# Supabase Storage
SUPABASE_BUCKET = "dalles-lidar"
MAX_CACHE_DALLES = 10

class NuageDePointsException(Exception):
    pass

# Client Supabase (initialisé à la demande)
_supabase_client: Optional[Client] = None

def get_supabase_client() -> Optional[Client]:
    """Initialise et retourne le client Supabase"""
    global _supabase_client
    if _supabase_client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SERVICE_KEY")
        if url and key:
            _supabase_client = create_client(url, key)
        else:
            # Supabase optionnel, on peut utiliser le cache local
            return None
    return _supabase_client

def load_catalogue():
    """Charge catalogue URLs dalles"""
    cat = {}
    if not CATALOGUE_PATH.exists():
        raise NuageDePointsException(f"Catalogue manquant: {CATALOGUE_PATH}")
    
    with open(CATALOGUE_PATH) as f:
        for line in f:
            line = line.strip()
            if line.startswith('http') and 'LHD_FXX_' in line:
                fname = line.split('/')[-1]
                parts = fname.split('_')
                dalle = f"{parts[2]}_{parts[3]}"
                cat[dalle] = line
    
    return cat

def get_parcelle_geometry(insee: str, section: str, numero: str):
    """Récupère géométrie IGN"""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LAYER,
        "srsName": "EPSG:2154",
        "outputFormat": "application/json",
        "CQL_FILTER": f"code_insee='{insee}' AND section='{section}' AND numero='{numero}'"
    }
    
    r = requests.get(IGN_WFS, params=params, timeout=30)
    r.raise_for_status()
    
    gdf = gpd.read_file(io.BytesIO(r.content))
    if gdf.empty:
        raise NuageDePointsException(f"Parcelle {section} {numero} non trouvée")
    
    return gdf.iloc[0].geometry, gdf.iloc[0]

def identify_tiles(bounds):
    """Identifie dalles nécessaires (avec correction +1 sur Y)"""
    minx, miny, maxx, maxy = bounds
    dalles = set()
    
    for x in [minx, maxx]:
        for y in [miny, maxy]:
            dx = int(x // 1000)
            dy = int(y // 1000) + 1  # Correction IGN
            dalles.add(f"{dx:04d}_{dy:04d}")
    
    return list(dalles)

def download_dalle(url: str, dalle_name: str):
    """Télécharge dalle depuis IGN"""
    fname = f"LHD_FXX_{dalle_name}_PTS_LAMB93_IGN69.copc.laz"
    local = TEMP_DIR / fname
    
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()
    
    with open(local, 'wb') as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    
    return local

async def get_dalle_from_supabase(dalle_name: str, catalogue: dict) -> Optional[Path]:
    """Récupère dalle depuis Supabase Storage avec FIFO"""
    supabase = get_supabase_client()
    if not supabase:
        return None
    
    fname = f"LHD_FXX_{dalle_name}_PTS_LAMB93_IGN69.copc.laz"
    storage_name = f"{dalle_name}.laz"
    
    try:
        # Vérifier si existe dans Supabase
        files = supabase.storage.from_(SUPABASE_BUCKET).list()
        file_exists = any(f.get('name') == storage_name for f in files)
        
        if file_exists:
            # Télécharger depuis Supabase
            data = supabase.storage.from_(SUPABASE_BUCKET).download(storage_name)
            local = TEMP_DIR / fname
            with open(local, 'wb') as f:
                f.write(data)
            return local
        
        # Pas en cache: télécharger IGN et uploader
        if dalle_name not in catalogue:
            return None
        
        local = download_dalle(catalogue[dalle_name], dalle_name)
        
        # Vérifier quota (max 10 dalles = 1GB) côté bucket
        files = supabase.storage.from_(SUPABASE_BUCKET).list()
        if len(files) >= MAX_CACHE_DALLES:
            # Supprimer la plus ancienne (FIFO)
            # Note: Supabase Storage ne fournit pas created_at par défaut
            # On utilise l'ordre de la liste (approximation)
            if files:
                oldest_name = files[0]['name']
                supabase.storage.from_(SUPABASE_BUCKET).remove([oldest_name])
        
        # Upload nouvelle dalle (sans écraser si déjà présente)
        with open(local, 'rb') as f:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                storage_name, 
                f.read(),
                file_options={
                    "content-type": "application/octet-stream",
                    "upsert": "false"
                }
            )
        
        return local
        
    except Exception as e:
        # En cas d'erreur Supabase, fallback sur cache local
        return None

async def get_dalle_paths(dalle_names, catalogue):
    """Récupère/télécharge dalles nécessaires via Supabase (cache principal)"""
    paths = []
    
    for dalle in dalle_names:
        supabase_path = await get_dalle_from_supabase(dalle, catalogue)
        if not supabase_path or not supabase_path.exists():
            raise NuageDePointsException(
                f"Dalle {dalle} introuvable dans Supabase et impossible à télécharger depuis l'IGN"
            )
        paths.append(supabase_path)
    
    if not paths:
        raise NuageDePointsException("Aucune dalle disponible")
    
    return paths

def extract_cloud(paths, polygon):
    """Extrait points des dalles pour la parcelle"""
    all_x, all_y, all_z, all_c = [], [], [], []
    
    for p in paths:
        las = laspy.read(str(p))
        mask = contains_xy(polygon, las.x, las.y)
        nb = mask.sum()
        
        if nb > 0:
            all_x.append(np.array(las.x[mask]))
            all_y.append(np.array(las.y[mask]))
            all_z.append(np.array(las.z[mask]))
            all_c.append(np.array(las.classification[mask]))
    
    if not all_x:
        raise NuageDePointsException("Aucun point trouvé dans la parcelle")
    
    x = np.concatenate(all_x)
    y = np.concatenate(all_y)
    z = np.concatenate(all_z)
    c = np.concatenate(all_c)
    
    # Statistiques classes
    stats = {}
    for cl in np.unique(c):
        n = (c == cl).sum()
        stats[int(cl)] = {
            'count': int(n),
            'percent': float(n / len(c) * 100)
        }
    
    return {'x': x, 'y': y, 'z': z, 'class': c, 'stats': stats}

def export_laz(data, file_id: str):
    """Exporte LAZ"""
    out = OUTPUT_DIR / f"{file_id}.laz"
    
    h = laspy.LasHeader(point_format=6, version="1.4")
    h.offsets = [data['x'].min(), data['y'].min(), data['z'].min()]
    h.scales = [0.01, 0.01, 0.01]
    
    las = laspy.LasData(h)
    las.x = data['x']
    las.y = data['y']
    las.z = data['z']
    las.classification = data['class']
    
    las.write(str(out))
    return out

async def generer_nuage_parcelle(insee: str, section: str, numero: str) -> Dict:
    """Pipeline complet génération nuage"""
    
    catalogue = load_catalogue()
    polygon, parcelle = get_parcelle_geometry(insee, section, numero)
    dalles = identify_tiles(polygon.bounds)
    paths = await get_dalle_paths(dalles, catalogue)
    data = extract_cloud(paths, polygon)
    
    file_id = f"parcelle_{insee}_{section}_{numero}"
    export_laz(data, file_id)
    
    return {
        "file_id": file_id,
        "commune": parcelle.get('nom_com', 'N/A'),
        "surface": float(polygon.area),
        "nb_points": len(data['x']),
        "alt_min": float(data['z'].min()),
        "alt_max": float(data['z'].max()),
        "classes": data['stats']
    }

def convertir_laz_to_json(file_path: Path) -> Dict:
    """Convertit LAZ en JSON pour visualisation web"""
    las = laspy.read(str(file_path))
    
    points = []
    for i in range(len(las.x)):
        points.append({
            'x': float(las.x[i]),
            'y': float(las.y[i]),
            'z': float(las.z[i]),
            'class': int(las.classification[i])
        })
    
    return {
        'points': points,
        'count': len(points)
    }