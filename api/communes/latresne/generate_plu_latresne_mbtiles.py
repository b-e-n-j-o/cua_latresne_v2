import os
import sys
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import mercantile
from tqdm import tqdm
import fiona
from shapely.geometry import shape, mapping, box
from shapely.strtree import STRtree
from shapely.ops import transform
from pyproj import Transformer
import mapbox_vector_tile


# Chemin de sortie (répertoire courant si /mnt/tiles-cache n'existe pas)
OUTPUT_DIR = "/mnt/tiles-cache" if os.path.exists("/mnt/tiles-cache") else os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "plu_latresne.mbtiles")

# Emprise de Latresne (en WGS84, comme avant)
BOUNDS = (-0.624233, 44.729314, -0.371891, 44.826553)
ZOOM_MIN, ZOOM_MAX = 14, 19

# Shapefile source (PLU)
SHAPEFILE_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/FLAVIO/Urba33 hors RGE Alti/33234_PLU_20170213 2/Donnees_geographiques/ZONE_URBA.shp"

# CRS source du shapefile (à ajuster si besoin)
# Les PLU Urba33 sont généralement en Lambert 93
SOURCE_CRS = "EPSG:2154"
TARGET_CRS = "EPSG:3857"

# Batch & parallélisme
BATCH_SIZE = 10_000          # Nombre d'INSERTs avant commit
NUM_WORKERS = max(1, int(os.getenv("MVT_WORKERS", "4")))  # Threads pour générer les MVT


# Index spatial global en mémoire (partagé entre les threads)
TREE = None
GEOM_TO_PROPS = None


def load_features():
    """
    Charge le shapefile en mémoire, reprojette en 3857 et construit un STRtree.
    On garde les propriétés nécessaires pour le style côté frontend.
    """
    print(f"📂 Chargement du shapefile : {SHAPEFILE_PATH}")
    if not os.path.exists(SHAPEFILE_PATH):
        print(f"❌ Shapefile introuvable : {SHAPEFILE_PATH}")
        sys.exit(1)

    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)

    geoms = []
    props_list = []

    with fiona.open(SHAPEFILE_PATH, "r") as src:
        total_features = len(src)
        print(f"📦 {total_features} entités lues depuis le shapefile")

        for feat in src:
            geom = shape(feat["geometry"])
            if geom.is_empty:
                continue

            # Reprojection en 3857
            geom_3857 = transform(transformer.transform, geom)

            # Propriétés : on essaie de garder les mêmes noms que dans la table SQL
            p = feat["properties"]
            zonage = p.get("zonage_reglement") or p.get("ZONAGE_REG") or p.get("ZONAGE") or p.get("zone", "")
            fid = p.get("id") or p.get("ID") or p.get("FID") or p.get("OBJECTID")

            geoms.append(geom_3857)
            props_list.append(
                {
                    "zonage_reglement": zonage,
                    "id": fid,
                }
            )

    tree = STRtree(geoms)
    geom_to_props = dict(zip(geoms, props_list))

    print(f"✅ Index spatial construit ({len(geoms)} géométries)")
    return tree, geom_to_props


def init_spatial_index():
    """Initialise les variables globales TREE et GEOM_TO_PROPS."""
    global TREE, GEOM_TO_PROPS
    if TREE is None or GEOM_TO_PROPS is None:
        TREE, GEOM_TO_PROPS = load_features()


def build_tile_mvt(z, x, y):
    """
    Construit le MVT d'une tuile (z, x, y) à partir du STRtree en mémoire.
    Retourne un BLOB (bytes) ou None si aucun contenu.
    """
    global TREE, GEOM_TO_PROPS

    if TREE is None or GEOM_TO_PROPS is None:
        init_spatial_index()

    # Envelope de la tuile en 3857
    tile = mercantile.Tile(x=x, y=y, z=z)
    bounds_3857 = mercantile.xy_bounds(tile)
    tile_bbox = box(bounds_3857.left, bounds_3857.bottom, bounds_3857.right, bounds_3857.top)

    candidates = TREE.query(tile_bbox)
    features = []

    for geom in candidates:
        if not geom.intersects(tile_bbox):
            continue
        clipped = geom.intersection(tile_bbox)
        if clipped.is_empty:
            continue

        props = GEOM_TO_PROPS[geom]
        features.append(
            {
                "geometry": mapping(clipped),
                "properties": props,
                "id": props.get("id"),
            }
        )

    if not features:
        return None

    # Encodage MVT (nom de couche conservé : 'plu_latresne')
    tile_data = mapbox_vector_tile.encode({"plu_latresne": features}, extent=4096)
    return tile_data


def _worker_generate_tile(args):
    """Worker pour ThreadPoolExecutor : génère la tuile MVT."""
    z, x, y, y_tms = args
    mvt = build_tile_mvt(z, x, y)
    return z, x, y_tms, mvt


def main():
    # Créer le répertoire de sortie s'il n'existe pas
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Créer / ouvrir le MBTiles
    mbtiles = sqlite3.connect(OUTPUT_FILE)
    mbtiles.execute(
        """
        CREATE TABLE IF NOT EXISTS metadata (name TEXT, value TEXT)
        """
    )
    mbtiles.execute(
        """
        CREATE TABLE IF NOT EXISTS tiles (
            zoom_level INTEGER,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_data BLOB,
            PRIMARY KEY (zoom_level, tile_column, tile_row)
        )
        """
    )

    # Metadata
    mbtiles.execute("DELETE FROM metadata")
    mbtiles.executemany(
        "INSERT INTO metadata VALUES (?, ?)",
        [
            ("name", "PLU Latresne"),
            ("format", "pbf"),
            ("bounds", f"{BOUNDS[0]},{BOUNDS[1]},{BOUNDS[2]},{BOUNDS[3]}"),
            ("minzoom", str(ZOOM_MIN)),
            ("maxzoom", str(ZOOM_MAX)),
            ("type", "overlay"),
        ],
    )

    # Charger les tuiles existantes (pour reprise éventuelle)
    existing = set()
    try:
        cursor = mbtiles.execute("SELECT zoom_level, tile_column, tile_row FROM tiles")
        for row in cursor:
            z0, x0, y_tms0 = row
            existing.add((z0, x0, y_tms0))
        print(f"📦 {len(existing):,} tuiles déjà présentes dans le MBTiles")
    except sqlite3.OperationalError:
        print("📦 Aucune tuile existante, génération complète")

    # Générer la liste totale de tuiles (pour tqdm)
    total = sum(len(list(mercantile.tiles(*BOUNDS, z))) for z in range(ZOOM_MIN, ZOOM_MAX + 1))
    print(f"🚀 Génération de ~{total:,} tuiles (sur la base du shapefile)")
    print(f"📦 Taille des batches (INSERTs) : {BATCH_SIZE:,} tuiles")
    print(f"🧵 Threads de génération MVT : {NUM_WORKERS}")
    print()

    generated = 0
    skipped = 0
    total_size_bytes = 0
    zoom_stats = {}
    batch_count = 0  # Compteur d'INSERTs depuis le dernier commit

    # Initialiser l'index spatial avant de lancer les threads
    init_spatial_index()

    try:
        with tqdm(total=total) as pbar:
            for z in range(ZOOM_MIN, ZOOM_MAX + 1):
                zoom_generated = 0
                zoom_size_bytes = 0
                zoom_sizes = []

                # Préparer la liste des tuiles à traiter pour ce zoom
                tiles_to_process = []
                for tile in mercantile.tiles(*BOUNDS, z):
                    y_tms = (2**z - 1) - tile.y

                    # Skip si déjà présente
                    if (z, tile.x, y_tms) in existing:
                        skipped += 1
                        pbar.update(1)
                        continue

                    tiles_to_process.append((z, tile.x, tile.y, y_tms))

                if not tiles_to_process:
                    continue

                # Génération parallèle des MVT pour ce zoom
                with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
                    for z_t, x_t, y_tms_t, mvt in executor.map(_worker_generate_tile, tiles_to_process):
                        if mvt and len(mvt) > 0:
                            mvt_size = len(mvt)
                            zoom_sizes.append(mvt_size)
                            zoom_size_bytes += mvt_size

                            mbtiles.execute(
                                "INSERT OR REPLACE INTO tiles VALUES (?, ?, ?, ?)",
                                (z_t, x_t, y_tms_t, mvt),
                            )
                            zoom_generated += 1
                            generated += 1
                            batch_count += 1

                            if batch_count >= BATCH_SIZE:
                                mbtiles.commit()
                                print(
                                    f"\n💾 Batch sauvegardé : {batch_count:,} tuiles commitées "
                                    f"(total générées: {generated:,})"
                                )
                                batch_count = 0

                        pbar.update(1)

                # Statistiques par niveau de zoom
                if zoom_generated > 0:
                    avg_size = zoom_size_bytes / zoom_generated
                    min_size = min(zoom_sizes)
                    max_size = max(zoom_sizes)
                    zoom_stats[z] = {
                        "count": zoom_generated,
                        "total_bytes": zoom_size_bytes,
                        "avg_bytes": avg_size,
                        "min_bytes": min_size,
                        "max_bytes": max_size,
                    }
                    total_size_bytes += zoom_size_bytes

                    print(
                        f"\n📊 Zoom {z:2d}: {zoom_generated:,} tuiles | "
                        f"Total: {zoom_size_bytes / 1024**2:.2f} MB | "
                        f"Avg: {avg_size / 1024:.1f} KB | "
                        f"Min: {min_size / 1024:.1f} KB | "
                        f"Max: {max_size / 1024:.1f} KB"
                    )

                # Commit à la fin de chaque niveau de zoom (pour les tuiles restantes du batch)
                if batch_count > 0:
                    mbtiles.commit()
                    print(f"💾 Fin zoom {z}: {batch_count:,} tuiles restantes commitées")
                    batch_count = 0

        # Commit final pour les tuiles restantes
        if batch_count > 0:
            mbtiles.commit()
            print(f"\n💾 Commit final : {batch_count:,} tuiles restantes sauvegardées")

        # Commit final de sécurité
        mbtiles.commit()

    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption utilisateur détectée")
        print("💾 Sauvegarde des données en cours...")
        if batch_count > 0:
            mbtiles.commit()
            print(f"✅ {batch_count:,} tuiles sauvegardées avant arrêt")
        mbtiles.commit()
        mbtiles.close()
        print(f"✅ Progression sauvegardée : {generated:,} tuiles générées")
        sys.exit(0)

    except Exception as e:
        print(f"\n\n❌ Erreur fatale : {str(e)}")
        print("💾 Tentative de sauvegarde des données...")
        try:
            if batch_count > 0:
                mbtiles.commit()
                print(f"✅ {batch_count:,} tuiles sauvegardées avant erreur")
            mbtiles.commit()
            mbtiles.close()
        except Exception:
            print("⚠️  Impossible de sauvegarder les données")
        print(f"✅ Progression sauvegardée : {generated:,} tuiles générées")
        raise

    # Fermeture normale
    mbtiles.close()

    # Taille finale du fichier
    final_size = os.path.getsize(OUTPUT_FILE)

    print()
    print("=" * 70)
    print(f"✅ {generated:,} tuiles générées → {OUTPUT_FILE}")
    if skipped > 0:
        print(f"⏭️  {skipped:,} tuiles skippées (déjà existantes)")
    print(f"📦 Taille totale du fichier MBTiles : {final_size / 1024**2:.2f} MB")
    if total_size_bytes > 0:
        print(f"📊 Taille des données MVT : {total_size_bytes / 1024**2:.2f} MB")
        print(f"💾 Overhead SQLite : {(final_size - total_size_bytes) / 1024**2:.2f} MB")
    print()
    print("📈 Répartition par niveau de zoom :")
    for z in sorted(zoom_stats.keys()):
        stats = zoom_stats[z]
        pct = (stats["total_bytes"] / total_size_bytes * 100) if total_size_bytes > 0 else 0
        print(
            f"   Zoom {z:2d}: {stats['count']:6,} tuiles | "
            f"{stats['total_bytes'] / 1024**2:7.2f} MB ({pct:5.1f}%) | "
            f"Avg: {stats['avg_bytes'] / 1024:6.1f} KB"
        )
    print("=" * 70)


if __name__ == "__main__":
    main()

