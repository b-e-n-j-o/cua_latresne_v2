import sqlite3
import gzip

conn = sqlite3.connect('plui_bordeaux.mbtiles')

# Récupérer la première tuile (la plus lourde)
row = conn.execute("""
    SELECT tile_data FROM tiles 
    WHERE zoom_level = 15 AND tile_column = 16333 AND tile_row = 20957
    LIMIT 1
""").fetchone()

if not row:
    print("❌ Tuile non trouvée")
    exit(1)

tile_data = row[0]

# Décompresser si gzip
if tile_data[:2] == b'\x1f\x8b':
    tile_data = gzip.decompress(tile_data)
    print("✅ Tuile décompressée")

# Sauvegarder pour inspection avec mbview
with open('sample_tile.mvt', 'wb') as f:
    f.write(tile_data)

print(f"✅ Tuile extraite: {len(tile_data)} bytes")
print("📁 Fichier: sample_tile.mvt")

# Essayer de décoder avec mapbox-vector-tile
try:
    import mapbox_vector_tile
    decoded = mapbox_vector_tile.decode(tile_data)
    print("\n🧱 COUCHES TROUVÉES:")
    for layer_name, layer_data in decoded.items():
        print(f"\n  Source-layer: '{layer_name}'")
        if layer_data['features']:
            first_feature = layer_data['features'][0]
            print(f"  Propriétés exemple:")
            for key, value in first_feature['properties'].items():
                print(f"    - {key}: {value}")
            break
except ImportError:
    print("\n⚠️ Installez mapbox-vector-tile: pip install mapbox-vector-tile")
except Exception as e:
    print(f"\n❌ Erreur décodage: {e}")

conn.close()