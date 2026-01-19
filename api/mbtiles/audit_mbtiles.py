import sqlite3
import sys
from collections import defaultdict

if len(sys.argv) != 2:
    print("Usage: python audit_mbtiles.py <file.mbtiles>")
    sys.exit(1)

MBTILES = sys.argv[1]

conn = sqlite3.connect(MBTILES)
cur = conn.cursor()

print("\n==============================")
print("📦 AUDIT MBTILES :", MBTILES)
print("==============================\n")

# --------------------------------------------------
# 1. Metadata
# --------------------------------------------------
print("🔎 METADATA\n-----------")
cur.execute("SELECT name, value FROM metadata")
metadata = dict(cur.fetchall())
for k in sorted(metadata):
    print(f"{k:25} {metadata[k]}")

# --------------------------------------------------
# 2. Global stats
# --------------------------------------------------
print("\n📊 STATS GLOBALES\n----------------")
cur.execute("SELECT COUNT(*) FROM tiles")
total_tiles = cur.fetchone()[0]
print(f"Nombre total de tuiles : {total_tiles}")

cur.execute("SELECT SUM(length(tile_data)) FROM tiles")
total_bytes = cur.fetchone()[0] or 0
print(f"Taille totale brute    : {total_bytes / 1024 / 1024:.2f} MB")

# --------------------------------------------------
# 3. Stats par zoom
# --------------------------------------------------
print("\n🔍 PAR ZOOM\n----------")
cur.execute("""
    SELECT zoom_level,
           COUNT(*) AS nb_tiles,
           SUM(length(tile_data)) AS bytes
    FROM tiles
    GROUP BY zoom_level
    ORDER BY zoom_level
""")

zoom_stats = cur.fetchall()
for z, n, b in zoom_stats:
    print(f"z{z:2} : {n:6} tuiles | {b/1024/1024:8.2f} MB")

# --------------------------------------------------
# 4. Tuiles les plus lourdes
# --------------------------------------------------
print("\n🚨 TOP 20 TUILES LES PLUS LOURDES\n--------------------------------")
cur.execute("""
    SELECT zoom_level, tile_column, tile_row,
           length(tile_data) AS size
    FROM tiles
    ORDER BY size DESC
    LIMIT 20
""")

for z, x, y, size in cur.fetchall():
    print(f"z{z} x{x} y{y} → {size/1024:.1f} KB")

# --------------------------------------------------
# 5. Distribution des tailles
# --------------------------------------------------
print("\n📐 DISTRIBUTION DES TAILLES DE TUILES\n-----------------------------------")
buckets = defaultdict(int)

cur.execute("SELECT length(tile_data) FROM tiles")
for (size,) in cur.fetchall():
    if size < 50_000:
        buckets["<50 KB"] += 1
    elif size < 100_000:
        buckets["50–100 KB"] += 1
    elif size < 250_000:
        buckets["100–250 KB"] += 1
    elif size < 500_000:
        buckets["250–500 KB"] += 1
    else:
        buckets[">500 KB"] += 1

for k in ["<50 KB", "50–100 KB", "100–250 KB", "250–500 KB", ">500 KB"]:
    print(f"{k:12} : {buckets[k]}")

# --------------------------------------------------
# 6. Couches vectorielles (si présentes)
# --------------------------------------------------
if "json" in metadata:
    print("\n🧱 COUCHES VECTORIELLES\n---------------------")
    print(metadata["json"])

conn.close()
