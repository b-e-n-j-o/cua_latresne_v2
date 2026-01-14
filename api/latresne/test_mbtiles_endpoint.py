import requests

API_BASE = "http://localhost:8000"  # adapte si besoin
LAYER = "plu"

# Coordonnées arbitraires mais réalistes pour Latresne
# (zoom 13–15 recommandé pour un premier test)
TEST_TILES = [
    (13, 4084, 2953),
    (13, 4085, 2953),
    (14, 8169, 5907),
    (15, 16338, 11814),
]

def test_tile(z, x, y):
    url = f"{API_BASE}/latresne/mbtiles/{LAYER}/{z}/{x}/{y}.mvt"
    r = requests.get(url)

    print(f"\nGET {url}")
    print("Status:", r.status_code)
    print("Content-Type:", r.headers.get("Content-Type"))
    print("Size:", len(r.content), "bytes")

    if r.status_code != 200:
        print("❌ ERROR")
    elif len(r.content) == 0:
        print("⚠️ Tuile vide (possible mais à confirmer)")
    else:
        print("✅ Tuile OK")

if __name__ == "__main__":
    for z, x, y in TEST_TILES:
        test_tile(z, x, y)
