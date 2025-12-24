import requests

BAN_URL = "https://api-adresse.data.gouv.fr/search/"

def geocode(adresse: str):
    r = requests.get(
        BAN_URL,
        params={
            "q": adresse,
            "limit": 1
        },
        timeout=10
    )
    r.raise_for_status()
    data = r.json()

    if not data["features"]:
        print("❌ Adresse introuvable")
        return

    feature = data["features"][0]
    lon, lat = feature["geometry"]["coordinates"]

    print("✅ Adresse trouvée")
    print(f"Adresse : {feature['properties'].get('label')}")
    print(f"Commune : {feature['properties'].get('city')}")
    print(f"INSEE   : {feature['properties'].get('citycode')}")
    print(f"Coordonnées (EPSG:4326) : lon={lon}, lat={lat}")

if __name__ == "__main__":
    geocode("12 rue des écoles, Latresne")
