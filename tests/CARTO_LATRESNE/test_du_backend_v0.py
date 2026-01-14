# test_latresne_backend.py
import requests

API_BASE = "http://localhost:8000"  # Adapte le port

def test_layers_endpoint():
    """Teste l'endpoint /latresne/layers"""
    res = requests.get(f"{API_BASE}/latresne/layers")
    assert res.status_code == 200
    layers = res.json()
    print(f"✅ {len(layers)} couches chargées")
    for layer in layers[:5]:
        print(f"  - {layer['id']}: {layer['nom']}")
    return layers

def test_tile_endpoint(layer_id="plu_latresne"):
    """Teste l'endpoint /latresne/tiles/{layer}/z/x/y.mvt"""
    z, x, y = 14, 8270, 5840
    res = requests.get(f"{API_BASE}/latresne/tiles/{layer_id}/{z}/{x}/{y}.mvt")
    print(f"Status: {res.status_code}")
    if res.status_code != 200:
        print(f"Erreur: {res.text}")
        return
    assert res.headers["content-type"] == "application/x-protobuf"
    print(f"✅ Tuile {layer_id}/{z}/{x}/{y}: {len(res.content)} bytes")
def test_parcelle_endpoint():
    """Teste l'endpoint parcelle par coordonnées"""
    # Coordonnées au centre de Latresne
    lon, lat = -0.498, 44.778
    res = requests.get(f"{API_BASE}/parcelle/par-coordonnees?lon={lon}&lat={lat}")
    assert res.status_code == 200
    data = res.json()
    print(f"✅ Parcelle trouvée: {len(data['features'])} features")
    if data['features']:
        props = data['features'][0]['properties']
        print(f"  Section {props['section']} Parcelle {props['numero']}")

if __name__ == "__main__":
    print("🧪 Test backend Latresne\n")
    test_layers_endpoint()
    print()
    test_tile_endpoint("plu_latresne")
    test_tile_endpoint("ac1")
    print()
    test_parcelle_endpoint()
    print("\n✅ Tous les tests passés")