# tests/test_plu_zonage.py
import requests

API_BASE = "http://localhost:8000"
INSEE = "33063"  # Bordeaux
LON, LAT = -0.551957,44.827353  # Coordonnées en zone UM12

def test_zonage_detection():
    """Test détection zone PLUI"""
    r = requests.get(f"{API_BASE}/api/plu/zonage/{INSEE}?lon={LON}&lat={LAT}")
    print(f"Status: {r.status_code}")
    print(f"Response: {r.text[:500]}")
    assert r.status_code == 200
    assert "zones" in r.json()
    zones = r.json()["zones"]
    print(f"\n📍 Zonage détecté: {zones}")
    assert len(zones) > 0
    return zones[0]

def test_reglement_zone():
    """Test récupération règlement par zone"""
    zone = test_zonage_detection()
    
    r = requests.get(f"{API_BASE}/api/plu/reglement/{INSEE}/zone/{zone}")
    print(f"\n📥 Règlement zone {zone}: {r.status_code}")
    assert r.status_code == 200
    
    data = r.json()
    print(f"📄 URL: {data['url'][:80]}...")
    
    # Télécharger le PDF
    pdf_response = requests.get(data['url'])
    print(f"📊 Taille PDF: {len(pdf_response.content) / 1024:.1f} Ko")
    assert len(pdf_response.content) > 1000
    
    with open(f"test_{zone}.pdf", "wb") as f:
        f.write(pdf_response.content)
    print(f"✅ Sauvegardé: test_{zone}.pdf")

if __name__ == "__main__":
    test_reglement_zone()