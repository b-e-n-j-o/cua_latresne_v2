#!/usr/bin/env python
import json
import requests

BASE_URL = "http://localhost:8000"

TEST_REQUESTS = [
    # mêmes valeurs que dans les logs 404
    ("33063", "OX", "0025"),
    ("33063", "OY", "0138"),
]


def test_zonage(insee: str, section: str, numero: str):
    url = f"{BASE_URL}/zonage-plui/{insee}/{section}/{numero}"
    print(f"\n=== Test zonage PLUi pour parcelle {section}{numero} ({insee}) ===")
    print(f"URL: {url}")
    try:
        resp = requests.get(url, timeout=60)
    except Exception as e:
        print(f"❌ Erreur de requête: {e}")
        return

    print(f"Statut HTTP: {resp.status_code}")
    ctype = resp.headers.get("content-type", "")
    if ctype.startswith("application/json"):
        try:
            data = resp.json()
            print("Body JSON formaté :")
            print(json.dumps(data, ensure_ascii=False, indent=2))
            print(
                f"→ typezone={data.get('typezone')!r}, "
                f"etiquette={data.get('etiquette')!r}, "
                f"message={data.get('message')!r}"
            )
        except Exception as e:
            print(f"❌ Erreur parsing JSON: {e}")
            print("Réponse brute:")
            print(resp.text[:1000])
    else:
        print(f"Content-Type inattendu: {ctype}")
        print("Réponse brute:")
        print(resp.text[:1000])


def main():
    for insee, section, numero in TEST_REQUESTS:
        test_zonage(insee, section, numero)


if __name__ == "__main__":
    main()