import requests
import json

API_BASE = "http://localhost:8000"  # ou ton URL prod
ENDPOINT = f"{API_BASE}/rag-meta-synthese"

payload = {
    "query": "Une construction est-elle autorisée en zone inondable ?",
    "legal_result": {
        "response": (
            "Le Code de l’urbanisme prévoit que les constructions peuvent être "
            "restreintes ou interdites dans les zones exposées à des risques naturels."
        ),
        "sources": [
            {
                "article_id": "L.101-2",
                "title": "Principes généraux",
            }
        ],
    },
    "plu_result": {
        "response": (
            "Le PLU classe la zone concernée en zone N inconstructible en raison du risque inondation."
        ),
        "sources": [
            {
                "article_id": "PLU-ZN-1",
                "title": "Zone naturelle inondable",
            }
        ],
    },
}

res = requests.post(
    ENDPOINT,
    headers={"Content-Type": "application/json"},
    data=json.dumps(payload),
)

print("Status:", res.status_code)
print("Response:\n")
print(res.json()["response"])
