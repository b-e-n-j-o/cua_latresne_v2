# test_chat_endpoint.py
import requests

from dotenv import load_dotenv

load_dotenv()

API_BASE = "http://localhost:8000"

def test_chat_plu():
    payload = {
        "insee": "33063",  # Bordeaux
        "zone": "UP27",
        "question": "Quelle est la hauteur maximale autorisée pour une construction ?",
        "conversation_history": []
    }
    
    print("🚀 Test endpoint /api/plu/chat")
    print(f"Question: {payload['question']}")
    print("─" * 50)
    
    response = requests.post(
        f"{API_BASE}/api/plu/chat",
        json=payload,
        timeout=60
    )
    
    if response.ok:
        data = response.json()
        print("✅ Réponse reçue:")
        print(data["answer"])
        print(f"\nZone: {data['zone']}")
    else:
        print(f"❌ Erreur {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_chat_plu()