import requests
import os

from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

def get_sentinel_token(client_id: str, client_secret: str) -> str:
    url = "https://services.sentinel-hub.com/auth/realms/main/protocol/openid-connect/token"

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(url, data=payload)

    if response.status_code == 200:
        data = response.json()
        token = data["access_token"]
        expires_in = data["expires_in"]  # Durée de validité en secondes (ex: 3600 = 1 heure)
        print(f"✅ Jeton récupéré avec succès (Valide pour {expires_in} secondes)")
        print(token)
        return token
    else:
        raise Exception(
            f"❌ Erreur {response.status_code} lors de la récupération du token : {response.text}"
        )


if __name__ == "__main__":
    get_sentinel_token(CLIENT_ID, CLIENT_SECRET)