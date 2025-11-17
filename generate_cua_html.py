import base64
import json

# 🟦 TON URL DU DOCX DANS SUPABASE
docx_url = "https://odlkagfeqkbrruajlcxm.supabase.co/storage/v1/object/public/visualisation/7fniMsdtcVUFK6KNasJTsWmA2N/CUA_unite_fonciere.docx"

# 🟦 Construire le payload attendu par /cua
payload = {
    "docx": docx_url
}

# 🟦 Base64
token = base64.b64encode(json.dumps(payload).encode()).decode()

# 🟦 URL finale
viewer_url = f"https://kerelia.fr/cua?t={token}"

print("URL CUA viewer 👉", viewer_url)
