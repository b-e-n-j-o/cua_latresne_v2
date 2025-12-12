import os
import logging
from datetime import datetime
from mistralai import Mistral
from dotenv import load_dotenv

load_dotenv()

# ======================================================
# CONFIG LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("mistral_audit")

# ======================================================
# API KEY CHARGEMENT
# ======================================================
API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise ValueError("⚠️ Tu dois définir MISTRAL_API_KEY dans ton environnement export MISTRAL_API_KEY='xxx'")

logger.info("🔐 Clé API chargée.")


# ======================================================
# FONCTION DE TEST API
# ======================================================
def run_mistral_audit():

    logger.info("🚀 Début du test Mistral AI")

    with Mistral(api_key=API_KEY) as client:

        messages = [
            {
                "role": "user",
                "content": "Donne une phrase en Français expliquant pourquoi Latresne est agréable à vivre.",
            }
        ]

        logger.info("📩 Envoi de la requête au modèle...")

        response = client.chat.complete(
            model="ministral-3b-2512",
            messages=messages,
            stream=False
        )

        logger.info("🤖 Réponse reçue !")

        # ======================================================
        # EXTRACTION DES INFORMATIONS
        # ======================================================
        # Correction du parsing selon le nouveau SDK Mistral
        assistant_msg = response.choices[0].message

        try:
            output_text = assistant_msg.content.strip()
        except Exception:
            output_text = str(assistant_msg)

        usage = response.usage  # tokens count

        logger.info("=================== ✨ AUDIT API MISTRAL ✨ ===================")
        logger.info(f"📌 Modèle utilisé        : ministral-3b-2512")
        logger.info(f"📩 Message Input         : {messages[0]['content']}")
        logger.info(f"💬 Réponse modèle        : {output_text}")
        logger.info("🔢 TOKENS")
        logger.info(f"    ➤ Input Tokens       : {usage.prompt_tokens}")
        logger.info(f"    ➤ Output Tokens      : {usage.completion_tokens}")
        logger.info(f"    ➤ Total Tokens       : {usage.total_tokens}")
        logger.info("==============================================================")

        print("\n🟢 **Réponse brute JSON renvoyée par l'API**:")
        print(response)


# ======================================================
# EXECUTION
# ======================================================
if __name__ == "__main__":
    run_mistral_audit()
