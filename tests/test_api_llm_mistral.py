#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test simple de l'API Mistral (hors pipeline CERFA)
-------------------------------------------------
Permet de tester différents modèles Mistral
avec un prompt court et isolé.
"""

import os
import time
from dotenv import load_dotenv

from mistralai import Mistral




# ============================================================
# Chargement de la clé API
# ============================================================

load_dotenv()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

if not MISTRAL_API_KEY:
    raise RuntimeError("❌ MISTRAL_API_KEY manquante dans le .env")

client = Mistral(api_key=MISTRAL_API_KEY)

# ============================================================
# Configuration test
# ============================================================

MODEL = "mistral-large-latest"  # ← change-le à la main
PROMPT = "Peux-tu répondre uniquement par 'OK' ?"

# ============================================================
# Test API
# ============================================================

def test_model(model_name: str):
    print("=" * 60)
    print(f"🧪 Test du modèle : {model_name}")
    print("=" * 60)

    messages = [
        ChatMessage(role="system", content="Tu es un assistant de test."),
        ChatMessage(role="user", content=PROMPT),
    ]

    start = time.time()

    try:
        response = client.chat.complete(
            model=model_name,
            messages=messages,
            temperature=0.0,
            max_tokens=10,
        )

        duration = time.time() - start
        content = response.choices[0].message.content

        print("✅ Succès")
        print(f"⏱️  Temps de réponse : {duration:.2f}s")
        print(f"📤 Réponse : {content!r}")

    except SDKError as e:
        duration = time.time() - start
        print("❌ Erreur API Mistral")
        print(f"⏱️  Temps avant erreur : {duration:.2f}s")
        print(f"📛 Détails : {e}")

    except Exception as e:
        duration = time.time() - start
        print("❌ Erreur inattendue")
        print(f"⏱️  Temps avant erreur : {duration:.2f}s")
        print(f"📛 Détails : {e}")


if __name__ == "__main__":
    test_model(MODEL)
