#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Appel API textuel simple avec Gemini 2.5 Pro (clé via .env)."""

import os
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = "gemini-2.5-pro"

PROMPT = "En une phrase, décris ce qu'est un formulaire CERFA d'urbanisme."


def main():
    if not API_KEY:
        print("❌ GEMINI_API_KEY manquante dans l'environnement")
        return

    genai.configure(api_key=API_KEY)
    model = genai.GenerativeModel(MODEL)
    response = model.generate_content(PROMPT)
    print(response.text.strip())


if __name__ == "__main__":
    main()
