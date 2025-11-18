#!/usr/bin/env python3
import os, json
from pathlib import Path
from pypdf import PdfReader
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

MODEL_PRIMARY = "gemini-2.5-flash"
MODEL_FALLBACK = "gemini-2.5-flash"

PROMPT = """
Tu lis UNIQUEMENT les pages 2 et 4 d'un CERFA 13410*12.

🎯 Objectif UNIQUE :
Extraire toutes les parcelles cadastrales (section + numéro), en considérant :

────────────────────────────────────────────
📌 COMMENT LES PARCELLES SONT PRÉSENTÉES
────────────────────────────────────────────

💠 Page 2 — Section 4.1 (première apparition possible)
On trouve parfois une seule parcelle, sous forme éclatée :

Préfixe : 000 Section : XXXX Numéro : XXXX


💠 Page 4 — Section 4.2 (toutes les références cadastrales)
Exemples de formats possibles :
- Section : AI  Numéro : 0310  Superficie : 5755 m²
- Section : AC  Numéro : 0058  Superficie : 256 m²
- Section : AC  Numéro : 0311  Superficie : 1368 m²
→ Il peut y avoir plusieurs lignes.
→ Il peut y avoir une page annexe, mais ici on analyse seulement page 4.

────────────────────────────────────────────
📌 RÈGLES D’EXTRACTION
────────────────────────────────────────────

1. Tu dois retourner TOUTES les parcelles trouvées sur les pages 2 ET 4.
2. Une parcelle est définie par :
   - section : 1 à 2 lettres majuscules (AC, AI, ZA…)
   - numero : exactement 4 chiffres (avec zéros initiaux)
3. IGNORE la superficie complète du terrain.
4. Si une même parcelle apparaît plusieurs fois → une seule occurrence dans la liste.
5. Ne devine rien : si un numéro est incomplet, mets null.
6. Le résultat doit contenir EXACTEMENT ce JSON :

{
  "parcelles": [
    {"section": "AC", "numero": "0310"},
    {"section": "AI", "numero": "0058"}
  ]
}

7. Aucune autre clé.
8. Aucune explication.
9. Aucun texte en dehors du JSON strict.

────────────────────────────────────────────
RENVOIE UNIQUEMENT LE JSON.
────────────────────────────────────────────
"""
  # page index 3 = page 4 réelle

def ask_gemini(text, model):
    try:
        m = genai.GenerativeModel(model)
        r = m.generate_content([text, PROMPT])
        t = r.text or ""
        i, j = t.find("{"), t.rfind("}")
        return json.loads(t[i:j+1])
    except:
        return None

def test_parcelles(pdf_path):
    content = pdf = PdfReader(pdf_path)

    data = ask_gemini(content, MODEL_PRIMARY)
    if data is None:
        data = ask_gemini(content, MODEL_FALLBACK)

    return data

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 test_parcelles_pages2_4.py cerfa.pdf")
        exit(1)

    res = test_parcelles(sys.argv[1])
    print(json.dumps(res, indent=2, ensure_ascii=False))
