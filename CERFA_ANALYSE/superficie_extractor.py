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
Tu lis UNIQUEMENT cette page 4 d'un CERFA 13410*12.
Ton objectif est très simple :

📌 Extraire : "Superficie totale du terrain (en m²)"

Renvoie STRICTEMENT :
{
  "superficie_totale_m2": <valeur entière ou null>
}

Pas d'autre texte, pas d'explication.
"""

def extract_page4(pdf_path):
    pdf = PdfReader(pdf_path)
    if len(pdf.pages) < 4:
        raise ValueError("Le PDF n'a pas de page 4.")
    return pdf.pages[3].extract_text()  # page index 3 = page 4 réelle

def ask_gemini(text, model):
    try:
        m = genai.GenerativeModel(model)
        r = m.generate_content([text, PROMPT])
        t = r.text or ""
        i, j = t.find("{"), t.rfind("}")
        return json.loads(t[i:j+1])
    except:
        return None

def test_superficie(pdf_path):
    page_text = extract_page4(pdf_path)
    data = ask_gemini(page_text, MODEL_PRIMARY)
    if data is None:
        data = ask_gemini(page_text, MODEL_FALLBACK)
    return data

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 test_superficie_page4.py cerfa.pdf")
        exit(1)

    res = test_superficie(sys.argv[1])
    print(json.dumps(res, indent=2, ensure_ascii=False))
