#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline simple : extraction parcelles avec Gemini Vision
"""

import io
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from pdf2image import convert_from_path

from google import genai
from google.genai import types

load_dotenv()

PDF_PATH = Path(
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/"
    "LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

PAGES  = [2, 4]
DPI    = 150
MODEL  = "gemini-2.5-flash"
API_KEY = os.getenv("GEMINI_API_KEY")

PROMPT = """
Tu es un expert en lecture de formulaires CERFA d'urbanisme.

On te fournit 2 images :
- Image 1 : partie "Références cadastrales" (page 2 du CERFA)
- Image 2 : page annexe avec des parcelles complémentaires

À partir de ces deux images, extrait TOUTES les parcelles cadastrales visibles.

Tu dois retourner STRICTEMENT un JSON valide de la forme :

{
  "references_cadastrales": [
    {"section": "AC", "numero": "0494", "surface_m2": 5755},
    {"section": "AK", "numero": "0058", "surface_m2": 256},
    {"section": "AM", "numero": "0311", "surface_m2": null}
  ],
  "superficie_totale_m2": 9520
}

Règles :
- "section" : 1 ou 2 lettres majuscules (ex : "AC", "AN", "ZA")
- "numero" : numéro cadastral tel qu'il apparaît (souvent 3 ou 4 chiffres, ex : "0221")
- "surface_m2" : entier en m² si indiqué clairement pour la parcelle ; sinon null
- "references_cadastrales" doit contenir toutes les lignes de parcelles que tu vois
- "superficie_totale_m2" : valeur globale de "Superficie totale du terrain (en m²)" si elle est indiquée ; sinon null

IMPORTANT :
- Retourne UNIQUEMENT le JSON, sans texte avant ou après.
- Si une information n'est pas visible ou pas sûre, mets null.
"""


def pdf_pages_to_pil_images(pdf_path: Path, pages, dpi: int = 150):
    """Extrait les pages en images PIL"""
    images = []
    for page_num in pages:
        imgs = convert_from_path(str(pdf_path), dpi=dpi, first_page=page_num, last_page=page_num)
        images.append(imgs[0])
        print(f"✅ Page {page_num} extraite")
    return images


def pil_to_parts(images: list) -> list:
    """Convertit des images PIL en types.Part pour le nouveau SDK."""
    parts = []
    for img in images:
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        parts.append(
            types.Part.from_bytes(data=buf.getvalue(), mime_type="image/jpeg")
        )
    return parts


def extraire_parcelles_depuis_pdf(pdf_path: str, model: str = MODEL) -> dict:
    """API utilisée par l'orchestrateur"""

    if not API_KEY:
        return {"success": False, "error": "GEMINI_API_KEY manquante"}

    pdf = Path(pdf_path)
    if not pdf.exists():
        return {"success": False, "error": f"Fichier introuvable: {pdf_path}"}

    images      = pdf_pages_to_pil_images(pdf, PAGES, dpi=DPI)
    image_parts = pil_to_parts(images)

    client   = genai.Client(api_key=API_KEY)
    contents = [PROMPT] + image_parts

    try:
        response = client.models.generate_content(model=model, contents=contents)
    except Exception as e:
        return {"success": False, "error": str(e)}

    raw   = response.text.strip()
    usage = getattr(response, "usage_metadata", None)

    # Nettoyage JSON
    if "```" in raw:
        parts_split = raw.split("```")
        for part in parts_split:
            part = part.strip()
            if part.lower().startswith("json"):
                part = part[4:].strip()
            if "{" in part:
                raw = part
                break

    try:
        data = json.loads(raw)
    except Exception as e:
        return {
            "success": False,
            "error": f"Impossible de parser la réponse: {e}",
            "raw": raw,
        }

    parcelles = data.get("references_cadastrales", []) or []
    total     = data.get("superficie_totale_m2") or 0
    somme     = sum(
        (p.get("surface_m2") or 0)
        for p in parcelles
        if p.get("surface_m2") is not None
    )

    stats = {
        "nb_parcelles":   len(parcelles),
        "somme_surfaces": somme,
        "ecart_total":    abs(somme - total) if total else None,
        "tokens":         getattr(usage, "total_token_count", 0) if usage else 0,
    }

    return {"success": True, "data": data, "stats": stats}


def main():
    result = extraire_parcelles_depuis_pdf(str(PDF_PATH), model=MODEL)

    if not result.get("success"):
        print(f"❌ Erreur: {result.get('error')}")
        if result.get("raw"):
            print(f"\nRéponse brute:\n{result['raw']}")
        return

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()