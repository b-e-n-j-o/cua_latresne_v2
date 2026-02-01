#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline simple :
- part du PDF CERFA complet
- extrait les pages 2 et 4
- envoie ces deux images à Mistral Vision
- affiche le JSON structuré des parcelles
"""

import os
import base64
import json
from pathlib import Path

from dotenv import load_dotenv
from pdf2image import convert_from_path
from mistralai import Mistral

load_dotenv()

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

PDF_PATH = Path(
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/"
    "LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

PAGES = [2, 4]
DPI = 300

MODEL = "ministral-8b-2512"
API_KEY = os.getenv("MISTRAL_API_KEY")

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


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def pdf_pages_to_images_b64(pdf_path: Path, pages, dpi: int = 300):
    """Extrait les pages demandées en PNG et renvoie leur contenu base64."""
    images_b64 = []

    for page_num in pages:
        imgs = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            first_page=page_num,
            last_page=page_num,
        )
        img = imgs[0]

        out_path = Path(f"cerfa_page_{page_num}.png")
        img.save(out_path, "PNG")

        with open(out_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        images_b64.append(b64)
        print(f"✅ Page {page_num} extraite → {out_path}")

    return images_b64


# -------------------------------------------------------------------
# API réutilisable par l'orchestrateur
# -------------------------------------------------------------------


def extraire_parcelles_depuis_pdf(pdf_path: str, model: str = MODEL) -> dict:
    """
    API simple utilisée par l'orchestrateur :
    - part du chemin PDF
    - extrait les pages 2 et 4
    - appelle Mistral
    - retourne un dict compatible avec l'orchestrateur :
      {
        "success": bool,
        "data": <JSON LLM>,
        "stats": {
            "nb_parcelles": int,
            "somme_surfaces": int,
            "ecart_total": int | None,
            "tokens": int
        }
      }
    """
    if not API_KEY:
        return {"success": False, "error": "MISTRAL_API_KEY manquante dans l'environnement"}

    pdf = Path(pdf_path)
    if not pdf.exists():
        return {"success": False, "error": f"Fichier introuvable: {pdf_path}"}

    # 1) PDF → images (pages 2 et 4) → base64
    images_b64 = pdf_pages_to_images_b64(pdf, PAGES, dpi=DPI)

    # 2) Appel Mistral
    client = Mistral(api_key=API_KEY)

    content = [{"type": "text", "text": PROMPT}]
    for b64 in images_b64:
        content.append(
            {
                "type": "image_url",
                "image_url": f"data:image/png;base64,{b64}",
            }
        )

    response = client.chat.complete(
        model=model,
        messages=[{"role": "user", "content": content}],
        max_tokens=1500,
        temperature=0.0,
    )

    raw = response.choices[0].message.content.strip()
    usage = response.usage

    # 3) Nettoyage des éventuelles balises ```json ... ``` puis parsing du JSON
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            part = part.strip()
            if not part:
                continue
            # retirer un éventuel préfixe "json"
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
            "error": f"Impossible de parser la réponse LLM: {e}",
            "raw": raw,
        }

    # 4) Stats sur les parcelles
    parcelles = data.get("references_cadastrales", []) or []
    total = data.get("superficie_totale_m2") or 0
    somme = sum(
        (p.get("surface_m2") or 0)
        for p in parcelles
        if p.get("surface_m2") is not None
    )

    stats = {
        "nb_parcelles": len(parcelles),
        "somme_surfaces": somme,
        "ecart_total": abs(somme - total) if total else None,
        "tokens": usage.total_tokens,
    }

    return {
        "success": True,
        "data": data,
        "stats": stats,
    }


# -------------------------------------------------------------------
# MAIN (utilisation en script autonome)
# -------------------------------------------------------------------

def main():
    """Entrée CLI : réutilise la même API mais sur le PDF de test fixe."""
    result = extraire_parcelles_depuis_pdf(str(PDF_PATH), model=MODEL)

    if not result.get("success"):
        print(f"❌ Erreur: {result.get('error')}")
        raw = result.get("raw")
        if raw:
            print("\nRéponse brute LLM:\n")
            print(raw)
        return

    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()