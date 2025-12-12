"""
Extraction parcelles : PDF → Images → Mistral Vision
"""

import os
import base64
from pathlib import Path
from mistralai import Mistral
from dotenv import load_dotenv
from pdf2image import convert_from_path
import json

load_dotenv()
client = Mistral(api_key=os.getenv("MISTRAL_API_KEY"))

PROMPT = """Extrais TOUTES les références cadastrales de ces 2 images.

Image 1 : section 4.2 avec premières parcelles
Image 2 : page annexe avec parcelles complémentaires

JSON strict :
{
  "references_cadastrales": [
    {"section": "AC", "numero": "0494", "surface_m2": 5755}
  ],
  "superficie_totale_m2": null
}

Format parcelle :
- Section : 1-2 lettres (AI, AC, ZA)
- Numéro : 4 chiffres (0494, 0058)
- Surface : entier en m²

Retourne UNIQUEMENT le JSON."""


def pdf_to_images(pdf_path: str, pages: list[int]) -> list[str]:
    """Convertit pages PDF en images base64"""
    
    print(f"🖼️  Conversion pages {pages} en images (300 DPI)...")
    
    images_base64 = []
    
    for page_num in pages:
        # Convertir page en image haute résolution
        images = convert_from_path(
            pdf_path, 
            dpi=300,
            first_page=page_num,
            last_page=page_num
        )
        
        # Sauver temporairement
        img_path = f"/tmp/page_{page_num}.png"
        images[0].save(img_path, "PNG")
        
        # Encoder en base64
        with open(img_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
            images_base64.append(b64)
        
        os.remove(img_path)
    
    print(f"✅ {len(images_base64)} images créées")
    return images_base64


def extraire_parcelles_images(pdf_path: str) -> dict:
    """Pipeline : PDF → Images → Vision"""
    
    # 1. Convertir pages 2 et 4 en images
    images = pdf_to_images(pdf_path, [2, 4])
    
    # 2. Construire message avec les 2 images
    print("🤖 Analyse Vision (ministral-14b)...")
    
    content = [{"type": "text", "text": PROMPT}]
    
    for i, img_b64 in enumerate(images, 1):
        content.append({
            "type": "image_url",
            "image_url": f"data:image/png;base64,{img_b64}"
        })
    
    response = client.chat.complete(
        model="ministral-14b-2512",
        messages=[{"role": "user", "content": content}],
        max_tokens=2000,
        temperature=0.0
    )
    
    result = response.choices[0].message.content.strip()
    
    # Parse JSON
    if result.startswith("```"):
        result = result.split("```")[1]
        if result.startswith("json"):
            result = result[4:]
    
    data = json.loads(result.strip())
    
    print(f"✅ Extraction OK ({response.usage.total_tokens} tokens)")
    
    return data, response.usage.total_tokens


def main():
    pdf_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    data, tokens = extraire_parcelles_images(pdf_path)
    
    # Affichage
    parcelles = data["references_cadastrales"]
    total = data.get("superficie_totale_m2") or 0
    somme = sum(p.get("surface_m2") or 0 for p in parcelles)
    
    print(f"\n📐 {len(parcelles)} parcelles extraites :")
    for i, p in enumerate(parcelles, 1):
        s = p.get("surface_m2") or 0
        print(f"  {i}. {p.get('section', '?'):4s} {p.get('numero', '?'):6s} → {s:8,} m²".replace(",", " "))
    
    print(f"\nSomme: {somme:,} m²".replace(",", " "))
    if total:
        print(f"Total: {total:,} m² | Écart: {abs(somme-total):,} m²".replace(",", " "))
    
    print(f"Tokens: {tokens}")
    
    # Sauvegarde
    with open("parcelles_images.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print("\n💾 Sauvegardé : parcelles_images.json")


if __name__ == "__main__":
    main()