#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Audit des limites Mistral Vision avec différents DPI
"""

import os
import base64
import json
import logging
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
from pdf2image import convert_from_path
from PIL import Image
from mistralai import Mistral

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'mistral_vision_audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

PDF_PATH = Path(
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/"
    "LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

PAGES = [2, 4]
MODEL = "mistral-small-2506"  # Modèle vision de Mistral
API_KEY = os.getenv("MISTRAL_API_KEY")

# Test avec différents DPI
DPI_TESTS = [100, 150, 200, 250, 300]

PROMPT = "Décris brièvement ce que tu vois sur ces images."


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def pdf_pages_to_images_b64(pdf_path: Path, pages, dpi: int = 150, max_width: int = 2000):
    """Extrait et optimise les pages."""
    images_b64 = []
    total_size = 0

    for page_num in pages:
        imgs = convert_from_path(
            str(pdf_path),
            dpi=dpi,
            first_page=page_num,
            last_page=page_num,
        )
        img = imgs[0]
        
        # Redimensionner si trop large
        original_size = img.size
        if img.width > max_width:
            ratio = max_width / img.width
            new_size = (max_width, int(img.height * ratio))
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        
        # Sauver en JPEG optimisé
        out_path = Path(f"temp_page_{page_num}_dpi{dpi}.jpg")
        img.save(out_path, "JPEG", quality=85, optimize=True)
        
        file_size = out_path.stat().st_size
        total_size += file_size

        with open(out_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        images_b64.append(b64)
        
        logger.info(
            f"  Page {page_num}: {original_size[0]}x{original_size[1]}px → "
            f"{img.size[0]}x{img.size[1]}px | "
            f"Fichier: {file_size/1024:.1f}KB | "
            f"Base64: {len(b64):,} chars"
        )
        
        # Nettoyage
        out_path.unlink()

    logger.info(f"  📦 Taille totale: {total_size/1024:.1f}KB | Base64 total: {sum(len(b) for b in images_b64):,} chars")
    return images_b64, total_size


def test_dpi(dpi: int):
    """Test un appel Mistral Vision avec un DPI donné."""
    logger.info("=" * 80)
    logger.info(f"🔍 TEST DPI = {dpi}")
    logger.info("=" * 80)
    
    try:
        # 1) Extraction images
        logger.info(f"📄 Extraction des pages {PAGES} à {dpi} DPI...")
        images_b64, total_size = pdf_pages_to_images_b64(PDF_PATH, PAGES, dpi=dpi)
        
        # 2) Préparation du contenu
        content = [{"type": "text", "text": PROMPT}]
        for idx, b64 in enumerate(images_b64):
            content.append({
                "type": "image_url",
                "image_url": f"data:image/jpeg;base64,{b64}",
            })
        
        # 3) Appel API
        client = Mistral(api_key=API_KEY)
        logger.info(f"📤 Envoi à {MODEL}...")
        
        start_time = datetime.now()
        response = client.chat.complete(
            model=MODEL,
            messages=[{"role": "user", "content": content}],
            max_tokens=500,
            temperature=0.0,
        )
        duration = (datetime.now() - start_time).total_seconds()
        
        # 4) Analyse de la réponse
        usage = response.usage
        logger.info("=" * 80)
        logger.info("✅ SUCCÈS")
        logger.info("=" * 80)
        logger.info(f"⏱️  Durée: {duration:.2f}s")
        logger.info(f"💰 Tokens:")
        logger.info(f"   - Prompt tokens: {usage.prompt_tokens:,}")
        logger.info(f"   - Completion tokens: {usage.completion_tokens:,}")
        logger.info(f"   - Total tokens: {usage.total_tokens:,}")
        logger.info(f"📊 Ratio: {usage.prompt_tokens / (total_size/1024):.0f} tokens/KB")
        logger.info(f"📝 Réponse (extrait): {response.choices[0].message.content[:200]}...")
        
        return {
            "success": True,
            "dpi": dpi,
            "file_size_kb": total_size / 1024,
            "base64_chars": sum(len(b) for b in images_b64),
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
            "duration": duration,
        }
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ ERREUR")
        logger.error("=" * 80)
        logger.error(f"Type: {type(e).__name__}")
        logger.error(f"Message: {str(e)}")
        
        if hasattr(e, 'status_code'):
            logger.error(f"Status code: {e.status_code}")
        if hasattr(e, 'response'):
            logger.error(f"Response: {e.response}")
        
        logger.exception("Stack trace:")
        
        return {
            "success": False,
            "dpi": dpi,
            "error": str(e),
            "error_type": type(e).__name__,
        }


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    print(f"API key: {API_KEY}")
    logger.info("🚀 Audit Mistral Vision - Tests DPI")
    logger.info(f"📄 PDF: {PDF_PATH.name}")
    logger.info(f"📋 Pages: {PAGES}")
    logger.info(f"🤖 Modèle: {MODEL}")
    logger.info("")
    
    if not API_KEY:
        logger.error("❌ MISTRAL_API_KEY manquante")
        return
    
    if not PDF_PATH.exists():
        logger.error(f"❌ PDF introuvable: {PDF_PATH}")
        return
    
    results = []
    
    for dpi in DPI_TESTS:
        result = test_dpi(dpi)
        results.append(result)
        logger.info("")
    
    # Résumé
    logger.info("=" * 80)
    logger.info("📊 RÉSUMÉ DES TESTS")
    logger.info("=" * 80)
    logger.info(f"{'DPI':<6} {'Taille':<12} {'Prompt tokens':<15} {'Status':<10}")
    logger.info("-" * 80)
    
    for r in results:
        if r["success"]:
            logger.info(
                f"{r['dpi']:<6} "
                f"{r['file_size_kb']:>8.1f} KB   "
                f"{r['prompt_tokens']:>12,}   "
                f"✅ OK"
            )
        else:
            logger.info(
                f"{r['dpi']:<6} "
                f"{'N/A':<12} "
                f"{'N/A':<15} "
                f"❌ {r['error_type']}"
            )
    
    logger.info("=" * 80)
    
    # Sauvegarde JSON
    output_file = f"audit_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"💾 Résultats sauvegardés: {output_file}")


if __name__ == "__main__":
    main()