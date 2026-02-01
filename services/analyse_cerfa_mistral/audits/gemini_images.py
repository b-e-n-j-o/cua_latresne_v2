#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Audit Gemini Vision - 2 images du PDF CERFA"""

import os
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pdf2image import convert_from_path
import google.generativeai as genai

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PDF_PATH = Path(
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/"
    "LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = "gemini-3-pro-preview"
PAGES = [2, 4]
DPI_TESTS = [250]

PROMPT = """Analyse ces deux pages de CERFA et extrait les parcelles cadastrales.
Retourne un JSON avec : section, numéro, surface_m2 pour chaque parcelle."""


def extract_pages(pdf_path: Path, pages, dpi: int = 150):
    """Extrait plusieurs pages."""
    images = []
    total_size = 0
    
    for page_num in pages:
        imgs = convert_from_path(str(pdf_path), dpi=dpi, first_page=page_num, last_page=page_num)
        img = imgs[0]
        
        temp = Path(f"temp_p{page_num}.jpg")
        img.save(temp, "JPEG", quality=85, optimize=True)
        size = temp.stat().st_size
        total_size += size
        
        logger.info(f"  Page {page_num}: {img.size[0]}x{img.size[1]}px | {size/1024:.1f}KB")
        
        images.append(img)
        temp.unlink()
    
    logger.info(f"  📦 Total: {total_size/1024:.1f}KB")
    return images, total_size


def test_dpi(dpi: int):
    logger.info("=" * 80)
    logger.info(f"🔍 TEST DPI = {dpi}")
    logger.info("=" * 80)
    
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel(MODEL)
        
        logger.info(f"📄 Extraction pages {PAGES} à {dpi} DPI...")
        images, total_size = extract_pages(PDF_PATH, PAGES, dpi)
        
        logger.info("📤 Envoi à Gemini...")
        start = datetime.now()
        
        response = model.generate_content([PROMPT] + images)
        
        duration = (datetime.now() - start).total_seconds()
        
        logger.info("✅ SUCCÈS")
        logger.info(f"⏱️  Durée: {duration:.2f}s")
        
        if hasattr(response, 'usage_metadata'):
            usage = response.usage_metadata
            logger.info(f"💰 Tokens: prompt={usage.prompt_token_count:,}, "
                       f"output={usage.candidates_token_count:,}, "
                       f"total={usage.total_token_count:,}")
            logger.info(f"📊 Ratio: {usage.prompt_token_count / (total_size/1024):.0f} tokens/KB")
        
        logger.info(f"📝 Réponse: {response.text[:400]}...")
        
        return {
            "success": True,
            "dpi": dpi,
            "file_size_kb": total_size / 1024,
            "prompt_tokens": usage.prompt_token_count if hasattr(response, 'usage_metadata') else None,
            "duration": duration
        }
        
    except Exception as e:
        logger.error("❌ ERREUR")
        logger.error(f"Type: {type(e).__name__}")
        logger.error(f"Message: {str(e)}")
        return {"success": False, "dpi": dpi, "error": str(e)}


def main():
    logger.info("🚀 Audit Gemini Vision - 2 images du PDF")
    logger.info(f"📄 PDF: {PDF_PATH.name}")
    logger.info(f"📋 Pages: {PAGES}")
    logger.info(f"🤖 Modèle: {MODEL}")
    logger.info("")
    
    if not API_KEY or not PDF_PATH.exists():
        logger.error("❌ Config invalide")
        return
    
    results = []
    for dpi in DPI_TESTS:
        result = test_dpi(dpi)
        results.append(result)
        logger.info("")
    
    logger.info("=" * 80)
    logger.info("📊 RÉSUMÉ")
    logger.info("=" * 80)
    logger.info(f"{'DPI':<6} {'Taille':<12} {'Prompt tokens':<15} {'Status':<10}")
    logger.info("-" * 80)
    
    for r in results:
        if r["success"]:
            tokens = f"{r['prompt_tokens']:,}" if r.get('prompt_tokens') else "N/A"
            logger.info(f"{r['dpi']:<6} {r['file_size_kb']:>8.1f} KB   {tokens:>12}   ✅ OK")
        else:
            logger.info(f"{r['dpi']:<6} {'N/A':<12} {'N/A':<15} ❌ {r.get('error_type', 'Error')}")


if __name__ == "__main__":
    main()
