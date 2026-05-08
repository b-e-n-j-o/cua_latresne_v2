"""
Test complet de mistral_utils avec logging
"""

import logging
from mistral_utils import MistralAnalyzer
from dotenv import load_dotenv
import os
load_dotenv()
logger = logging.getLogger("mistral_utils")

API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise ValueError("⚠️ Tu dois définir MISTRAL_API_KEY dans ton environnement export MISTRAL_API_KEY='xxx'")

logger.info("🔐 Clé API chargée.")

# Config logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_analyze_text(analyzer):
    """Test analyse texte"""
    logger.info("=== TEST 1: Analyse texte ===")
    
    result = analyzer.analyze_text(
        prompt="Explique l'OCR vs Vision en 2 phrases",
        model="ministral-3b-2512"
    )
    
    logger.info(f"Modèle: {result['model']}")
    logger.info(f"Tokens: {result['tokens']}")
    logger.info(f"Réponse: {result['content'][:100]}...")
    
    return result


def test_analyze_pdf_ocr(analyzer, pdf_path):
    """Test analyse PDF via OCR"""
    logger.info("\n=== TEST 2: Analyse PDF OCR ===")
    
    result = analyzer.analyze_pdf_ocr(
        pdf_path=pdf_path,
        prompt="Quelle est la commune concernée par ce document?",
        model="ministral-8b-2512"
    )
    
    logger.info(f"Modèle: {result['model']}")
    logger.info(f"Pages OCR: {result['ocr_pages']}")
    logger.info(f"Tokens: {result['tokens']}")
    logger.info(f"Réponse: {result['content'][:150]}...")
    
    return result


def test_analyze_pdf_vision(analyzer, pdf_path):
    """Test analyse PDF via Vision"""
    logger.info("\n=== TEST 3: Analyse PDF Vision ===")
    
    result = analyzer.analyze_pdf_vision(
        pdf_path=pdf_path,
        prompt="Extrais les références cadastrales section et numéro uniquement",
        pages=[2, 4],
        model="ministral-14b-2512",
        dpi=300
    )
    
    logger.info(f"Modèle: {result['model']}")
    logger.info(f"Images: {result['images_processed']}")
    logger.info(f"Tokens: {result['tokens']}")
    logger.info(f"Réponse: {result['content'][:200]}...")
    
    return result


def main():
    pdf_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    logger.info("Initialisation MistralAnalyzer...")
    analyzer = MistralAnalyzer()
    
    try:
        # Tests
        r1 = test_analyze_text(analyzer)
        r2 = test_analyze_pdf_ocr(analyzer, pdf_path)
        r3 = test_analyze_pdf_vision(analyzer, pdf_path)
        
        # Résumé
        logger.info("\n=== RÉSUMÉ ===")
        logger.info(f"Total tokens: {r1['tokens'] + r2['tokens'] + r3['tokens']}")
        logger.info("✅ Tous les tests réussis")
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}", exc_info=True)


if __name__ == "__main__":
    main()