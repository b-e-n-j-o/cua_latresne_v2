#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Benchmark Mistral API - Détection des limites de rate limiting
================================================================
Test progressif pour identifier :
- TPM (Tokens Per Minute) max
- RPM (Requests Per Minute) max
- Payload max par requête
"""

import os
import time
import base64
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv
from mistralai import Mistral
from pdf2image import convert_from_path

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("benchmark")

# ============================================================
# CONFIG
# ============================================================

IMAGE_PATH = (
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/"
    "cua_latresne_v4/__temp_page_0.png"
)

PDF_PATH = (
    "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/"
    "cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
)

MODEL = "ministral-8b-2512"

PROMPT_BASE = "Décris brièvement cette image du formulaire CERFA."

# ============================================================
# DATACLASSES
# ============================================================

@dataclass
class TestResult:
    """Résultat d'un test"""
    test_name: str
    success: bool
    nb_images: int
    dpi: int
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    duration_s: float
    payload_mb: float
    error: Optional[str] = None
    is_rate_limited: bool = False


# ============================================================
# HELPERS
# ============================================================

def b64_image(path: str) -> str:
    """Encode image en base64"""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def pdf_page_to_b64(pdf_path: str, page: int, dpi: int = 250) -> str:
    """Convertit une page PDF en base64"""
    images = convert_from_path(pdf_path, dpi=dpi, first_page=page, last_page=page)
    tmp = f"/tmp/bench_page_{page}.png"
    images[0].save(tmp, "PNG")
    
    with open(tmp, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("utf-8")
    
    os.remove(tmp)
    return b64


def is_rate_limited(error: Exception) -> bool:
    """Détecte un rate limit"""
    s = str(error).lower()
    return ("429" in s or "rate limit" in s or "rate_limited" in s or 
            "too many requests" in s)


def calculate_payload_size(images_b64: List[str]) -> float:
    """Calcule taille payload en MB"""
    return sum(len(img) for img in images_b64) / (1024 * 1024)


# ============================================================
# TESTS
# ============================================================

def test_single_request(
    client: Mistral,
    nb_images: int,
    dpi: int,
    source: str = "image"  # "image" ou "pdf"
) -> TestResult:
    """
    Test avec N images à X DPI
    
    Args:
        client: Client Mistral
        nb_images: Nombre d'images à envoyer
        dpi: Résolution
        source: "image" (répéter même image) ou "pdf" (pages différentes)
    """
    test_name = f"{nb_images}img_{dpi}dpi_{source}"
    logger.info(f"🧪 Test: {test_name}")
    
    try:
        # Préparation images
        images_b64 = []
        
        if source == "image":
            # Répéter la même image
            img = b64_image(IMAGE_PATH)
            images_b64 = [img] * nb_images
        else:
            # Pages différentes du PDF
            for page in range(1, min(nb_images + 1, 5)):  # Max 4 pages
                images_b64.append(pdf_page_to_b64(PDF_PATH, page, dpi))
        
        payload_mb = calculate_payload_size(images_b64)
        
        # Construction message
        content = [{"type": "text", "text": PROMPT_BASE}]
        for img_b64 in images_b64:
            content.append({
                "type": "image_url",
                "image_url": f"data:image/png;base64,{img_b64}"
            })
        
        # Appel API
        t_start = time.time()
        response = client.chat.complete(
            model=MODEL,
            messages=[{"role": "user", "content": content}],
            temperature=0.0,
            max_tokens=500,
        )
        duration = time.time() - t_start
        
        usage = response.usage
        
        result = TestResult(
            test_name=test_name,
            success=True,
            nb_images=nb_images,
            dpi=dpi,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
            duration_s=duration,
            payload_mb=payload_mb,
        )
        
        logger.info(f"   ✅ {usage.total_tokens} tokens | {payload_mb:.2f} MB | {duration:.2f}s")
        return result
        
    except Exception as e:
        is_rl = is_rate_limited(e)
        logger.error(f"   ❌ {'Rate limited' if is_rl else type(e).__name__}")
        
        return TestResult(
            test_name=test_name,
            success=False,
            nb_images=nb_images,
            dpi=dpi,
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=0,
            duration_s=0,
            payload_mb=calculate_payload_size(images_b64) if images_b64 else 0,
            error=str(e)[:200],
            is_rate_limited=is_rl,
        )


def test_burst_requests(
    client: Mistral,
    nb_requests: int,
    spacing_s: float = 0.0,
) -> List[TestResult]:
    """
    Test de N requêtes successives
    
    Args:
        client: Client Mistral
        nb_requests: Nombre de requêtes
        spacing_s: Délai entre requêtes (0 = burst)
    """
    logger.info(f"\n🔥 Burst test: {nb_requests} requêtes (spacing={spacing_s}s)")
    
    results = []
    for i in range(nb_requests):
        logger.info(f"   Requête {i+1}/{nb_requests}")
        
        result = test_single_request(client, nb_images=1, dpi=250, source="image")
        results.append(result)
        
        if not result.success and result.is_rate_limited:
            logger.warning(f"   ⚠️ Rate limited à la requête {i+1}")
            break
        
        if i < nb_requests - 1 and spacing_s > 0:
            time.sleep(spacing_s)
    
    success_count = sum(1 for r in results if r.success)
    logger.info(f"   Succès: {success_count}/{len(results)}")
    
    return results


# ============================================================
# BENCHMARK PRINCIPAL
# ============================================================

def run_benchmark():
    """Execute tous les tests"""
    
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise ValueError("MISTRAL_API_KEY manquante")

    print(f"API key: {api_key}")
    
    if not Path(IMAGE_PATH).exists():
        raise FileNotFoundError(f"Image test introuvable: {IMAGE_PATH}")
    
    client = Mistral(api_key=api_key)
    all_results = []
    
    print("\n" + "="*70)
    print("🎯 BENCHMARK MISTRAL API - RATE LIMITS")
    print("="*70)
    
    # ============================================================
    # TEST 1: Progression du nombre d'images (DPI fixe)
    # ============================================================
    print("\n📊 TEST 1: Impact du nombre d'images (250 DPI)")
    print("-"*70)
    
    for nb_img in [1, 2, 4, 6, 8]:
        result = test_single_request(client, nb_img, dpi=250, source="image")
        all_results.append(result)
        
        if not result.success and result.is_rate_limited:
            logger.warning(f"⚠️ Limite atteinte à {nb_img} images")
            break
        
        time.sleep(2)  # Pause entre tests
    
    # ============================================================
    # TEST 2: Impact de la résolution DPI (images fixes)
    # ============================================================
    print("\n📊 TEST 2: Impact de la résolution DPI (4 images)")
    print("-"*70)
    
    for dpi in [150, 200, 250, 300, 350]:
        result = test_single_request(client, nb_images=4, dpi=dpi, source="pdf")
        all_results.append(result)
        
        if not result.success and result.is_rate_limited:
            logger.warning(f"⚠️ Limite atteinte à {dpi} DPI")
            break
        
        time.sleep(2)
    
    # ============================================================
    # TEST 3: Burst - requêtes successives sans délai
    # ============================================================
    print("\n📊 TEST 3: Burst - 5 requêtes coup sur coup")
    print("-"*70)
    
    burst_results = test_burst_requests(client, nb_requests=5, spacing_s=0)
    all_results.extend(burst_results)
    
    time.sleep(5)  # Pause avant test suivant
    
    # ============================================================
    # TEST 4: Burst avec espacement
    # ============================================================
    print("\n📊 TEST 4: 5 requêtes espacées de 3s")
    print("-"*70)
    
    spaced_results = test_burst_requests(client, nb_requests=5, spacing_s=3)
    all_results.extend(spaced_results)
    
    # ============================================================
    # ANALYSE DES RÉSULTATS
    # ============================================================
    print("\n" + "="*70)
    print("📈 ANALYSE DES RÉSULTATS")
    print("="*70)
    
    successful = [r for r in all_results if r.success]
    rate_limited = [r for r in all_results if r.is_rate_limited]
    
    if successful:
        max_tokens = max(r.total_tokens for r in successful)
        max_payload = max(r.payload_mb for r in successful)
        avg_tokens = sum(r.total_tokens for r in successful) / len(successful)
        
        print(f"\n✅ Requêtes réussies: {len(successful)}/{len(all_results)}")
        print(f"   • Tokens max:     {max_tokens:,}")
        print(f"   • Tokens moyen:   {avg_tokens:,.0f}")
        print(f"   • Payload max:    {max_payload:.2f} MB")
    
    if rate_limited:
        first_rl = rate_limited[0]
        print(f"\n⚠️ Premier rate limit:")
        print(f"   • Test:    {first_rl.test_name}")
        print(f"   • Images:  {first_rl.nb_images}")
        print(f"   • DPI:     {first_rl.dpi}")
        print(f"   • Payload: {first_rl.payload_mb:.2f} MB")
    
    # TPM estimé sur 60s
    if successful and len(successful) > 1:
        total_time = sum(r.duration_s for r in successful)
        total_tokens = sum(r.total_tokens for r in successful)
        
        if total_time > 0:
            tpm_estimate = (total_tokens / total_time) * 60
            print(f"\n📊 Estimation TPM actuel:")
            print(f"   • {tpm_estimate:,.0f} tokens/minute")
            print(f"   • Basé sur {len(successful)} requêtes en {total_time:.1f}s")
    
    # Recommandations
    print(f"\n💡 RECOMMANDATIONS:")
    
    if rate_limited:
        safe_tokens = max(r.total_tokens for r in successful) if successful else 0
        print(f"   • Limiter à ~{safe_tokens:,} tokens par requête")
        print(f"   • Espacer les requêtes de 3-5 secondes minimum")
        print(f"   • Utiliser retry avec backoff exponentiel")
    else:
        print(f"   • Aucune limite atteinte lors des tests")
        print(f"   • Vous pouvez augmenter progressivement")
    
    print("\n" + "="*70)
    
    # Sauvegarde résultats
    import json
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "model": MODEL,
        "results": [
            {
                "test": r.test_name,
                "success": r.success,
                "nb_images": r.nb_images,
                "dpi": r.dpi,
                "tokens": r.total_tokens,
                "payload_mb": round(r.payload_mb, 2),
                "duration_s": round(r.duration_s, 2),
                "rate_limited": r.is_rate_limited,
            }
            for r in all_results
        ]
    }
    
    with open("benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print("💾 Résultats sauvegardés: benchmark_results.json\n")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    try:
        run_benchmark()
    except KeyboardInterrupt:
        print("\n\n⚠️ Benchmark interrompu par l'utilisateur")
    except Exception as e:
        logger.exception("Erreur critique")