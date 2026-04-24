# backend/rag/rag_routes_meta.py
import os
import time
import logging
import tiktoken
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI

from utils.llm_openai import call_gpt_5

load_dotenv()

router = APIRouter(prefix="/rag-meta-synthese", tags=["RAG-META"])

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
META_MODEL = os.getenv("RAG_META_MODEL", "gpt-5-mini")

logger = logging.getLogger("rag_meta")
logger.setLevel(logging.INFO)

MAX_OUTPUT_TOKENS = 10000
REASONING_EFFORT = "low"

# Configuration tiktoken
try:
    # On utilise o200k_base pour les modèles les plus récents (GPT-4o family)
    encoding = tiktoken.get_encoding("o200k_base")
except Exception:
    # Fallback cl100k_base si o200k_base n'est pas dispo
    encoding = tiktoken.get_encoding("cl100k_base")

# Grille tarifaire (USD / 1M tokens)
PRICE_INPUT_1M = 0.25
PRICE_CACHED_INPUT_1M = 0.025
PRICE_OUTPUT_1M = 2.00


def count_tokens(text: str) -> int:
    """Compte précisément les tokens avec tiktoken."""
    if not text:
        return 0
    return len(encoding.encode(text))


class MetaSynthesisRequest(BaseModel):
    query: str
    legal_result: Optional[Dict[str, Any]] = None   # RAG Codes
    plu_cag_result: Optional[Dict[str, Any]] = None # CAG PLU


class MetaSynthesisResponse(BaseModel):
    question: str
    response: str


SYSTEM_PROMPT = """
Tu es un assistant expert en droit de l’urbanisme français.

Tu n’effectues PAS de raisonnement juridique primaire.
Tu synthétises et mets en cohérence des analyses juridiques déjà produites.

Tu reçois deux résultats distincts :
- un résultat issu des codes juridiques nationaux (droit national),
- un résultat issu du règlement du PLU analysé par CAG (droit local).

RÈGLES IMPÉRATIVES :

1) Périmètre strict
- Tu utilises EXCLUSIVEMENT les informations fournies.
- Tu n’ajoutes aucune connaissance externe, interprétation nouvelle ou règle implicite.

2) Hiérarchie normative
- Le droit local (PLU) prévaut sur le droit national lorsqu’il est plus précis.
- Le droit national sert de cadre général lorsque le PLU est silencieux.

3) Fidélité aux sources
- Tu ne reformules pas une autorisation ou une interdiction de manière plus large que les sources.
- Tu ne transformes jamais une règle encadrante en autorisation implicite.

4) Synthèse structurée
- Tu produis une réponse claire, professionnelle et exploitable par un non-juriste.
- Tu distingues explicitement :
  (A) le cadre juridique national,
  (B) les règles locales applicables (PLU).

5) Gestion des incertitudes
- Si une information est absente, ambiguë ou non précisée,
  tu écris explicitement : « Non précisé dans les sources fournies ».

6) Ton et responsabilité
- Tu n’indiques jamais qu’une action est « autorisée » sans conditions.
- Tu utilises des formulations prudentes et conditionnelles lorsque nécessaire.
- Tu n’émet pas d’avis personnel ni de décision administrative.

OBJECTIF FINAL :
Fournir à l’utilisateur une compréhension synthétique, hiérarchisée et fiable
de sa situation au regard des règles nationales et locales applicables.

"""


def build_user_prompt(
    query: str,
    legal_result: Optional[dict],
    plu_cag_result: Optional[dict],
) -> str:
    legal_text = (legal_result or {}).get("response", "")
    legal_sources = (legal_result or {}).get("sources", [])

    # Note : Le CAG PLU utilise la clé "content" pour le texte raisonné
    plu_text = (plu_cag_result or {}).get("content", "")
    plu_sources = (plu_cag_result or {}).get("sources", [])

    return f"""
QUESTION UTILISATEUR :
{query}

====================
(A) ANALYSE CODES NATIONAUX
====================
{legal_text}

SOURCES (CODES) :
{legal_sources}

====================
(B) ANALYSE PLU (LOCAL)
====================
{plu_text}

SOURCES (PLU) :
{plu_sources}

====================
CONSIGNE
====================
Rédige une réponse structurée en 4 blocs :

1) SYNTHÈSE
2) (A) CADRE NATIONAL (CODES)
3) (B) RÈGLES LOCALES (PLU)
4) LIMITES / INFOS MANQUANTES
""".strip()


def run_meta_synthesis(
    query: str,
    legal_result: Optional[dict],
    plu_cag_result: Optional[dict],
) -> Dict[str, Any]:
    """
    Fonction synchrone pour exécuter la méta-synthèse.
    À appeler via run_in_threadpool dans un contexte async.
    """
    query = query.strip()
    if not query:
        raise ValueError("Query vide")

    # Logging des clés reçues
    logger.info(
        "META-LLM | keys_received: legal_result=%s | plu_cag_result=%s",
        "YES" if legal_result else "NO",
        "YES" if plu_cag_result else "NO"
    )

    if plu_cag_result:
        content_plu = plu_cag_result.get("content", "")
        excerpt_plu = content_plu[:200].replace("\n", " ") + "..."
        logger.info("META-LLM | plu_cag_content_excerpt: %s", excerpt_plu)

    if not legal_result and not plu_cag_result:
        raise ValueError("Aucun résultat fourni (legal_result ou plu_cag_result requis)")

    user_prompt = build_user_prompt(
        query,
        legal_result,
        plu_cag_result,
    )

    t0 = time.perf_counter()

    # Comptage précis des tokens input
    input_tokens = count_tokens(SYSTEM_PROMPT + user_prompt)

    answer = call_gpt_5(
        model=META_MODEL,
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
        reasoning_effort=REASONING_EFFORT,
        max_output_tokens=MAX_OUTPUT_TOKENS,
    )

    # Comptage précis des tokens output
    output_tokens = count_tokens(answer)
    total_tokens = input_tokens + output_tokens

    t1 = time.perf_counter()

    # Calcul du coût (USD / 1M tokens)
    cost_est_usd = (input_tokens / 1_000_000 * PRICE_INPUT_1M) + \
                   (output_tokens / 1_000_000 * PRICE_OUTPUT_1M)

    logger.info(
        "META-LLM | model=%s | time=%.2fs | input_tok=%d | output_tok=%d | total_tok=%d | cost=$%.6f",
        META_MODEL,
        t1 - t0,
        input_tokens,
        output_tokens,
        total_tokens,
        cost_est_usd,
    )

    return {"question": query, "response": answer}


@router.post("", response_model=MetaSynthesisResponse)
async def meta_synthese(payload: MetaSynthesisRequest):
    try:
        result = run_meta_synthesis(
            payload.query,
            payload.legal_result,
            payload.plu_cag_result,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur méta-LLM: {str(e)}")
