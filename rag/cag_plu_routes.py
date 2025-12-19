# -*- coding: utf-8 -*-

import json
import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Dict, Any

from rag.cag_plu_engine import (
    run_cag_plu,
    detect_zonage_from_question,
    fetch_plu_articles,
    build_reglement_text,
)

router = APIRouter(prefix="/cag-plu", tags=["CAG-PLU"])

logger = logging.getLogger("cag_plu")
logger.setLevel(logging.INFO)

# ============================================================
# SCHEMAS
# ============================================================

class CAGPLURequest(BaseModel):
    question: str
    commune_insee: str


class CAGPLUResponse(BaseModel):
    content: str
    sources: List[Dict[str, Any]]


# ============================================================
# ENDPOINT
# ============================================================

@router.post("", response_model=CAGPLUResponse)
def cag_plu(req: CAGPLURequest):

    # 1) Détection zonage (heuristique)
    zonage = detect_zonage_from_question(req.question)

    # 2) Fetch Supabase
    articles = fetch_plu_articles(
        insee=req.commune_insee,
        zonage=zonage,
    )

    # 3) Reconstruction du contexte
    reglement_text = build_reglement_text(articles)

    # 4) Logging métier (clé pour debug & tuning)
    logger.info(
        "CAG PLU | insee=%s | zonage_detect=%s | nb_articles=%d | context_chars=%d",
        req.commune_insee,
        zonage or "NONE",
        len(articles),
        len(reglement_text),
    )

    # Log de l'extrait du texte injecté
    excerpt = reglement_text[:300].replace("\n", " ") + "..."
    logger.info("CAG PLU | context_excerpt: %s", excerpt)

    # 5) Appel CAG (retourne une string JSON)
    result_str = run_cag_plu(
        question=req.question,
        commune_insee=req.commune_insee,
    )

    # Parse du JSON en objet Python
    result = json.loads(result_str)

    # Log de sortie (très utile)
    logger.info(
        "CAG PLU | output_chars=%d | sources=%d",
        len(result.get("content", "")),
        len(result.get("sources", [])),
    )

    return result
