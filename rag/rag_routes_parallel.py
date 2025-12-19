# -*- coding: utf-8 -*-

import json
import asyncio
import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, Any, Dict
from fastapi.concurrency import run_in_threadpool

from rag.rag_engine import SimpleLegalRAG
from rag.cag_plu_engine import run_cag_plu
from rag.rag_routes_meta import run_meta_synthesis

router = APIRouter(prefix="/chat-urba-parallel", tags=["RAG-PARALLEL"])

logger = logging.getLogger("rag_parallel")
logger.setLevel(logging.INFO)

rag_engine = SimpleLegalRAG(verbose=True)


# ============================================================
# SCHEMAS
# ============================================================

class ParallelRequest(BaseModel):
    query: str
    codes: Optional[list[str]] = None
    commune_insee: Optional[str] = None


# ============================================================
# FONCTIONS AGENTS ISOLÉES
# ============================================================

async def none_coroutine():
    """Coroutine helper qui retourne None."""
    return None


def call_rag_legal(query: str, codes: Optional[list[str]]):
    """Agent RAG juridique (synchrone, bloquant)."""
    codes_to_use = None
    if codes and "all" not in codes:
        codes_to_use = codes
    return rag_engine.ask(
        question=query,
        codes=codes_to_use,
        top_k=5
    )


def call_cag_plu(query: str, commune_insee: str):
    """Agent CAG PLU (synchrone, bloquant)."""
    result_str = run_cag_plu(
        question=query,
        commune_insee=commune_insee,
    )
    return json.loads(result_str)


# ============================================================
# ENDPOINT ORCHESTRATEUR
# ============================================================

@router.post("")
async def chat_parallel(req: ParallelRequest):

    logger.info(
        "PARALLEL | query_len=%d | codes=%s | insee=%s",
        len(req.query),
        req.codes,
        req.commune_insee,
    )

    # RAG juridique
    if req.codes:
        legal_task = run_in_threadpool(call_rag_legal, req.query, req.codes)
    else:
        legal_task = none_coroutine()

    # CAG PLU
    if req.commune_insee:
        plu_task = run_in_threadpool(call_cag_plu, req.query, req.commune_insee)
    else:
        plu_task = none_coroutine()

    legal_result, plu_result = await asyncio.gather(legal_task, plu_task)

    logger.info(
        "PARALLEL | rag=%s | cag=%s",
        "OK" if legal_result else "NONE",
        "OK" if plu_result else "NONE",
    )

    # Méta-synthèse (bloquante → threadpool)
    meta_response = await run_in_threadpool(
        run_meta_synthesis,
        req.query,
        legal_result,
        plu_result,
    )

    return {
        "question": req.query,
        "response": meta_response.get("response"),
        "sources": {
            "codes": legal_result.get("sources") if legal_result else [],
            "plu": plu_result.get("sources") if plu_result else [],
        },
        "raw": {
            "legal": legal_result,
            "plu": plu_result,
        },
    }

