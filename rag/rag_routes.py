# backend/rag/rag_routes.py

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from rag.rag_engine import SimpleLegalRAG

router = APIRouter(prefix="/chat-urba", tags=["RAG"])

rag_engine = SimpleLegalRAG(verbose=True)


class ChatRequest(BaseModel):
    query: str
    codes: Optional[List[str]] = None
    top_k: Optional[int] = 5


class ChatResponse(BaseModel):
    response: str
    sources: list
    question: str


@router.post("", response_model=ChatResponse)
async def chat_urba(payload: ChatRequest):
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="Query vide")

    try:
        # Gérer le cas ["all"] → None pour interroger tous les codes
        codes_to_use = None
        if payload.codes and "all" not in payload.codes:
            codes_to_use = payload.codes
        
        result = rag_engine.ask(
            question=payload.query,
            codes=codes_to_use,
            top_k=payload.top_k or 5
        )
        return result

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erreur moteur RAG: {str(e)}"
        )
