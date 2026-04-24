from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional

from rag.rag_engine_plu import PLURAGEngine

router = APIRouter(prefix="/rag-plu", tags=["RAG-PLU"])

plu_rag = PLURAGEngine(verbose=True)


class PLUChatRequest(BaseModel):
    query: str
    insee: str                      # ✅ obligatoire
    document: Optional[str] = None  # ✅ optionnel (None => tous docs)
    top_k: Optional[int] = 10


class PLUChatResponse(BaseModel):
    question: str
    response: str
    sources: list


@router.post("", response_model=PLUChatResponse)
async def chat_plu(payload: PLUChatRequest):
    if not payload.query.strip():
        raise HTTPException(status_code=400, detail="Question vide")

    if not payload.insee or not payload.insee.strip():
        raise HTTPException(status_code=400, detail="INSEE requis pour interroger un PLU")

    try:
        return plu_rag.ask(
            question=payload.query,
            insee=payload.insee,
            document=payload.document,   # None => tous docs
            top_k=payload.top_k or 10
        )

    except Exception as e:
        print("❌ PLU RAG ERROR:", repr(e))
        raise HTTPException(
            status_code=500,
            detail=f"Erreur moteur RAG PLU : {str(e)}"
        )
