# backend/routes/chat.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import PyPDF2
from io import BytesIO
from typing import List, Dict


import os
from dotenv import load_dotenv

load_dotenv()

from utils.llm_openai import call_gpt_5
from api.plu.fetch_plu import get_plu_code

from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

router = APIRouter(prefix="/api/plu", tags=["plu-chat"])

class ChatPLURequest(BaseModel):
    insee: str
    zone: str
    question: str
    conversation_history: List[Dict[str, str]] = []

@router.post("/chat")
async def chat_with_plu(request: ChatPLURequest):
    try:
        # 1. Récupérer le code PLU
        plu_code = get_plu_code(request.insee)["code"]
        pdf_path = f"reglements/{plu_code}/{request.zone}.pdf"
        
        # 2. Télécharger depuis Supabase Storage
        pdf_bytes = supabase.storage.from_("plu-reglements-cached").download(pdf_path)
        
        # 3. Extraire le texte
        pdf_text = extract_text_from_pdf(pdf_bytes)
        
        # 4. Construire le contexte avec historique
        history_text = ""
        if request.conversation_history:
            history_text = "\n\n=== HISTORIQUE DE CONVERSATION ===\n"
            for msg in request.conversation_history:
                role_label = "Utilisateur" if msg["role"] == "user" else "Assistant"
                history_text += f"{role_label}: {msg['content']}\n"
        
        # 5. System prompt avec règlement
        system_prompt = f"""Tu es un assistant spécialisé en urbanisme français.
Tu réponds aux questions sur le règlement du PLU/PLUI de la zone {request.zone}.

RÈGLEMENT COMPLET DE LA ZONE {request.zone}:
{pdf_text}

Réponds de manière précise en citant les articles pertinents (ex: "Article UP 11.1.1").
Utilise un langage clair et accessible.{history_text}"""
        
        # 6. Appel GPT-5 Nano
        answer = call_gpt_5(
            model="gpt-5-nano",
            system_prompt=system_prompt,
            user_prompt=request.question,
            reasoning_effort="low",  # Nano = rapide
            max_output_tokens=1500
        )
        
        return {
            "answer": answer,
            "zone": request.zone,
            "commune": request.insee
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extrait le texte d'un PDF"""
    pdf_file = BytesIO(pdf_bytes)
    reader = PyPDF2.PdfReader(pdf_file)
    
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n\n"
    
    return text.strip()