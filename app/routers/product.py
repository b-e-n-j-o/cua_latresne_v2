"""
Utilitaires produit : résumé / analyse IA du CUA (Gemini).
"""

import json
import os
from io import BytesIO

import google.generativeai as genai
import mammoth
import requests
from fastapi import APIRouter
from pydantic import BaseModel

from app.deps import supabase

router = APIRouter(tags=["product"])


def get_dossier_from_slug(slug: str):
    """Récupère un pipeline (dossier CUA) depuis Supabase via son slug."""
    resp = (
        supabase.schema("latresne")
        .table("pipelines")
        .select("*")
        .eq("slug", slug)
        .limit(1)
        .execute()
    )

    rows = resp.data or []
    if not rows:
        return None
    return rows[0]


def extract_docx_text(docx_bytes: bytes) -> str:
    """Extrait le texte brut d'un DOCX (mammoth)."""
    try:
        result = mammoth.extract_raw_text(BytesIO(docx_bytes))
        return result.value
    except Exception as e:
        print(f"⚠️ Erreur extraction DOCX: {e}")
        return ""


class AISummaryRequest(BaseModel):
    slug: str


@router.post("/cua/ai_summary")
async def ai_summary(req: AISummaryRequest):
    try:
        slug = req.slug

        dossier = get_dossier_from_slug(slug)
        if not dossier:
            return {"success": False, "error": "Dossier introuvable"}

        docx_url = dossier.get("output_cua")
        intersections_json = dossier.get("intersections")

        if not docx_url:
            return {"success": False, "error": "Le CUA n'est pas encore généré"}

        data = requests.get(docx_url).content
        text = extract_docx_text(data)

        prompt = f"""
        Tu es un expert en urbanisme et en relecture de documents.
        Voici un certificat d'urbanisme généré automatiquement.

        === CONTENU DOCX ===
        {text}

        === COUCHES INTERSECTÉES ===
        {json.dumps(intersections_json, indent=2)}

        Tâches :
        1) Détecte incohérences, erreurs, duplications, typos ou défauts de génération, ou encore éléments pas cohérents ou clair avec la réglementaiton. Sois le plus exhaustif possible.
        2) Signale tout élément étrange ou potentiellement faux.
        3) Fais des propositions de modifications pour améliorer le CUA en fonction des incohérences et erreurs détectées.
        Réponds de façon structurée, concise et fiable.
        Réponds directement l'analyse, sans préambule.
        N'ecris pas de ** ou * dans la réponse.
        """

        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not gemini_api_key:
            return {"success": False, "error": "GEMINI_API_KEY manquante dans les variables d'environnement"}

        genai.configure(api_key=gemini_api_key)
        model = genai.GenerativeModel("gemini-2.5-flash-lite")
        response = model.generate_content(prompt)

        summary = response.text

        return {"success": True, "summary": summary}

    except Exception as e:
        return {"success": False, "error": str(e)}
