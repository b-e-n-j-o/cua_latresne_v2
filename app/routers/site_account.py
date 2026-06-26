"""
Site vitrine / compte : santé API, leads, réinitialisation mot de passe Supabase.
"""

import requests
from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import BaseModel, EmailStr

from app.deps import SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL, supabase
from services.auth.current_user import get_current_user_id
from utils.email_utils import send_internal_email, send_password_reset_email

router = APIRouter(tags=["site-account"])


@router.get("/health")
async def health_check():
    """Vérifie que l'API est en ligne."""
    return {"status": "ok", "message": "Kerelia API opérationnelle 🚀"}


@router.post("/lead")
async def receive_lead(payload: dict):
    try:
        supabase.table("leads").insert({
            "profile": payload.get("profile"),
            "email": payload.get("email"),
            "commune": payload.get("commune"),
            "parcelle": payload.get("parcelle"),
            "message": payload.get("message"),
        }).execute()

        send_internal_email(payload)

        return {"status": "ok"}

    except Exception as e:
        print("❌ Erreur /lead:", e)
        raise HTTPException(status_code=500, detail="Erreur serveur")


@router.post("/auth/generate-reset-token")
def generate_reset_token(email: str = Body(..., embed=True)):
    """Génère un lien de réinitialisation Supabase (recovery)."""
    url = f"{SUPABASE_URL}/auth/v1/admin/generate_link"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "type": "recovery",
        "email": email,
    }

    r = requests.post(url, headers=headers, json=payload)
    data = r.json()

    if "action_link" not in data:
        print("❌ Erreur generate_link :", data)
        raise HTTPException(500, "Impossible de générer un token Supabase")

    return {"reset_url": data["action_link"]}


class PasswordResetRequest(BaseModel):
    email: EmailStr


@router.post("/auth/send-password-reset")
def send_password_reset(req: PasswordResetRequest):
    """Email de réinitialisation custom Kerelia (lien Supabase + SendGrid)."""
    email = req.email

    url = f"{SUPABASE_URL}/auth/v1/admin/generate_link"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "type": "recovery",
        "email": email,
        "redirect_to": "https://kerelia.fr/update-password",
    }

    r = requests.post(url, headers=headers, json=payload)
    data = r.json()

    if "action_link" not in data:
        print("❌ Erreur generate_link :", data)
        raise HTTPException(500, "Impossible de générer un lien de réinitialisation")

    reset_url = data["action_link"]

    try:
        send_password_reset_email(email, reset_url)
    except Exception as e:
        print("❌ Erreur envoi email custom :", e)
        raise HTTPException(status_code=500, detail="Erreur durant l'envoi email")

    return {"success": True}


@router.get("/account/commune-access")
async def get_account_commune_access(user_id: str = Depends(get_current_user_id)):
    """
    Communes portail autorisées pour l'utilisateur authentifié (garde de routes front).
    Même logique que services.auth.commune_access (table + metadata legacy).
    """
    from services.auth.commune_access import (
        get_authorized_commune_slugs,
        get_authorized_insee_codes,
        is_superadmin,
    )

    slugs = get_authorized_commune_slugs(user_id)
    insee_codes = get_authorized_insee_codes(user_id)

    return {
        "success": True,
        "unrestricted": slugs is None,
        "is_superadmin": is_superadmin(user_id),
        "allowed_commune_slugs": slugs,
        "allowed_insee_codes": insee_codes,
    }
