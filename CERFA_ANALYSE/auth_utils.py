# auth_utils.py
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# 🔍 FONCTIONS
# ============================================================

def get_user_insee_list(user_id: str) -> list[str]:
    """
    Récupère la liste des codes INSEE autorisés pour un utilisateur.
    Si le champ 'insee' est vide ou absent → accès à toutes les communes.
    """
    try:
        user = supabase.auth.admin.get_user_by_id(user_id)
        meta = user.user.user_metadata or {}

        insee_field = meta.get("insee")

        # Cas 1 : champ unique ex: "33234"
        if isinstance(insee_field, str):
            return [insee_field]

        # Cas 2 : liste de codes ex: ["33234", "33531"]
        if isinstance(insee_field, list):
            return insee_field

        # Cas 3 : pas de champ du tout => pas de restriction
        return []
    except Exception as e:
        print(f"[auth_utils] ⚠️ Erreur récupération métadonnées INSEE: {e}")
        return []


def is_authorized_for_insee(user_id: str, commune_insee: str) -> bool:
    """
    Vérifie si l'utilisateur est autorisé à analyser la commune donnée.
    - Si la liste est vide → accès libre
    - Sinon, la commune doit être dans la liste
    """
    allowed_insee = get_user_insee_list(user_id)
    if not allowed_insee:  # aucune restriction
        return True

    return commune_insee in allowed_insee
