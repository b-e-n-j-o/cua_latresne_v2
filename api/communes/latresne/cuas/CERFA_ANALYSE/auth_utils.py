# auth_utils.py — Compatibilité ascendante (pipeline CERFA Latresne).
# Source de vérité : services.auth.commune_access

from services.auth.commune_access import (
    get_authorized_insee_codes,
    is_authorized_for_insee,
)


def get_user_insee_list(user_id: str) -> list[str]:
    """
    Liste des codes INSEE autorisés.
    Liste vide → pas de restriction (toutes les communes).
    """
    allowed = get_authorized_insee_codes(user_id)
    return allowed or []


__all__ = ["get_user_insee_list", "is_authorized_for_insee", "get_authorized_insee_codes"]
