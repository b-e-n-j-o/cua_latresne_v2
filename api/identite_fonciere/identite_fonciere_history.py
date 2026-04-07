"""
Persistance métadonnées CIF (carte d'identité foncière) dans Supabase, liées à un utilisateur.

- Même `project_id` que le préfixe Storage (`if_*`).
- Centroïde de l'UF en WGS84 pour futurs pings carte (identique à `latresne.pipelines.centroid`).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform as shp_transform

from .identite_fonciere import _detect_input_srid

logger = logging.getLogger(__name__)

# Injecté depuis main.py (client service role)
supabase: Any = None


def geojson_centroid_wgs84(
    geometry: Dict[str, Any],
    srid_explicit: Optional[int] = None,
) -> Optional[Dict[str, float]]:
    """Centroïde {lon, lat} en EPSG:4326 à partir d'un GeoJSON d'UF."""
    if not isinstance(geometry, dict) or "type" not in geometry:
        return None
    try:
        g = shape(geometry)
        if g.is_empty:
            return None
        det = _detect_input_srid(geometry, srid_explicit)
        if det == 4326:
            c = g.centroid
            return {"lon": float(c.x), "lat": float(c.y)}
        tf = Transformer.from_crs(f"EPSG:{det}", "EPSG:4326", always_xy=True)
        g4326 = shp_transform(lambda x, y, z=None: tf.transform(x, y), g)
        c = g4326.centroid
        return {"lon": float(c.x), "lat": float(c.y)}
    except Exception as exc:
        logger.warning("CIF history : centroïde impossible : %s", exc)
        return None


def record_identite_fonciere_project(
    *,
    project_id: str,
    user_id: Optional[str],
    user_email: Optional[str],
    commune: str,
    insee: str,
    parcelle_label: Optional[str],
    parcelles_cadastrales: Optional[List[Dict[str, Any]]],
    geometry: Dict[str, Any],
    srid: Optional[int],
    carte_url: str,
    pdf_url: str,
    nb_intersections: int,
) -> bool:
    """
    Insère une ligne dans `latresne.identite_fonciere_projects`.
    Retourne False si client indisponible ou erreur (sans lever : ne pas faire échouer /publier).
    """
    if supabase is None:
        logger.warning("CIF history : client Supabase non injecté, enregistrement ignoré")
        return False
    if not user_id or not str(user_id).strip():
        logger.info("CIF history : pas de user_id, enregistrement historique ignoré")
        return False

    centroid = geojson_centroid_wgs84(geometry, srid)
    row: Dict[str, Any] = {
        "project_id": project_id.strip(),
        "user_id": str(user_id).strip(),
        "user_email": (user_email or "").strip() or None,
        "commune": (commune or "").strip(),
        "insee": (insee or "").strip(),
        "parcelle_label": (parcelle_label or "").strip() or None,
        "parcelles_cadastrales": parcelles_cadastrales,
        "centroid": centroid,
        "carte_url": carte_url,
        "pdf_url": pdf_url,
        "nb_intersections": int(nb_intersections),
    }
    try:
        supabase.schema("latresne").table("identite_fonciere_projects").insert(row).execute()
        logger.info("CIF history : ligne enregistrée (project_id=%s, user_id=%s)", project_id, user_id)
        return True
    except Exception as exc:
        logger.warning("CIF history : échec insert Supabase : %s", exc, exc_info=True)
        return False


def list_identite_fonciere_projects_by_user(
    user_id: str,
    *,
    limit: int = 50,
) -> Dict[str, Any]:
    """Liste les CIF d'un utilisateur (même principe que GET /pipelines/by_user)."""
    if supabase is None:
        return {"success": False, "error": "Client Supabase non configuré"}
    try:
        res = (
            supabase.schema("latresne")
            .table("identite_fonciere_projects")
            .select("*")
            .eq("user_id", user_id.strip())
            .order("created_at", desc=True)
            .limit(min(max(limit, 1), 200))
            .execute()
        )
        rows = res.data or []
        return {"success": True, "count": len(rows), "projects": rows}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def delete_identite_fonciere_project_for_user(project_id: str, user_id: str) -> Dict[str, Any]:
    """
    Supprime la ligne `identite_fonciere_projects` pour ce couple (project_id, user_id),
    puis supprime les fichiers Storage sous `{project_id}/`.
    """
    pid = (project_id or "").strip()
    uid = (user_id or "").strip()
    if not pid or not uid:
        return {"success": False, "error": "project_id et user_id requis"}
    if supabase is None:
        return {"success": False, "error": "Client Supabase non configuré"}

    try:
        check = (
            supabase.schema("latresne")
            .table("identite_fonciere_projects")
            .select("project_id")
            .eq("project_id", pid)
            .eq("user_id", uid)
            .limit(1)
            .execute()
        )
        if not (check.data and len(check.data) > 0):
            return {"success": False, "error": "Dossier introuvable ou accès refusé"}
    except Exception as exc:
        logger.warning("CIF history delete : lecture impossible : %s", exc)
        return {"success": False, "error": str(exc)}

    try:
        supabase.schema("latresne").table("identite_fonciere_projects").delete().eq("project_id", pid).eq(
            "user_id", uid
        ).execute()
    except Exception as exc:
        logger.warning("CIF history delete : échec suppression ligne : %s", exc)
        return {"success": False, "error": str(exc)}

    warnings: List[str] = []
    try:
        from .storage_et_urls import delete_identite_fonciere_storage_prefix

        delete_identite_fonciere_storage_prefix(pid)
    except RuntimeError as exc:
        warnings.append(f"Storage non configuré : {exc}")
    except Exception as exc:
        warnings.append(f"Storage : {exc}")
        logger.warning("CIF history delete : fichiers Storage : %s", exc, exc_info=True)

    return {"success": True, "project_id": pid, "warnings": warnings or None}
