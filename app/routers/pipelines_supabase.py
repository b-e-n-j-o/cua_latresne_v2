"""
Pipelines persistés (Supabase public.pipelines) et debug connexion.
"""

from fastapi import APIRouter, Depends, HTTPException

from app.deps import supabase
from services.auth.current_user import get_current_user_id
from services.auth.pipelines_query import pipelines_schema
from services.history.pipeline_enrichment import enrich_pipelines_for_history
from services.history.project_management import assert_can_view_pipeline

router = APIRouter(tags=["pipelines-supabase"])


def _pipelines_table():
    return supabase.schema(pipelines_schema()).table("pipelines")


@router.get("/pipelines/latest")
def get_latest_pipelines(limit: int = 10, user_id: str | None = None):
    """Derniers pipelines enregistrés (filtrés par droits si user_id fourni)."""
    try:
        query = _pipelines_table().select("*").order("created_at", desc=True).limit(limit)
        if user_id:
            from services.auth.commune_access import get_authorized_insee_codes

            allowed = get_authorized_insee_codes(user_id)
            if allowed:
                query = query.in_("code_insee", allowed)

        response = query.execute()
        pipelines = response.data or []
        if user_id:
            from services.auth.commune_access import filter_pipelines_for_viewer

            pipelines = filter_pipelines_for_viewer(pipelines, user_id)
        pipelines = enrich_pipelines_for_history(pipelines)
        return {
            "success": True,
            "count": len(pipelines),
            "pipelines": pipelines,
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@router.get("/pipelines/by_slug")
def get_pipeline_by_slug(slug: str, user_id: str = Depends(get_current_user_id)):
    """Retrouve un pipeline par slug (lien court CUA)."""
    try:
        pipeline = assert_can_view_pipeline(slug, user_id)
        enriched = enrich_pipelines_for_history([pipeline])
        pipeline = enriched[0] if enriched else pipeline

        return {
            "success": True,
            "pipeline": pipeline,
        }

    except HTTPException:
        raise
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@router.get("/debug/supabase")
def debug_supabase():
    """Vérifie la connectivité Supabase (pipelines + public)."""
    schema = pipelines_schema()
    try:
        print("🧩 [DEBUG] Vérification connexion Supabase...")

        res_pipelines = (
            supabase.schema(schema)
            .table("pipelines")
            .select("slug, commune_slug, code_insee, created_at")
            .limit(3)
            .execute()
        )
        nb_pipelines = len(res_pipelines.data or [])
        print(f"✅ [DEBUG] {schema}.pipelines OK — {nb_pipelines} ligne(s) visibles")

        res_public = (
            supabase.schema("public")
            .table("shortlinks")
            .select("slug, target_url, created_at")
            .limit(3)
            .execute()
        )
        nb_public = len(res_public.data or [])
        print(f"✅ [DEBUG] public.shortlinks OK — {nb_public} ligne(s) visibles")

        res_access = None
        nb_access = 0
        try:
            res_access = (
                supabase.schema("public")
                .table("user_commune_access")
                .select("user_id, commune_slug, role")
                .limit(3)
                .execute()
            )
            nb_access = len(res_access.data or [])
            print(f"✅ [DEBUG] public.user_commune_access OK — {nb_access} ligne(s)")
        except Exception as access_err:
            print(f"⚠️ [DEBUG] user_commune_access : {access_err}")

        return {
            "status": "ok",
            "pipelines_schema": schema,
            "pipelines": {
                "rows": nb_pipelines,
                "examples": res_pipelines.data,
            },
            "user_commune_access": {
                "rows": nb_access,
                "examples": (res_access.data if res_access else []),
            },
            "public": {
                "rows": nb_public,
                "examples": res_public.data,
            },
        }

    except Exception as e:
        print(f"💥 [DEBUG] Erreur connexion Supabase : {e}")
        return {
            "status": "error",
            "details": str(e),
        }
