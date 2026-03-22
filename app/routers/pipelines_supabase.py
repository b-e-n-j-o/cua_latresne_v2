"""
Pipelines persistés (Supabase latresne.pipelines) et debug connexion.
"""

from fastapi import APIRouter

from app.deps import supabase

router = APIRouter(tags=["pipelines-supabase"])


@router.get("/pipelines/latest")
def get_latest_pipelines(limit: int = 10):
    """Derniers pipelines enregistrés pour Latresne depuis Supabase."""
    try:
        response = (
            supabase.schema("latresne")
            .table("pipelines")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )

        pipelines = response.data or []
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
def get_pipeline_by_slug(slug: str):
    """Retrouve un pipeline par slug (lien court CUA)."""
    try:
        response = (
            supabase.schema("latresne")
            .table("pipelines")
            .select("*")
            .eq("slug", slug)
            .limit(1)
            .execute()
        )

        rows = response.data or []
        if not rows:
            return {
                "success": False,
                "error": "Slug introuvable",
            }

        return {
            "success": True,
            "pipeline": rows[0],
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


@router.get("/debug/supabase")
def debug_supabase():
    """Vérifie la connectivité Supabase (schémas latresne + public)."""
    try:
        print("🧩 [DEBUG] Vérification connexion Supabase...")

        res_latresne = (
            supabase.schema("latresne")
            .table("pipelines")
            .select("id, slug, created_at")
            .limit(3)
            .execute()
        )
        nb_latresne = len(res_latresne.data or [])
        print(f"✅ [DEBUG] latresne.pipelines OK — {nb_latresne} ligne(s) visibles")

        res_public = (
            supabase.schema("public")
            .table("shortlinks")
            .select("slug, target_url, created_at")
            .limit(3)
            .execute()
        )
        nb_public = len(res_public.data or [])
        print(f"✅ [DEBUG] public.shortlinks OK — {nb_public} ligne(s) visibles")

        return {
            "status": "ok",
            "latresne": {
                "rows": nb_latresne,
                "examples": res_latresne.data,
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
