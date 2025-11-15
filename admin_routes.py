from fastapi import APIRouter, HTTPException, Query, Body
from supabase import create_client
import os, json
from pathlib import Path

router = APIRouter(prefix="/admin", tags=["admin"])

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SERVICE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

CATALOGUE_DIR = Path(__file__).resolve().parent / "CATALOGUES"
CATALOGUES = {
    "intersections": CATALOGUE_DIR / "catalogue_intersections_tagged.json",
    "cartes": CATALOGUE_DIR / "catalogue_couches_map.json",
}

# -------------------------------------------------
# 🔵 1) Récupérer la liste des schémas
# -------------------------------------------------
@router.get("/schemas")
def list_schemas():
    """Retourne les schémas disponibles (déduits de Supabase)."""
    try:
        q = """
        SELECT schema_name 
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%' AND schema_name NOT LIKE 'information_%'
        """
        res = supabase.rpc("exec_sql", {"sql": q}).execute()
        return [r["schema_name"] for r in res.data]
    except Exception as e:
        raise HTTPException(500, f"Erreur list_schemas : {e}")

# -------------------------------------------------
# 🔵 2) Récupérer les tables d’un schéma
# -------------------------------------------------
@router.get("/tables")
def list_tables(schema: str):
    q = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = %(schema)s
    ORDER BY table_name
    """
    res = supabase.rpc("exec_sql", {"sql": q, "schema": schema}).execute()
    return [r["table_name"] for r in res.data]

# -------------------------------------------------
# 🔵 3) Lire une table paginée
# -------------------------------------------------
@router.get("/table-data")
def table_data(schema: str, table: str, page: int = 1):
    page_size = 10
    start = (page - 1) * page_size

    try:
        rows = (
            supabase
            .schema(schema)
            .table(table)
            .select("*")
            .range(start, start + page_size - 1)
            .execute()
        ).data

        # retirer les colonnes geom
        if rows:
            for r in rows:
                for k in list(r.keys()):
                    if "geom" in k.lower():
                        r[k] = "<GEOMETRY HIDDEN>"

        return {
            "success": True,
            "page": page,
            "page_size": page_size,
            "rows": rows
        }
    except Exception as e:
        raise HTTPException(500, f"Erreur lecture table : {e}")

# -------------------------------------------------
# 🟢 4) Lister les catalogues
# -------------------------------------------------
@router.get("/catalogues")
def list_catalogues():
    return list(CATALOGUES.keys())

# -------------------------------------------------
# 🟢 5) Lire un catalogue JSON
# -------------------------------------------------
@router.get("/catalogue/{name}")
def get_catalogue(name: str):
    path = CATALOGUES.get(name)
    if not path or not path.exists():
        raise HTTPException(404, "Catalogue introuvable")
    return json.loads(path.read_text(encoding="utf-8"))

# -------------------------------------------------
# 🟢 6) Modifier un catalogue JSON
# -------------------------------------------------
@router.post("/catalogue/{name}")
def update_catalogue(name: str, payload: dict = Body(...)):
    path = CATALOGUES.get(name)
    if not path:
        raise HTTPException(404, "Catalogue inexistant")

    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return {"success": True, "updated": name}
