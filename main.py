"""
Point d'entrée FastAPI Kerelia CUA — agrège middleware, routeurs métier et routeurs existants (carto, RAG, etc.).
"""

from dotenv import load_dotenv

load_dotenv()

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from admin_routes import router as admin_router
from api.communes import router as communes_router
from api.departements import router as departements_router
from api.generate_dpe import router as dpe_router
from api.identite_parcelle.route_identite_parcelle import router as identite_parcelle_router
from api.identite_parcelle.zonage_plui import router as zonage_plui_router
from api.latresne.parcelles_geojson import router as parcelles_geojson_router
from api.latresne.patrimoine import router as patrimoine_router
from api.latresne.tiles_latresne import router as latresne_router
from api.latresne.tiles_mbtiles import router as latresne_mbtiles_router
from api.parcelle_et_voisins import router as parcelle_router
from api.parcelle_geometrie import router as parcelle_geometrie_router
from api.plu.chat import router as chat_router
from api.plu.fetch_plu import router as plu_router
from api.tiles_generic import router as tiles_router
from api.tiles_mbtiles import router as mbtiles_router
from api.tiles_mbtiles_parcelles import router as tiles_parcelles
from api.topography_consolidated import router as topo_router
from app.deps import supabase
from app.routers.cerfa import router as cerfa_router
from app.routers.cua_pipeline import router as cua_pipeline_router
from app.routers.pipelines_supabase import router as pipelines_supabase_router
from app.routers.product import router as product_router
from app.routers.site_account import router as site_account_router
from CUA.docx import cua_docx_viewer_routes
from rag.cag_plu_routes import router as cag_plu_router
from rag.rag_routes import router as rag_router
from rag.rag_routes_meta import router as rag_meta_router
from rag.rag_routes_parallel import router as rag_parallel_router
from rag.rag_routes_plu import router as rag_plu_router
from services.history.centroid_history import router as centroid_history_router
import services.history.centroid_history as centroid_history_module
from services.history.suivi import router as suivi_router
import services.history.suivi as suivi_module

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

cua_docx_viewer_routes.supabase = supabase
centroid_history_module.supabase = supabase
suivi_module.supabase = supabase

app = FastAPI(title="Kerelia CUA API", version="2.1")


@app.on_event("startup")
async def log_routes():
    print("\n=== ROUTES DISPONIBLES ===")
    for route in app.routes:
        print(route.path, route.methods)
    print("=========================\n")


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "https://www.kerelia.fr",
        "https://kerelia.fr",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Back-office / admin ---
app.include_router(admin_router)
app.include_router(cua_docx_viewer_routes.router)

# --- RAG / chat PLU ---
app.include_router(rag_router)
app.include_router(rag_plu_router)
app.include_router(rag_meta_router)
app.include_router(cag_plu_router)
app.include_router(rag_parallel_router)

# --- Données / carto ---
app.include_router(communes_router)
app.include_router(departements_router)
app.include_router(latresne_mbtiles_router)
app.include_router(tiles_router)
app.include_router(mbtiles_router)
app.include_router(parcelle_router)
app.include_router(topo_router)
app.include_router(dpe_router)
app.include_router(plu_router)
app.include_router(chat_router)
app.include_router(identite_parcelle_router)
app.include_router(zonage_plui_router)
app.include_router(latresne_router)
app.include_router(parcelles_geojson_router)
app.include_router(patrimoine_router)
app.include_router(parcelle_geometrie_router)
app.include_router(tiles_parcelles)

# Analyse CERFA (PDF)
app.include_router(cerfa_router)
app.include_router(centroid_history_router)
app.include_router(suivi_router)

# --- Cœur métier CUA / parcelles (jobs + polling) ---
app.include_router(cua_pipeline_router)

# --- Pipelines Supabase + debug ---
app.include_router(pipelines_supabase_router)

# --- Utilitaires produit (résumé IA CUA) ---
app.include_router(product_router)

# --- Site / compte (santé, leads, auth) ---
app.include_router(site_account_router)
