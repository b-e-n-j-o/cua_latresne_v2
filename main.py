"""
Point d'entrée FastAPI Kerelia CUA — agrège middleware, routeurs métier et routeurs existants (carto, RAG, etc.).
"""

from dotenv import load_dotenv

load_dotenv()

import logging
import os
import sys
from datetime import datetime, timezone

import httpx
import requests

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text

from admin_routes import router as admin_router
from api.departements import router as departements_router
from api.generate_dpe import router as dpe_router
import api.identite_fonciere.identite_fonciere_history as identite_fonciere_history_module
from api.identite_fonciere.route_identite_parcelle import (
    router as identite_parcelle_router,
    router_fonciere as identite_fonciere_router,
)
from api.documents_urba.router_reglement import router as reglement_router
from api.communes.latresne.parcelles_geojson import (
    router as parcelles_geojson_router,
    communes_router as communes_parcelles_geojson_router,
)
from api.communes.latresne.parcelles_via_adresse import router as parcelles_via_adresse_router
from api.communes.latresne.patrimoine import router as patrimoine_router
from api.communes.latresne.tiles_latresne import router as latresne_router
from api.communes.latresne.tiles_mbtiles import router as latresne_mbtiles_router
from api.parcelle_geometrie import router as parcelle_geometrie_router
from api.plu.chat import router as chat_router
from api.plu.fetch_plu import router as plu_router
from api.documents_urba.pieces_dossier_urba import router as pieces_dossier_urba_router
from api.agents.plu_agent.api import argeles_router as plu_agent_argeles_router
from api.agents.plu_agent.api import france_router as plu_agent_france_router
from api.agents.plu_agent.api import latresne_router as plu_agent_latresne_router
from api.agents.plu_agent.api import mios_router as plu_agent_mios_router
from api.lidar.lidar_router import router as lidar_router
from api.mnt.router_mnt import router as mnt_router
from api.tiles_generic import router as tiles_router
from api.tiles_mbtiles import router as mbtiles_router
from api.tiles_mbtiles_parcelles import router as tiles_parcelles
from api.topography_consolidated import router as topo_router
from services.ingestion_cadastre.router_ingest_parcelles import router as parcelles_ingest_router
from services.ingestion_cadastre.router_sync_parcelles import router as parcelles_sync_router
from services.ingestion_cadastre.router_etl_commune import router as etl_commune_router
from services.ingestion_mnt_lidar.router_ingestion_mnt_lidar import router as ingestion_mnt_lidar_router
from app.deps import supabase
from app.routers.cerfa import router as cerfa_router
from app.routers.cua_pipeline import router as cua_pipeline_router
from api.cuas.catalogue_routes import router as cua_catalogue_router
from api.cuas.cua_router import router as cua_generate_router
from api.cuas.parcelles_intersections_routes import router as parcelles_intersections_router
from app.routers.pipelines_supabase import router as pipelines_supabase_router
from app.routers.site_account import router as site_account_router
from api.communes.latresne.cuas.CUA.docx import cua_docx_viewer_routes
from services.history.centroid_history import router as centroid_history_router
import services.history.centroid_history as centroid_history_module
from services.history.suivi import router as suivi_router
import services.history.suivi as suivi_module
from services.history.project_management import router as project_management_router
import services.history.project_management as project_management_module
from services.history.project_directory import router as project_directory_router
import services.history.project_directory as project_directory_module
from services.plu_txt_markdown.router import router as plu_txt_markdown_router
from api.reglements.router_reglements import router as reglements_admin_router
from api.reglements.router_documents import router as documents_router


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

_logger = logging.getLogger(__name__)


def _slack_deploy_webhook() -> str:
    """URL Slack Incoming Webhook (secret) — définir SLACK_DEPLOY_WEBHOOK sur Render, pas dans le code."""
    return (os.getenv("SLACK_DEPLOY_WEBHOOK") or os.getenv("SLACK_WEBHOOK_URL") or "").strip()


def _slack_notifications_allowed() -> bool:
    """
    Évite le spam en local : pas de Slack sauf sur Render (variable RENDER injectée par Render)
    ou si SLACK_FORCE_NOTIFY=1 pour tester volontairement en local.
    """
    if os.getenv("SLACK_FORCE_NOTIFY", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    return (os.getenv("RENDER") or "").strip().lower() in ("true", "1", "yes")


def notify_slack(message: str) -> None:
    if not _slack_notifications_allowed():
        return
    url = _slack_deploy_webhook()
    if not url:
        return
    try:
        requests.post(url, json={"text": message}, timeout=10)
    except Exception as e:
        _logger.warning("Slack notification failed: %s", e)


def _slack_excepthook(exc_type, exc_value, exc_traceback):
    if exc_type is KeyboardInterrupt:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    try:
        notify_slack(f"Erreur non gérée (processus principal) : {exc_value!s}")
    except Exception:
        pass
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = _slack_excepthook

cua_docx_viewer_routes.supabase = supabase
centroid_history_module.supabase = supabase
identite_fonciere_history_module.supabase = supabase
suivi_module.supabase = supabase
project_management_module.supabase = supabase
project_directory_module.supabase = supabase

app = FastAPI(title="Kerelia CUA API", version="2.1")

# Render : INTERNAL_TOKEN + SLACK_WEBHOOK_RENDER_ALERT — header x-internal-token pour curl/scripts.
_INTERNAL_AGENT_TOKEN = (
    os.getenv("INTERNAL_TOKEN")
    or os.getenv("KERELIA_INTERNAL_AGENT_TOKEN")
    or os.getenv("INTERNAL_AGENT_TOKEN")
    or ""
).strip()
_SLACK_RENDER_ALERT_WEBHOOK = (os.getenv("SLACK_WEBHOOK_RENDER_ALERT") or "").strip()
_SCANNER_UA_FRAGMENTS = ("python-requests", "curl", "wget", "nikto", "sqlmap")


def _client_ip(request: Request) -> str:
    forwarded = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


async def _notify_slack_render_alert(message: str) -> None:
    if not _slack_notifications_allowed() or not _SLACK_RENDER_ALERT_WEBHOOK:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                _SLACK_RENDER_ALERT_WEBHOOK,
                json={"text": message},
                timeout=10.0,
            )
    except Exception as e:
        _logger.warning("Slack render alert failed: %s", e)


@app.middleware("http")
async def block_scanners(request: Request, call_next):
    """Bloque les UA type scanner sauf si x-internal-token correspond au secret env."""
    if not _INTERNAL_AGENT_TOKEN:
        return await call_next(request)
    ua = request.headers.get("user-agent") or ""
    token = (request.headers.get("x-internal-token") or "").strip()
    is_scanner = any(fragment in ua.lower() for fragment in _SCANNER_UA_FRAGMENTS)
    if is_scanner and token != _INTERNAL_AGENT_TOKEN:
        headers = dict(request.headers)
        msg = (
            f"Scanner bloqué | IP: {_client_ip(request)} | UA: {ua}"
            f" | Method: {request.method} | Path: {request.url.path}"
            f" | Headers: {headers}"
        )
        _logger.warning(msg)
        await _notify_slack_render_alert(msg)
        return JSONResponse(status_code=403, content={"detail": "Forbidden"})
    return await call_next(request)


@app.on_event("startup")
async def notify_slack_deploy_ok():
    """Message Slack à chaque démarrage réussi sur Render uniquement (voir _slack_notifications_allowed)."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    notify_slack(f"Backend Kerelia CUA démarré avec succès — {ts}")


@app.on_event("startup")
async def log_routes():
    print("\n=== ROUTES DISPONIBLES ===")
    for route in app.routes:
        print(route.path, route.methods)
    print("=========================\n")


@app.on_event("startup")
async def log_supabase_db_diagnostics():
    """
    Journalise la config Supabase DB réellement utilisée et tente de lire la
    capacité PG (si permissions suffisantes) pour faciliter le diagnostic pooler.
    """
    logger = logging.getLogger("startup.db")

    host = (os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
    db = (os.getenv("SUPABASE_DB") or "").strip().strip('"').strip("'")
    user = (os.getenv("SUPABASE_USER") or "").strip().strip('"').strip("'")
    password = (os.getenv("SUPABASE_PASSWORD") or "").strip().strip('"').strip("'")
    port = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")

    mode = "direct"
    if "pooler.supabase.com" in host:
        mode = "transaction" if port == "6543" else "session" if port == "5432" else "pooler"

    logger.info("=== SUPABASE DB DIAGNOSTICS ===")
    logger.info("host=%s", host or "<missing>")
    logger.info("port=%s", port)
    logger.info("db=%s", db or "<missing>")
    logger.info("user=%s", user or "<missing>")
    logger.info("mode_detecte=%s", mode)
    if "pooler.supabase.com" in host and port == "5432":
        logger.warning("Pooler session (5432) detecte: risque MaxClientsInSessionMode. Recommande: 6543.")

    if not all([host, db, user, password]):
        logger.warning("Variables DB incomplètes, diagnostic capacité ignoré.")
        return

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(
        db_url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 5, "sslmode": "require"},
    )

    try:
        with engine.connect() as conn:
            max_conn = conn.execute(text("SHOW max_connections")).scalar()
            reserved = conn.execute(text("SHOW superuser_reserved_connections")).scalar()
            try:
                active = conn.execute(text("SELECT COUNT(*) FROM pg_stat_activity")).scalar()
            except Exception:
                active = None

        logger.info("postgres.max_connections=%s", max_conn)
        logger.info("postgres.superuser_reserved_connections=%s", reserved)
        if active is not None:
            logger.info("postgres.pg_stat_activity.count=%s", active)
        else:
            logger.info("postgres.pg_stat_activity.count=<inaccessible>")
    except Exception as e:
        logger.warning("Impossible de lire la capacité PostgreSQL: %s", e)
    finally:
        engine.dispose()


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:5174",
        "http://localhost:3000",
        "https://www.kerelia.fr",
        "https://kerelia.fr",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Center-X", "X-Center-Y", "X-N-Points"],
)

# --- Back-office / admin ---
app.include_router(admin_router)
app.include_router(reglements_admin_router)
app.include_router(documents_router)
app.include_router(cua_docx_viewer_routes.router)
app.include_router(parcelles_ingest_router, prefix="/admin")
app.include_router(parcelles_sync_router, prefix="/admin")
app.include_router(etl_commune_router, prefix="/admin")
app.include_router(ingestion_mnt_lidar_router, prefix="/admin")


# --- Données / carto ---
app.include_router(departements_router)
app.include_router(latresne_mbtiles_router)
app.include_router(tiles_router)
app.include_router(mbtiles_router)
app.include_router(topo_router)
app.include_router(dpe_router)
app.include_router(plu_router)
app.include_router(chat_router)
app.include_router(plu_agent_argeles_router)
app.include_router(plu_agent_france_router)
app.include_router(plu_agent_latresne_router)
app.include_router(plu_agent_mios_router)
app.include_router(identite_parcelle_router)
app.include_router(identite_fonciere_router)
app.include_router(reglement_router)
app.include_router(pieces_dossier_urba_router)
app.include_router(latresne_router)
app.include_router(parcelles_geojson_router)
app.include_router(communes_parcelles_geojson_router)
app.include_router(parcelles_via_adresse_router)
app.include_router(patrimoine_router)
app.include_router(parcelle_geometrie_router)
app.include_router(tiles_parcelles)

# Analyse CERFA (PDF)
app.include_router(cerfa_router)
app.include_router(centroid_history_router)
app.include_router(suivi_router)
app.include_router(project_management_router)
app.include_router(project_directory_router)

# --- Cœur métier CUA / parcelles (jobs + polling) ---
app.include_router(cua_pipeline_router)
app.include_router(cua_catalogue_router)
app.include_router(cua_generate_router)
app.include_router(parcelles_intersections_router)

# --- Pipelines Supabase + debug ---
app.include_router(pipelines_supabase_router)

# --- PLU : TXT brut → Markdown (batch LLM) ---
app.include_router(plu_txt_markdown_router)

# --- LiDAR HD (nuage de points) ---
app.include_router(lidar_router, prefix="/lidar")

# --- MNT 3D (topographie) ---
app.include_router(mnt_router, prefix="/mnt")


# --- Site / compte (santé, leads, auth) ---
app.include_router(site_account_router)

