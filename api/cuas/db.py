# -*- coding: utf-8 -*-
"""
db.py — Configuration et persistance du pipeline CUA.

Section CONFIG :
- constantes schéma (SCHEMA, SRID, GEOM_COL, bucket)
- get_engine() : engine SQLAlchemy (psycopg2), réutilisé partout

Section PERSISTANCE :
- upload Supabase Storage + upsert table <schema>.pipelines
- importable séparément via persist_cua() en fin de pipeline

Exemple :
    from db import get_engine, SCHEMA, persist_cua
    persist_cua(slug=..., docx_path=..., refs=uf.parcelles,
                surface_cad=uf.surface_cadastrale, commune="argeles", code_insee="66008")
"""

import os
import functools
import logging
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from supabase import create_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cua")

# ============================================================
# CONFIG — constantes schéma + connexion PostGIS
# ============================================================
SCHEMA = os.getenv("ARGELES_SCHEMA", "argeles")
SRID = 2154
GEOM_COL = "geom_2154"  # convention du schéma argeles (override possible par couche)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "visualisation")


def _database_url() -> str:
    host = os.getenv("SUPABASE_HOST")
    db = os.getenv("SUPABASE_DB")
    user = os.getenv("SUPABASE_USER")
    pwd = os.getenv("SUPABASE_PASSWORD")
    port = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")

    # Gotcha pooler : en session mode 5432 on sature vite (MaxClientsInSessionMode).
    if host and "pooler.supabase.com" in host and port == "5432":
        logger.warning("SUPABASE_PORT=5432 sur pooler → bascule auto vers 6543 (transaction mode).")
        port = "6543"

    if not all([host, db, user, pwd]):
        raise RuntimeError(
            "Variables d'environnement manquantes : "
            "SUPABASE_HOST / SUPABASE_DB / SUPABASE_USER / SUPABASE_PASSWORD."
        )
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


@functools.lru_cache(maxsize=1)
def get_engine():
    """Engine SQLAlchemy unique (cached). Pool volontairement petit (run séquentiel)."""
    engine = create_engine(
        _database_url(),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    logger.info("🔗 Engine SQLAlchemy initialisé.")
    return engine


# ============================================================
# PERSISTANCE — Supabase Storage + table pipelines
# ============================================================
_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


@functools.lru_cache(maxsize=1)
def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not (url and key):
        raise RuntimeError("SUPABASE_URL et SERVICE_KEY/SUPABASE_SERVICE_ROLE_KEY requis pour la persistance.")
    return create_client(url, key)


def upload_file(local_path, remote_path, bucket=None, content_type="application/octet-stream") -> str:
    """Upload un fichier et renvoie l'URL publique."""
    bucket = bucket or SUPABASE_BUCKET
    sb = get_supabase()
    data = Path(local_path).read_bytes()
    sb.storage.from_(bucket).upload(
        remote_path,
        data,
        {"content-type": content_type, "cache-control": "no-cache", "upsert": "true"},
    )
    return f"{os.getenv('SUPABASE_URL')}/storage/v1/object/public/{bucket}/{remote_path}"


def persist_cua(
    *,
    slug: str,
    docx_path: str,
    refs,
    surface_cad: float,
    commune: str,
    code_insee: str,
    user_id: str | None = None,
    user_email: str | None = None,
    extra: dict | None = None,
) -> dict:
    """Upload le CUA DOCX puis upsert la ligne dans <schema>.pipelines."""
    sb = get_supabase()

    remote = f"{slug}/CUA_unite_fonciere.docx"
    cua_url = upload_file(docx_path, remote, content_type=_DOCX_MIME)
    logger.info(f"📎 CUA uploadé : {cua_url}")

    record = {
        "slug": slug,
        "commune": commune,
        "code_insee": code_insee,
        "status": "success",
        "bucket_path": slug,
        "output_cua": cua_url,
        "parcelles": refs,
        "surface_cadastrale": surface_cad,
        "user_id": user_id,
        "user_email": user_email,
    }
    if extra:
        record.update(extra)

    try:
        sb.schema(SCHEMA).table("pipelines").upsert(record).execute()
        logger.info(f"🗄️  Pipeline {slug} enregistré dans {SCHEMA}.pipelines.")
    except Exception as e:
        logger.error(f"💥 Échec insert {SCHEMA}.pipelines : {e}")
        raise

    return {"slug": slug, "cua_url": cua_url}
