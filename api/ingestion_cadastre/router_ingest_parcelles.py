"""
router_ingest_parcelles.py

Endpoint FastAPI d'ingestion des parcelles cadastrales Etalab.
À monter sur le backend Render — le fetch Etalab et l'upsert Supabase
se font entièrement côté serveur, sans transit par le Mac.

Montage dans main.py (cua_latresne_v4) :
    from api.ingestion_cadastre.router_ingest_parcelles import router as parcelles_ingest_router
    app.include_router(parcelles_ingest_router, prefix="/admin")

Endpoints :
    POST /admin/parcelles/ingest
    GET  /admin/parcelles/status/{job_id}
    GET  /admin/parcelles/jobs

Notes index :
    Avant un run massif, désactiver les index dans Supabase SQL editor :
        DROP INDEX IF EXISTS parcelles.idx_parcelles_geom_2154;
        DROP INDEX IF EXISTS parcelles.idx_parcelles_code_insee;
        DROP INDEX IF EXISTS parcelles.idx_parcelles_section_num;
    Après le run, recréer :
        CREATE INDEX CONCURRENTLY idx_parcelles_geom_2154 ON parcelles.parcelles USING GIST (geom_2154);
        CREATE INDEX CONCURRENTLY idx_parcelles_code_insee ON parcelles.parcelles (code_insee);
        CREATE INDEX CONCURRENTLY idx_parcelles_section_num ON parcelles.parcelles (section, numero);

Lancer le run : 
curl -X POST https://api.kerelia.fr/admin/parcelles/ingest \  -H "Content-Type: application/json" \
  -d '{"departements": ["16", "17", "19", "23", "24", "40", "47", "64", "79", "86", "87"]}'
"""

import logging
import os
import uuid
import time
import io
import requests
import geopandas as gpd
import psycopg2
from datetime import datetime
from shapely.geometry import shape, MultiPolygon, Polygon
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DB_URL = (
    f"postgresql://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
    f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT', '5432')}/{os.getenv('SUPABASE_DB', 'postgres')}"
)
DIRECT_URL = (os.getenv("SUPABASE_DIRECT_URL") or "").strip()

ETALAB_COM   = "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles"
GEO_API_DEP  = "https://geo.api.gouv.fr/departements/{dep}/communes?fields=code&format=json"
TARGET_TABLE = "parcelles.parcelles"
PAUSE_S      = 0.2

NOUVELLE_AQUITAINE = ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"]

JOBS: dict[str, dict] = {}
logger = logging.getLogger(__name__)
router = APIRouter(tags=["parcelles"])

# ─── SCHEMAS ──────────────────────────────────────────────────────────────────


class IngestRequest(BaseModel):
    communes:           Optional[list[str]] = None
    departements:       Optional[list[str]] = None
    nouvelle_aquitaine: bool = False
    dry_run:            bool = False


class JobStatus(BaseModel):
    job_id:              str
    status:              str
    started_at:          Optional[str]
    finished_at:         Optional[str]
    communes_done:       int
    communes_total:      int
    parcelles_upserted:  int
    errors:              list[str]
    log:                 list[str]


# ─── HELPERS ──────────────────────────────────────────────────────────────────


def get_engine():
    return create_engine(
        DB_URL,
        poolclass=NullPool,
        connect_args={"sslmode": "require"},
    )


def get_communes_du_dep(dep: str) -> list[str]:
    """Récupère les codes INSEE des communes d'un département via geo.api.gouv.fr."""
    try:
        r = requests.get(GEO_API_DEP.format(dep=dep), timeout=30)
        r.raise_for_status()
        return [c["code"] for c in r.json()]
    except Exception as e:
        logger.warning("geo.api.gouv.fr dep %s échoué : %s", dep, e)
        return []


def fetch_commune_geojson(insee: str) -> list[dict] | None:
    try:
        r = requests.get(ETALAB_COM.format(insee=insee), timeout=120)
        r.raise_for_status()
        feats = r.json().get("features", [])
        logger.info("Etalab commune %s : %d features", insee, len(feats))
        return feats
    except Exception as e:
        logger.warning("Etalab commune %s échoué : %s", insee, e)
        return None


def features_to_rows(features: list[dict]) -> list[dict]:
    """
    Reprojection vectorisée : un seul GeoDataFrame pour toute la commune.
    Peak RAM ~10 Mo par commune. del explicite pour libérer après.
    """
    props_list, geoms = [], []

    for f in features:
        p     = f["properties"]
        insee = p.get("commune", "")
        try:
            geom = shape(f["geometry"])
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            geoms.append(geom)
        except Exception:
            continue
        props_list.append({
            "idu":        p.get("id"),
            "code_dep":   insee[:2] if len(insee) >= 2 else None,
            "code_insee": insee,
            "section":    p.get("section"),
            "numero":     p.get("numero"),
            "feuille":    None,
            "com_abs":    p.get("prefixe"),
            "contenance": p.get("contenance"),
            "arpente":    p.get("arpente"),
            "updated":    p.get("updated"),
        })

    if not props_list:
        return []

    gdf      = gpd.GeoDataFrame(props_list, geometry=geoms, crs="EPSG:4326")
    gdf_2154 = gdf.to_crs("EPSG:2154")

    rows = []
    for i in range(len(gdf_2154)):
        g = gdf_2154.geometry.iloc[i]
        if g is None or g.is_empty:
            continue
        row = {k: gdf_2154.iloc[i][k] for k in [
            "idu", "code_dep", "code_insee", "section",
            "numero", "com_abs", "contenance", "arpente", "updated", "feuille"
        ]}
        row["geom_2154"] = g.wkt
        rows.append(row)

    del gdf, gdf_2154
    return rows


def upsert_rows_copy(rows: list[dict], dry_run: bool, job_id: str) -> tuple[int, int]:
    """
    Ingestion via COPY staging + INSERT SELECT ON CONFLICT.
    Connexion directe PostgreSQL (bypass PgBouncer).
    """
    if dry_run or not rows:
        return len(rows), 0

    if not DIRECT_URL:
        raise RuntimeError("SUPABASE_DIRECT_URL manquant")

    t0   = time.perf_counter()
    conn = psycopg2.connect(DIRECT_URL, sslmode="require")
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE parcelles_staging (
                    idu        text,
                    code_dep   text,
                    code_insee text,
                    section    text,
                    numero     text,
                    feuille    integer,
                    com_abs    text,
                    contenance double precision,
                    arpente    boolean,
                    updated    date,
                    geom_2154  text
                ) ON COMMIT DROP
            """)

            def val(v):
                if v is None:
                    return "\\N"
                if isinstance(v, bool):
                    return "true" if v else "false"
                return str(v).replace("\t", " ").replace("\n", " ")

            buf = io.StringIO()
            for r in rows:
                buf.write("\t".join([
                    val(r.get("idu")),        val(r.get("code_dep")),
                    val(r.get("code_insee")), val(r.get("section")),
                    val(r.get("numero")),     val(r.get("feuille")),
                    val(r.get("com_abs")),    val(r.get("contenance")),
                    val(r.get("arpente")),    val(r.get("updated")),
                    val(r.get("geom_2154")),
                ]) + "\n")
            buf.seek(0)

            t_copy = time.perf_counter()
            cur.copy_from(buf, "parcelles_staging", sep="\t", null="\\N",
                columns=["idu","code_dep","code_insee","section","numero",
                         "feuille","com_abs","contenance","arpente","updated","geom_2154"])
            copy_s = time.perf_counter() - t_copy

            t_merge = time.perf_counter()
            cur.execute(f"""
                INSERT INTO {TARGET_TABLE}
                    (idu, code_dep, code_insee, section, numero, feuille,
                     com_abs, contenance, arpente, updated, geom_2154)
                SELECT
                    idu, code_dep, code_insee, section, numero, feuille,
                    com_abs, contenance, arpente, updated,
                    ST_Multi(ST_GeomFromText(geom_2154, 2154))
                FROM parcelles_staging
                WHERE geom_2154 IS NOT NULL
                ON CONFLICT (idu, code_dep) DO UPDATE SET
                    section    = EXCLUDED.section,
                    numero     = EXCLUDED.numero,
                    com_abs    = EXCLUDED.com_abs,
                    contenance = EXCLUDED.contenance,
                    arpente    = EXCLUDED.arpente,
                    updated    = EXCLUDED.updated,
                    geom_2154  = EXCLUDED.geom_2154,
                    ingere_le  = now()
            """)
            merge_s  = time.perf_counter() - t_merge
            inserted = cur.rowcount
            conn.commit()

        logger.info(
            "[parcelles_ingest job=%s] COPY+MERGE %d lignes "
            "(copy=%.2fs merge=%.2fs total=%.2fs)",
            job_id, inserted, copy_s, merge_s, time.perf_counter() - t0
        )
        return inserted, 0

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ─── RÉSOLUTION DES CIBLES ────────────────────────────────────────────────────


def resolve_communes(req: IngestRequest, job: dict) -> list[str]:
    """
    Résout toujours en liste de codes INSEE commune par commune.
    Départements → appel geo.api.gouv.fr.
    Garantit que jamais un fichier département entier n'est chargé en RAM.
    """
    if req.nouvelle_aquitaine:
        deps = NOUVELLE_AQUITAINE
    elif req.departements:
        deps = req.departements
    else:
        deps = []

    communes = list(req.communes or [])

    for dep in deps:
        job["log"].append(f"Résolution communes dep {dep}...")
        logger.info("[parcelles_ingest job=%s] résolution dep %s", job["job_id"], dep)
        insee_list = get_communes_du_dep(dep)
        if not insee_list:
            job["log"].append(f"  ⚠️  Aucune commune pour dep {dep}")
        else:
            job["log"].append(f"  → {len(insee_list)} communes pour dep {dep}")
            logger.info("[parcelles_ingest job=%s] dep %s → %d communes",
                        job["job_id"], dep, len(insee_list))
        communes.extend(insee_list)

    return communes


# ─── JOB BACKGROUND ───────────────────────────────────────────────────────────


def run_ingest_job(job_id: str, req: IngestRequest):
    job               = JOBS[job_id]
    job["status"]     = "running"
    job["started_at"] = datetime.utcnow().isoformat()
    engine            = get_engine()

    communes = resolve_communes(req, job)
    if not communes:
        job["status"] = "error"
        job["errors"].append("Aucune commune résolue")
        return

    job["communes_total"] = len(communes)
    job["log"].append(f"→ {len(communes)} communes à traiter — dry_run={req.dry_run}")
    logger.info("[parcelles_ingest job=%s] démarrage : %d communes dry_run=%s",
                job_id, len(communes), req.dry_run)

    for i, insee in enumerate(communes):
        label = f"[{i+1}/{len(communes)}] {insee}"

        # Fetch
        t0       = time.perf_counter()
        features = fetch_commune_geojson(insee)
        fetch_s  = round(time.perf_counter() - t0, 1)

        if features is None:
            msg = f"{label} ❌ fetch échoué"
            job["log"].append(msg)
            job["errors"].append(msg)
            job["communes_done"] += 1
            continue

        # Conversion vectorisée
        t1     = time.perf_counter()
        rows   = features_to_rows(features)
        conv_s = round(time.perf_counter() - t1, 1)

        # Upsert COPY
        try:
            t2          = time.perf_counter()
            upserted, _ = upsert_rows_copy(rows, req.dry_run, job_id)
            ups_s       = round(time.perf_counter() - t2, 1)
        except Exception as e:
            msg = f"{label} ❌ upsert échoué : {str(e)[:120]}"
            job["log"].append(msg)
            job["errors"].append(msg)
            logger.error("[parcelles_ingest job=%s] %s", job_id, msg)
            job["communes_done"] += 1
            continue

        job["parcelles_upserted"] += upserted
        job["communes_done"]      += 1

        suffix = "(dry run)" if req.dry_run else f"{upserted} upsertées"
        job["log"].append(
            f"{label} ✅ {len(rows)} parcelles — {suffix} "
            f"(fetch={fetch_s}s conv={conv_s}s ups={ups_s}s)"
        )
        logger.info(
            "[parcelles_ingest job=%s] %s — %s fetch=%.1fs conv=%.1fs ups=%.1fs",
            job_id, label, suffix, fetch_s, conv_s, ups_s
        )

        # Progression tous les 50
        if (i + 1) % 50 == 0:
            pct = round((i + 1) / len(communes) * 100)
            job["log"].append(
                f"── Progression {i+1}/{len(communes)} ({pct}%) "
                f"— {job['parcelles_upserted']:,} parcelles upsertées"
            )

        time.sleep(PAUSE_S)

    job["status"]      = "done"
    job["finished_at"] = datetime.utcnow().isoformat()
    job["log"].append(f"✅ Terminé — {job['parcelles_upserted']:,} parcelles upsertées")
    logger.info(
        "[parcelles_ingest job=%s] job terminé status=done "
        "parcelles_upserted=%d errors=%d",
        job_id, job["parcelles_upserted"], len(job["errors"])
    )


# ─── ENDPOINTS ────────────────────────────────────────────────────────────────


@router.post("/parcelles/ingest")
async def ingest_parcelles(req: IngestRequest, background_tasks: BackgroundTasks):
    """
    Lance une ingestion en background sur Render.
    Toujours commune par commune — peak RAM ~10 Mo/commune.
    Upsert via COPY + connexion directe PostgreSQL (bypass PgBouncer).

    Exemples :
        {"nouvelle_aquitaine": true}
        {"departements": ["33", "17"]}
        {"communes": ["33234", "33063"]}
        {"departements": ["33"], "dry_run": true}
    """
    if not any([req.communes, req.departements, req.nouvelle_aquitaine]):
        raise HTTPException(400, "Spécifier communes, departements ou nouvelle_aquitaine=true")

    job_id = str(uuid.uuid4())[:8]
    JOBS[job_id] = {
        "job_id":             job_id,
        "status":             "pending",
        "started_at":         None,
        "finished_at":        None,
        "communes_done":      0,
        "communes_total":     0,
        "parcelles_upserted": 0,
        "errors":             [],
        "log":                [],
    }

    background_tasks.add_task(run_ingest_job, job_id, req)
    logger.info(
        "[parcelles_ingest job=%s] tâche background enqueued "
        "(communes=%s departements=%s na=%s dry_run=%s)",
        job_id, req.communes, req.departements, req.nouvelle_aquitaine, req.dry_run,
    )

    return {
        "job_id":     job_id,
        "message":    "Ingestion lancée — commune par commune, COPY direct PostgreSQL",
        "status_url": f"/admin/parcelles/status/{job_id}",
    }


@router.get("/parcelles/status/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    if job_id not in JOBS:
        raise HTTPException(404, f"Job {job_id} introuvable")
    return JOBS[job_id]


@router.get("/parcelles/jobs")
async def list_jobs():
    return [
        {k: v for k, v in job.items() if k != "log"}
        for job in JOBS.values()
    ]