"""
router_sync_parcelles.py

Endpoint de synchronisation intelligente des parcelles cadastrales.
Compare les dates de mise à jour Etalab vs base et ne réingère
que les communes qui ont été modifiées depuis le dernier import.

Montage dans main.py :
    from api.ingestion_cadastre.router_sync_parcelles import router as parcelles_sync_router
    app.include_router(parcelles_sync_router, prefix="/admin")

Endpoints :
    POST /admin/parcelles/sync
    GET  /admin/parcelles/sync/status/{job_id}
    GET  /admin/parcelles/sync/jobs
    GET  /admin/parcelles/sync/check          → dry check sans réingestion

Stratégie :
    1. Pour chaque commune cible, récupère MAX(updated) en base
    2. Fetch les 3 premières features Etalab (stream partiel)
       pour lire la date updated la plus récente
    3. Si updated_etalab > updated_base → réingérer
    4. Si commune absente de la base → ingérer
    5. Si à jour → skip
"""

import logging
import os
import uuid
import time
import io
import requests
import geopandas as gpd
import psycopg2
from datetime import datetime, date
from shapely.geometry import shape, MultiPolygon, Polygon
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel
from sqlalchemy.pool import NullPool

logger = logging.getLogger(__name__)
router = APIRouter(tags=["parcelles-sync"])

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DIRECT_URL   = (os.getenv("SUPABASE_DIRECT_URL") or "").strip()
ETALAB_COM   = "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles"
GEO_API_DEP  = "https://geo.api.gouv.fr/departements/{dep}/communes?fields=code&format=json"
TARGET_TABLE = "parcelles.parcelles"
PAUSE_S      = 0.2

NOUVELLE_AQUITAINE = ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"]

SYNC_JOBS: dict[str, dict] = {}

# ─── SCHEMAS ──────────────────────────────────────────────────────────────────

class SyncRequest(BaseModel):
    communes:           Optional[list[str]] = None
    departements:       Optional[list[str]] = None
    nouvelle_aquitaine: bool = False
    dry_run:            bool = False   # check sans réingestion
    force:              bool = False   # forcer même si à jour


class SyncJobStatus(BaseModel):
    job_id:           str
    status:           str
    started_at:       Optional[str]
    finished_at:      Optional[str]
    communes_checked: int
    communes_total:   int
    communes_synced:  int    # réingérées
    communes_skipped: int    # déjà à jour
    communes_new:     int    # pas encore en base
    communes_errors:  int
    parcelles_upserted: int
    log:              list[str]


# ─── DB HELPERS ───────────────────────────────────────────────────────────────

def get_conn():
    if not DIRECT_URL:
        raise RuntimeError("SUPABASE_DIRECT_URL manquant")
    return psycopg2.connect(DIRECT_URL, sslmode="require")


def get_communes_du_dep(dep: str) -> list[str]:
    try:
        r = requests.get(GEO_API_DEP.format(dep=dep), timeout=30)
        r.raise_for_status()
        return [c["code"] for c in r.json()]
    except Exception as e:
        logger.warning("geo.api.gouv.fr dep %s échoué : %s", dep, e)
        return []


def get_max_updated_en_base(insee_list: list[str]) -> dict[str, date | None]:
    """
    Récupère MAX(updated) par commune pour une liste de codes INSEE.
    Retourne un dict {insee: date | None}.
    Une seule requête SQL pour toute la liste.
    """
    if not insee_list:
        return {}
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT code_insee, MAX(updated)
                FROM parcelles.parcelles
                WHERE code_insee = ANY(%s)
                GROUP BY code_insee
            """, (insee_list,))
            result = {row[0]: row[1] for row in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        logger.error("get_max_updated_en_base échoué : %s", e)
        return {}


def get_max_updated_etalab(insee: str) -> date | None:
    """
    Récupère la date updated la plus récente sur Etalab
    en ne lisant que les premiers Ko du stream GeoJSON.
    On cherche le champ "updated" dans les premières features.
    """
    try:
        r = requests.get(
            ETALAB_COM.format(insee=insee),
            timeout=30,
            stream=True
        )
        r.raise_for_status()

        # Lire jusqu'à avoir suffisamment de données pour extraire les dates
        chunk_total = b""
        for chunk in r.iter_content(chunk_size=16384):
            chunk_total += chunk
            # Arrêter après 100 Ko — largement suffisant pour avoir
            # plusieurs dates "updated" et trouver la plus récente
            if len(chunk_total) > 100_000:
                break
        r.close()

        # Extraire toutes les occurrences de "updated":"YYYY-MM-DD"
        import re
        dates_found = re.findall(rb'"updated"\s*:\s*"(\d{4}-\d{2}-\d{2})"', chunk_total)
        if not dates_found:
            return None

        # Retourner la plus récente
        max_date_str = max(d.decode() for d in dates_found)
        return date.fromisoformat(max_date_str)

    except Exception as e:
        logger.warning("get_max_updated_etalab %s échoué : %s", insee, e)
        return None


# ─── INGESTION (réutilise la même logique que router_ingest_parcelles) ────────

def features_to_rows(features: list[dict]) -> list[dict]:
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


def upsert_rows_copy(rows: list[dict], job_id: str) -> tuple[int, int]:
    if not rows:
        return 0, 0

    t0   = time.perf_counter()
    conn = get_conn()
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE parcelles_staging (
                    idu text, code_dep text, code_insee text,
                    section text, numero text, feuille integer,
                    com_abs text, contenance double precision,
                    arpente boolean, updated date, geom_2154 text
                ) ON COMMIT DROP
            """)

            def val(v):
                if v is None: return "\\N"
                if isinstance(v, bool): return "true" if v else "false"
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
                SELECT idu, code_dep, code_insee, section, numero, feuille,
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
            "[parcelles_sync job=%s] COPY+MERGE %d lignes (copy=%.2fs merge=%.2fs total=%.2fs)",
            job_id, inserted, copy_s, merge_s, time.perf_counter() - t0
        )
        return inserted, 0

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ingest_commune(insee: str, job_id: str, dry_run: bool) -> tuple[int, str]:
    """
    Ingère une commune complète.
    Retourne (nb_upserted, statut)
    statut: 'ok' | 'fetch_failed' | 'upsert_failed'
    """
    try:
        r = requests.get(ETALAB_COM.format(insee=insee), timeout=120)
        r.raise_for_status()
        features = r.json().get("features", [])
    except Exception as e:
        logger.warning("[parcelles_sync job=%s] fetch %s échoué : %s", job_id, insee, e)
        return 0, "fetch_failed"

    rows = features_to_rows(features)
    if not rows:
        return 0, "ok"

    if dry_run:
        return len(rows), "ok"

    try:
        upserted, _ = upsert_rows_copy(rows, job_id)
        return upserted, "ok"
    except Exception as e:
        logger.error("[parcelles_sync job=%s] upsert %s échoué : %s", job_id, insee, e)
        return 0, "upsert_failed"


# ─── RÉSOLUTION DES CIBLES ────────────────────────────────────────────────────

def resolve_communes(req: SyncRequest, job: dict) -> list[str]:
    if req.nouvelle_aquitaine:
        deps = NOUVELLE_AQUITAINE
    elif req.departements:
        deps = req.departements
    else:
        deps = []

    communes = list(req.communes or [])
    for dep in deps:
        job["log"].append(f"Résolution dep {dep}...")
        insee_list = get_communes_du_dep(dep)
        job["log"].append(f"  → {len(insee_list)} communes pour dep {dep}")
        communes.extend(insee_list)

    return communes


# ─── JOB BACKGROUND ───────────────────────────────────────────────────────────

def run_sync_job(job_id: str, req: SyncRequest):
    job               = SYNC_JOBS[job_id]
    job["status"]     = "running"
    job["started_at"] = datetime.utcnow().isoformat()

    communes = resolve_communes(req, job)
    if not communes:
        job["status"] = "error"
        job["log"].append("❌ Aucune commune résolue")
        return

    job["communes_total"] = len(communes)
    job["log"].append(
        f"→ {len(communes)} communes à vérifier — "
        f"dry_run={req.dry_run} force={req.force}"
    )
    logger.info(
        "[parcelles_sync job=%s] démarrage %d communes dry_run=%s force=%s",
        job_id, len(communes), req.dry_run, req.force
    )

    # Récupérer toutes les dates en base en une seule requête
    job["log"].append("Chargement des dates en base...")
    dates_en_base = get_max_updated_en_base(communes)
    job["log"].append(
        f"  → {len(dates_en_base)} communes déjà en base, "
        f"{len(communes) - len(dates_en_base)} absentes"
    )

    for i, insee in enumerate(communes):
        label     = f"[{i+1}/{len(communes)}] {insee}"
        date_base = dates_en_base.get(insee)  # None si absente de la base

        # Déterminer si sync nécessaire
        if req.force:
            raison = "force=True"
            need_sync = True
        elif date_base is None:
            raison = "commune absente de la base"
            need_sync = True
            job["communes_new"] += 1
        else:
            # Vérifier la date Etalab (stream partiel ~100 Ko)
            t_check   = time.perf_counter()
            date_etalab = get_max_updated_etalab(insee)
            check_s   = round(time.perf_counter() - t_check, 1)

            if date_etalab is None:
                # Impossible de vérifier → on sync par sécurité
                raison    = f"date Etalab indisponible → sync par sécurité"
                need_sync = True
            elif date_etalab > date_base:
                raison    = f"Etalab ({date_etalab}) > base ({date_base})"
                need_sync = True
            else:
                raison    = f"à jour (base={date_base}, etalab={date_etalab})"
                need_sync = False

        if not need_sync:
            job["communes_skipped"] += 1
            job["communes_checked"] += 1
            job["log"].append(f"{label} ⏭️  {raison}")
            logger.info("[parcelles_sync job=%s] %s skip : %s", job_id, label, raison)
            time.sleep(0.05)
            continue

        # Sync nécessaire
        job["log"].append(f"{label} 🔄 {raison} — ingestion...")
        logger.info("[parcelles_sync job=%s] %s sync : %s", job_id, label, raison)

        t0               = time.perf_counter()
        upserted, statut = ingest_commune(insee, job_id, req.dry_run)
        elapsed          = round(time.perf_counter() - t0, 1)

        job["communes_checked"] += 1

        if statut == "ok":
            job["communes_synced"]    += 1
            job["parcelles_upserted"] += upserted
            suffix = "(dry run)" if req.dry_run else f"{upserted:,} upsertées"
            job["log"].append(f"{label} ✅ {suffix} en {elapsed}s")
            logger.info(
                "[parcelles_sync job=%s] %s — %s en %.1fs",
                job_id, label, suffix, elapsed
            )
        else:
            job["communes_errors"] += 1
            job["log"].append(f"{label} ❌ {statut} en {elapsed}s")
            logger.error(
                "[parcelles_sync job=%s] %s — %s en %.1fs",
                job_id, label, statut, elapsed
            )

        # Progression tous les 50
        if (i + 1) % 50 == 0:
            pct = round((i + 1) / len(communes) * 100)
            job["log"].append(
                f"── Progression {i+1}/{len(communes)} ({pct}%) "
                f"— synced={job['communes_synced']} "
                f"skipped={job['communes_skipped']} "
                f"new={job['communes_new']} "
                f"parcelles={job['parcelles_upserted']:,}"
            )

        time.sleep(PAUSE_S)

    job["status"]      = "done"
    job["finished_at"] = datetime.utcnow().isoformat()
    job["log"].append(
        f"✅ Terminé — "
        f"{job['communes_synced']} synced, "
        f"{job['communes_skipped']} skipped, "
        f"{job['communes_new']} new, "
        f"{job['communes_errors']} errors, "
        f"{job['parcelles_upserted']:,} parcelles upsertées"
    )
    logger.info(
        "[parcelles_sync job=%s] terminé synced=%d skipped=%d new=%d "
        "errors=%d parcelles=%d",
        job_id,
        job["communes_synced"],
        job["communes_skipped"],
        job["communes_new"],
        job["communes_errors"],
        job["parcelles_upserted"],
    )


# ─── ENDPOINTS ────────────────────────────────────────────────────────────────

@router.post("/parcelles/sync")
async def sync_parcelles(req: SyncRequest, background_tasks: BackgroundTasks):
    """
    Synchronisation intelligente — ne réingère que les communes modifiées.

    Logique par commune :
      - Absente de la base        → ingestion
      - updated Etalab > base     → réingestion
      - À jour                    → skip

    Paramètres :
        force=true   → réingérer même si à jour
        dry_run=true → vérifier sans ingérer

    Exemples :
        {"departements": ["33"]}
        {"communes": ["33234", "33063"], "dry_run": true}
        {"nouvelle_aquitaine": true, "force": false}
    """
    if not any([req.communes, req.departements, req.nouvelle_aquitaine]):
        raise HTTPException(400, "Spécifier communes, departements ou nouvelle_aquitaine=true")

    job_id = str(uuid.uuid4())[:8]
    SYNC_JOBS[job_id] = {
        "job_id":             job_id,
        "status":             "pending",
        "started_at":         None,
        "finished_at":        None,
        "communes_checked":   0,
        "communes_total":     0,
        "communes_synced":    0,
        "communes_skipped":   0,
        "communes_new":       0,
        "communes_errors":    0,
        "parcelles_upserted": 0,
        "log":                [],
    }

    background_tasks.add_task(run_sync_job, job_id, req)
    logger.info(
        "[parcelles_sync job=%s] enqueued communes=%s deps=%s na=%s dry=%s force=%s",
        job_id, req.communes, req.departements, req.nouvelle_aquitaine,
        req.dry_run, req.force,
    )

    return {
        "job_id":     job_id,
        "message":    "Sync lancé en background",
        "status_url": f"/admin/parcelles/sync/status/{job_id}",
    }


@router.get("/parcelles/sync/status/{job_id}", response_model=SyncJobStatus)
async def get_sync_status(job_id: str):
    if job_id not in SYNC_JOBS:
        raise HTTPException(404, f"Job sync {job_id} introuvable")
    return SYNC_JOBS[job_id]


@router.get("/parcelles/sync/jobs")
async def list_sync_jobs():
    return [
        {k: v for k, v in job.items() if k != "log"}
        for job in SYNC_JOBS.values()
    ]


@router.get("/parcelles/sync/check")
async def check_communes(
    departement: Optional[str] = None,
    communes: Optional[str] = None,  # comma-separated
):
    """
    Check rapide sans lancer de job — retourne l'état de sync de chaque commune.
    Utile pour diagnostiquer avant de lancer un vrai sync.

    Exemples :
        GET /admin/parcelles/sync/check?departement=33
        GET /admin/parcelles/sync/check?communes=33234,33063,33550
    """
    if departement:
        insee_list = get_communes_du_dep(departement)
    elif communes:
        insee_list = [c.strip() for c in communes.split(",")]
    else:
        raise HTTPException(400, "Spécifier departement ou communes")

    if len(insee_list) > 200:
        raise HTTPException(400, f"Trop de communes ({len(insee_list)}) — max 200 pour un check synchrone")

    dates_base = get_max_updated_en_base(insee_list)

    results = []
    for insee in insee_list:
        date_base   = dates_base.get(insee)
        date_etalab = get_max_updated_etalab(insee)

        if date_base is None:
            statut = "absent"
        elif date_etalab is None:
            statut = "etalab_unavailable"
        elif date_etalab > date_base:
            statut = "outdated"
        else:
            statut = "up_to_date"

        results.append({
            "insee":        insee,
            "date_en_base": str(date_base) if date_base else None,
            "date_etalab":  str(date_etalab) if date_etalab else None,
            "statut":       statut,
        })

    summary = {
        "total":            len(results),
        "absent":           sum(1 for r in results if r["statut"] == "absent"),
        "outdated":         sum(1 for r in results if r["statut"] == "outdated"),
        "up_to_date":       sum(1 for r in results if r["statut"] == "up_to_date"),
        "etalab_unavailable": sum(1 for r in results if r["statut"] == "etalab_unavailable"),
    }

    return {"summary": summary, "communes": results}