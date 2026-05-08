"""
ingest_local.py

Ingestion locale complète des parcelles Etalab vers Supabase.
Tourne entièrement sur le Mac — pas de Render.

Stratégie :
- Fetch Etalab commune par commune
- Reprojection GeoPandas (profite des CPU M-series)
- COPY vers table staging + INSERT SELECT ON CONFLICT
  via connexion directe PostgreSQL (bypass PgBouncer)
- Skip communes déjà en base (preload au démarrage)
- Parallélisme configurable (N communes simultanées)

Usage :
    python ingest_local.py                        # toutes communes < 5k
    python ingest_local.py --dry-run              # sans insert
    python ingest_local.py --dep 33               # un département
    python ingest_local.py --parallelisme 4       # N threads
    python ingest_local.py --limit-communes 50    # test limité
"""

import os
import io
import csv
import time
import gc
import threading
import argparse
import requests
import geopandas as gpd
import psycopg2
import psutil
from psycopg2 import pool
from datetime import datetime
from shapely.geometry import shape, MultiPolygon, Polygon
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

CSV_PATH    = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CONFIG/v_commune_2025.csv"
ETALAB_COM  = "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles"

# Connexion directe PostgreSQL (bypass PgBouncer)
DIRECT_URL  = os.getenv(
    "SUPABASE_DIRECT_URL",
    f"postgresql://postgres:{os.getenv('SUPABASE_PASSWORD')}@db.odlkagfeqkbrruajlcxm.supabase.co:5432/postgres"
)

TARGET_TABLE              = "parcelles.parcelles"
MAX_PARCELLES_PAR_COMMUNE = 20_000
MAX_DB_SIZE_GO            = 2.0
PARALLELISME_DEFAULT      = 2
PAUSE_BETWEEN_S           = 0.1
POOL_MAXCONN              = 4

# ─── HELPERS DB ───────────────────────────────────────────────────────────────

_pool = None


def log_memory(label: str = ""):
    process = psutil.Process(os.getpid())
    ram_mo = process.memory_info().rss / 1e6
    print(f"  🧠 RAM {label}: {ram_mo:.0f} Mo", flush=True)


def get_pool():
    """Pool de connexions partagé, thread-safe."""
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=POOL_MAXCONN,
            dsn=DIRECT_URL,
            sslmode="require",
        )
    return _pool


def get_conn():
    return get_pool().getconn()


def release_conn(conn):
    get_pool().putconn(conn)


def load_communes_en_base() -> set[str]:
    """Charge tous les code_insee distincts déjà en base en une seule requête."""
    print("  Chargement des communes déjà en base...", flush=True)
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT code_insee FROM parcelles.parcelles")
            result = {row[0] for row in cur.fetchall()}
        print(f"  → {len(result):,} communes déjà en base\n")
        return result
    except Exception as e:
        print(f"  ⚠️  Impossible de charger les communes en base : {e}\n")
        return set()
    finally:
        if conn is not None:
            release_conn(conn)


def upsert_rows_copy(rows: list[dict]) -> tuple[int, int]:
    """
    Ingestion ultra-rapide via COPY + staging table :
    1. COPY toutes les lignes vers table temporaire
    2. INSERT SELECT ON CONFLICT depuis la temp
    Bypass complet de PgBouncer via connexion directe.
    """
    if not rows:
        return 0, 0

    conn = get_conn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Table temporaire de staging
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
                    geom_2154  text,
                    geom_3857  text
                ) ON COMMIT DROP
            """)

            # Préparer le buffer COPY
            buffer = io.StringIO()
            for r in rows:
                def val(v):
                    if v is None:
                        return "\\N"
                    if isinstance(v, bool):
                        return "true" if v else "false"
                    return str(v).replace("\t", " ").replace("\n", " ")

                line = "\t".join([
                    val(r["idu"]),
                    val(r["code_dep"]),
                    val(r["code_insee"]),
                    val(r["section"]),
                    val(r["numero"]),
                    val(r["feuille"]),
                    val(r["com_abs"]),
                    val(r["contenance"]),
                    val(r["arpente"]),
                    val(r["updated"]),
                    val(r["geom_2154"]),
                    val(r["geom_3857"]),
                ])
                buffer.write(line + "\n")
            buffer.seek(0)

            # COPY vers staging
            cur.copy_from(
                buffer, "parcelles_staging",
                sep="\t", null="\\N",
                columns=["idu","code_dep","code_insee","section","numero",
                         "feuille","com_abs","contenance","arpente","updated",
                         "geom_2154","geom_3857"]
            )

            # INSERT SELECT ON CONFLICT depuis staging
            cur.execute(f"""
                INSERT INTO {TARGET_TABLE}
                    (idu, code_dep, code_insee, section, numero, feuille,
                     com_abs, contenance, arpente, updated, geom_2154, geom_3857)
                SELECT
                    idu, code_dep, code_insee, section, numero, feuille,
                    com_abs, contenance, arpente, updated,
                    ST_Multi(ST_GeomFromText(geom_2154, 2154)),
                    ST_Multi(ST_GeomFromText(geom_3857, 3857))
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
                    geom_3857  = EXCLUDED.geom_3857,
                    ingere_le  = now()
            """)

            inserted = cur.rowcount
            conn.commit()
            return inserted, 0

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        release_conn(conn)


# ─── HELPERS ETALAB ───────────────────────────────────────────────────────────

def count_parcelles_etalab(insee: str) -> int | None:
    """Estimation du nb de parcelles via stream partiel."""
    try:
        r = requests.get(ETALAB_COM.format(insee=insee), timeout=60, stream=True)
        r.raise_for_status()
        chunk_total = b""
        count = 0
        for chunk in r.iter_content(chunk_size=32768):
            chunk_total += chunk
            count = chunk_total.count(b'"id":"')
            if count > MAX_PARCELLES_PAR_COMMUNE or len(chunk_total) > 2_000_000:
                r.close()
                return count
        r.close()
        return count
    except Exception:
        return None


def fetch_et_convertir(insee: str) -> list[dict] | None:
    """Fetch + reprojection GeoPandas pour une commune."""
    try:
        r = requests.get(ETALAB_COM.format(insee=insee), timeout=120)
        r.raise_for_status()
        features = r.json().get("features", [])
    except Exception:
        return None

    if not features:
        return []

    props_list = []
    geoms      = []

    for f in features:
        p     = f["properties"]
        insee_code = p.get("commune", "")
        try:
            geom = shape(f["geometry"])
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            geoms.append(geom)
        except Exception:
            continue
        props_list.append({
            "idu":        p.get("id"),
            "code_dep":   insee_code[:2] if len(insee_code) >= 2 else None,
            "code_insee": insee_code,
            "section":    p.get("section"),
            "numero":     p.get("numero"),
            "com_abs":    p.get("prefixe"),
            "contenance": p.get("contenance"),
            "arpente":    p.get("arpente"),
            "updated":    p.get("updated"),
            "feuille":    None,
        })

    if not props_list:
        return []

    gdf      = gpd.GeoDataFrame(props_list, geometry=geoms, crs="EPSG:4326")
    log_memory(f"{insee} avant reprojection")
    gdf_2154 = gdf.to_crs("EPSG:2154")
    gdf_3857 = gdf.to_crs("EPSG:3857")
    log_memory(f"{insee} après reprojection")

    rows = []
    for i in range(len(gdf_2154)):
        g2154 = gdf_2154.geometry.iloc[i]
        g3857 = gdf_3857.geometry.iloc[i]
        if g2154 is None or g2154.is_empty:
            continue
        row = {k: gdf_2154.iloc[i][k] for k in [
            "idu","code_dep","code_insee","section",
            "numero","com_abs","contenance","arpente","updated","feuille"
        ]}
        row["geom_2154"] = g2154.wkt
        row["geom_3857"] = g3857.wkt if g3857 else None
        rows.append(row)

    # Limite l'accumulation mémoire en purgeant explicitement les GeoDataFrames
    del gdf, gdf_2154, gdf_3857
    gc.collect()

    return rows


# ─── TRAITEMENT D'UNE COMMUNE ─────────────────────────────────────────────────

def traiter_commune(com: dict, dry_run: bool, skip_count: bool) -> dict:
    """Traite une commune complète — appelable depuis un thread."""
    insee = com["insee"]
    nom   = com["nom"]
    t0    = time.time()

    # Count
    if not skip_count:
        nb = count_parcelles_etalab(insee)
        if nb is None:
            return {"insee": insee, "nom": nom, "status": "count_failed", "upserted": 0, "nb": 0, "elapsed": 0}
        if nb > MAX_PARCELLES_PAR_COMMUNE:
            return {"insee": insee, "nom": nom, "status": "ignoree", "upserted": 0, "nb": nb, "elapsed": 0}
    else:
        nb = "?"

    # Fetch + conversion
    rows = fetch_et_convertir(insee)
    if rows is None:
        return {"insee": insee, "nom": nom, "status": "fetch_failed", "upserted": 0, "nb": nb, "elapsed": 0}

    if not rows:
        return {"insee": insee, "nom": nom, "status": "ok", "upserted": 0, "nb": 0, "elapsed": 0}

    # Upsert
    if dry_run:
        elapsed = round(time.time() - t0, 1)
        return {"insee": insee, "nom": nom, "status": "ok", "upserted": len(rows), "nb": nb, "elapsed": elapsed, "dry_run": True}

    try:
        upserted, errors = upsert_rows_copy(rows)
        elapsed = round(time.time() - t0, 1)
        return {"insee": insee, "nom": nom, "status": "ok", "upserted": upserted, "nb": nb, "elapsed": elapsed}
    except Exception as e:
        elapsed = round(time.time() - t0, 1)
        return {"insee": insee, "nom": nom, "status": f"upsert_error: {str(e)[:80]}", "upserted": 0, "nb": nb, "elapsed": elapsed}


# ─── CHARGEMENT CSV ───────────────────────────────────────────────────────────

def load_communes(csv_path: str, dep_filter: str = None) -> list[dict]:
    communes = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["TYPECOM"] != "COM":
                continue
            if dep_filter and row["DEP"] != dep_filter:
                continue
            communes.append({
                "insee": row["COM"],
                "nom":   row["NCCENR"],
                "dep":   row["DEP"],
            })
    return communes


def estimate_db_size_go(total_upserted: int) -> float:
    return (total_upserted * 500) / 1e9


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",        action="store_true")
    parser.add_argument("--dep",            type=str, default=None)
    parser.add_argument("--limit-communes", type=int, default=None)
    parser.add_argument("--parallelisme",   type=int, default=PARALLELISME_DEFAULT)
    parser.add_argument("--skip-count",     action="store_true")
    args = parser.parse_args()

    mode = "🔍 DRY RUN" if args.dry_run else "🚀 INGESTION RÉELLE"
    print(f"\n{'='*65}")
    print(f"  ingest_local.py — {mode}")
    print(f"  Seuil     : < {MAX_PARCELLES_PAR_COMMUNE:,} parcelles/commune")
    print(f"  Threads   : {args.parallelisme}")
    print(f"  Stop      : {MAX_DB_SIZE_GO} Go ingérés")
    print(f"  Connexion : directe PostgreSQL (bypass PgBouncer)")
    if args.dep:
        print(f"  Filtre    : dep {args.dep}")
    print(f"{'='*65}\n")

    # Charger CSV
    communes = load_communes(CSV_PATH, dep_filter=args.dep)
    if args.limit_communes:
        communes = communes[:args.limit_communes]
    print(f"  {len(communes):,} communes chargées depuis le CSV")

    # Preload communes déjà en base
    if not args.dry_run:
        en_base = load_communes_en_base()
        communes = [c for c in communes if c["insee"] not in en_base]
        print(f"  {len(communes):,} communes restantes à traiter\n")
    else:
        print()

    # Stats
    total_upsertees  = 0
    total_ignorees   = 0
    total_erreurs    = 0
    total_traitees   = 0
    communes_erreurs = []
    started_at       = datetime.now()
    print_lock       = threading.Lock()
    stop_event       = threading.Event()

    with ThreadPoolExecutor(max_workers=args.parallelisme) as executor:
        futures = {
            executor.submit(traiter_commune, com, args.dry_run, args.skip_count): com
            for com in communes
        }

        done_count = 0
        for future in as_completed(futures):
            if stop_event.is_set():
                future.cancel()
                continue

            done_count += 1
            res   = future.result()
            isbn  = res["insee"]
            nom   = res["nom"]
            label = f"[{done_count:05d}/{len(communes):05d}] {nom} ({isbn})"

            with print_lock:
                if res["status"] == "ok":
                    total_upsertees += res["upserted"]
                    total_traitees  += 1
                    suffix = "(dry run)" if args.dry_run else f"{res['upserted']:,} upsertées"
                    print(f"  {label} — ✅ {suffix} en {res['elapsed']}s")

                elif res["status"] == "ignoree":
                    total_ignorees += 1
                    print(f"  {label} — ⏭️  {res['nb']:,} parcelles > seuil")

                else:
                    total_erreurs += 1
                    communes_erreurs.append(res)
                    print(f"  {label} — ❌ {res['status']}")

                if done_count % 50 == 0:
                    log_memory(f"après {done_count} communes")

                # Progression tous les 100
                if done_count % 100 == 0:
                    size_go = estimate_db_size_go(total_upsertees)
                    elapsed = (datetime.now() - started_at).seconds // 60
                    vitesse = total_upsertees / max(1, (datetime.now() - started_at).seconds)
                    print(f"\n  ── {done_count}/{len(communes)} communes"
                          f" — {total_upsertees:,} parcelles"
                          f" — ~{size_go:.2f} Go"
                          f" — {vitesse:.0f} parc/s"
                          f" — {elapsed} min\n")

                    if size_go >= MAX_DB_SIZE_GO:
                        print(f"\n  🛑 Seuil {MAX_DB_SIZE_GO} Go atteint — arrêt")
                        stop_event.set()
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

    # Rapport final
    elapsed_total = (datetime.now() - started_at).seconds
    size_go       = estimate_db_size_go(total_upsertees)
    vitesse_moy   = total_upsertees / max(1, elapsed_total)

    print(f"\n{'='*65}")
    print(f"  RAPPORT FINAL — {mode}")
    print(f"{'='*65}")
    print(f"  Durée              : {elapsed_total//60} min {elapsed_total%60} s")
    print(f"  Communes traitées  : {total_traitees:,}")
    print(f"  Communes ignorées  : {total_ignorees:,} (> seuil)")
    print(f"  Communes en erreur : {total_erreurs:,}")
    print(f"  Parcelles upsertées: {total_upsertees:,}")
    print(f"  Vitesse moyenne    : {vitesse_moy:.1f} parcelles/s")
    print(f"  Taille DB estimée  : ~{size_go:.2f} Go")
    print(f"{'='*65}")

    if communes_erreurs:
        print(f"\n  ❌ Communes en erreur :")
        for e in communes_erreurs[:20]:
            print(f"      {e['nom']} ({e['insee']}) — {e['status']}")
    print()


if __name__ == "__main__":
    main()