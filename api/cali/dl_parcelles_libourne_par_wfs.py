#!/usr/bin/env python3
"""
ETL CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle → libourne.parcelles_{nom_commune}
Pagination WFS par startIndex, filtre CQL code_insee
"""

import os
import argparse
import threading
import time
import hashlib
import resource
import geopandas as gpd
import requests
from requests.adapters import HTTPAdapter
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform as shapely_transform
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pyproj import Transformer

load_dotenv()

WFS_URL   = "https://data.geopf.fr/wfs/ows"
CODE_INSEE = "33234"   # Latresne
NOM_TABLE  = "parcelles_latresne"   # → latresne.parcelles_latresne
SCHEMA_NAME = "latresne"
# Pages plus petites = GeoJSON plus légers par requête (souvent moins de timeouts / réponses >60s qu’avec 1000)
PAGE_SIZE  = 5000
# Pause entre pages (WFS IGN throttlé) — monter à 3.0 si besoin
PAGE_SLEEP_SECONDS = 1.5
# Retries applicatifs par page (après la couche urllib3)
PAGE_FETCH_MAX_ATTEMPTS = 8
PAGE_FETCH_BACKOFF_CAP_SECONDS = 90
# Timeout (connexion, lecture) — gros GeoJSON possible
WFS_TIMEOUT = (30, 120)
# Affiche une ligne toutes les N secondes si la requête (HTTP + parse JSON) n’a pas fini
HTTP_PROGRESS_PULSE_SECONDS = 20
WFS_HEADERS = {"Connection": "close"}
UPSERT_BATCH_SIZE = 1000


def get_session():
    """Pas de Retry urllib3 ici : ils sont silencieux (longs) ; tout passe par fetch_wfs_page_json."""
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=0))
    return session


def log_memory(stage: str):
    """Affiche la RAM RSS du process (MB)."""
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS -> bytes ; Linux -> KB
    rss_mb = rss_kb / (1024 * 1024) if rss_kb > 10_000_000 else rss_kb / 1024
    print(f"  [RAM] {stage}: {rss_mb:.1f} MB", flush=True)


def normalize_scalar(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:.12g}"
    return str(v).strip()


def build_record_hash(record: dict, geom_wkb_2154: bytes) -> str:
    payload = "|".join(
        [
            normalize_scalar(record.get("id")),
            normalize_scalar(record.get("gid")),
            normalize_scalar(record.get("numero")),
            normalize_scalar(record.get("feuille")),
            normalize_scalar(record.get("section")),
            normalize_scalar(record.get("code_dep")),
            normalize_scalar(record.get("nom_com")),
            normalize_scalar(record.get("code_com")),
            normalize_scalar(record.get("com_abs")),
            normalize_scalar(record.get("code_arr")),
            normalize_scalar(record.get("contenance")),
            normalize_scalar(record.get("code_insee")),
        ]
    ).encode("utf-8")
    h = hashlib.sha1()
    h.update(payload)
    h.update(b"|")
    h.update(geom_wkb_2154 or b"")
    return h.hexdigest()


def _http_get_json_with_pulse(session, params: dict, page_label: str):
    """GET + json() dans un thread + messages si le blocage dure (transfert lent, serveur, gros JSON)."""
    done = threading.Event()
    result, err = [], []

    def target():
        try:
            t0 = time.monotonic()
            r = session.get(
                WFS_URL,
                params=params,
                timeout=WFS_TIMEOUT,
                headers=WFS_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
            dt = time.monotonic() - t0
            result.append((data, dt))
        except Exception as e:
            err.append(e)
        finally:
            done.set()

    threading.Thread(target=target, daemon=True).start()
    waited = 0
    while not done.wait(timeout=HTTP_PROGRESS_PULSE_SECONDS):
        waited += HTTP_PROGRESS_PULSE_SECONDS
        print(
            f"    … {page_label} : transfert / traitement en cours ({waited}s, "
            f"timeout lecture {WFS_TIMEOUT[1]}s) …",
            flush=True,
        )
    if err:
        raise err[0]
    if not result:
        raise RuntimeError(f"Requête {page_label} terminée sans résultat ni erreur")
    data, dt = result[0]
    print(f"    ✓ {page_label} répondu en {dt:.1f}s", flush=True)
    return data


def fetch_wfs_page_json(session, params: dict, page_label: str) -> dict:
    """
    GET WFS + parse JSON : retries applicatifs uniquement (backoff visible, pas de Retry urllib3 muet).
    """
    last_err = None
    for attempt in range(1, PAGE_FETCH_MAX_ATTEMPTS + 1):
        try:
            print(
                f"    → HTTP {page_label} (tentative applicative {attempt}/{PAGE_FETCH_MAX_ATTEMPTS})…",
                flush=True,
            )
            return _http_get_json_with_pulse(session, params, page_label)
        except Exception as e:
            last_err = e
            if attempt >= PAGE_FETCH_MAX_ATTEMPTS:
                break
            wait = min(2 ** (attempt - 1), PAGE_FETCH_BACKOFF_CAP_SECONDS)
            print(
                f"\n    ⚠ {page_label} tentative {attempt}/{PAGE_FETCH_MAX_ATTEMPTS} : {e!r} "
                f"— attente {wait}s",
                flush=True,
            )
            time.sleep(wait)
    raise RuntimeError(f"WFS indisponible après {PAGE_FETCH_MAX_ATTEMPTS} tentatives : {last_err}") from last_err


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
        f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT', '6543')}/{os.getenv('SUPABASE_DB', 'postgres')}"
    )


def fetch_parcelles_wfs(code_insee: str) -> gpd.GeoDataFrame:
    print(f"\n[1/3] FETCH WFS PARCELLAIRE_EXPRESS — code_insee={code_insee}")

    session = get_session()
    all_features = []
    start_index = 0
    total = None

    while True:
        params = {
            "service":      "WFS",
            "version":      "2.0.0",
            "request":      "GetFeature",
            "typeNames":    "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle",
            "srsName":      "EPSG:2154",
            "outputFormat": "application/json",
            "CQL_FILTER":   f"code_insee='{code_insee}'",
            "count":        PAGE_SIZE,
            "startIndex":   start_index,
        }

        print(f"  → Page startIndex={start_index} …", flush=True)

        data = fetch_wfs_page_json(
            session,
            params,
            page_label=f"startIndex={start_index}",
        )

        features = data.get("features", [])
        all_features.extend(features)

        if total is None:
            total = data.get("numberMatched") or data.get("totalFeatures")

        print(f"→ {len(features)} entités  ({len(all_features)}/{total})")

        if len(features) < PAGE_SIZE:
            break

        start_index += PAGE_SIZE
        time.sleep(PAGE_SLEEP_SECONDS)

    print(f"\n  ✓ {len(all_features)} parcelles récupérées")

    if not all_features:
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:2154")
    print(f"  ✓ Colonnes : {list(gdf.columns)}")
    log_memory("apres fetch WFS")
    return gdf


def create_table(engine, nom_table: str):
    full = f"{SCHEMA_NAME}.{nom_table}"
    print(f"\n[2/3] CRÉATION SCHÉMA + TABLE {full}")
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {full} (
                id          TEXT,
                gid         INTEGER,
                numero      TEXT,
                feuille     INTEGER,
                section     TEXT,
                code_dep    TEXT,
                nom_com     TEXT,
                code_com    TEXT,
                com_abs     TEXT,
                code_arr    TEXT,
                idu         TEXT,
                contenance  DOUBLE PRECISION,
                code_insee  TEXT,
                geom_2154   GEOMETRY,
                geom_3857   GEOMETRY
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_parcelles_{nom_table}_idu_unique
            ON {full} (idu);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_parcelles_{nom_table}_geom_2154
            ON {full} USING GIST (geom_2154);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_parcelles_{nom_table}_geom_3857
            ON {full} USING GIST (geom_3857);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS parcelles_{nom_table}_idx_section
            ON {full} (section, numero);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS parcelles_{nom_table}_idx_insee
            ON {full} (code_insee);
        """))
    print(f"  ✓ Table et index prêts")
    return full


def run_etl(engine, gdf: gpd.GeoDataFrame, full_table: str, dry_run: bool = False):
    print(f"\n[3/3] DIFF & UPSERT → {full_table}")

    # Index existant + contenu courant (pour hash et diff réel)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT
                    idu, id, gid, numero, feuille, section, code_dep, nom_com,
                    code_com, com_abs, code_arr, contenance, code_insee,
                    ST_AsBinary(geom_2154) AS geom_wkb
                FROM {full_table}
            """)).fetchall()
        existing_by_idu = {
            row[0]: {
                "id": row[1],
                "gid": row[2],
                "numero": row[3],
                "feuille": row[4],
                "section": row[5],
                "code_dep": row[6],
                "nom_com": row[7],
                "code_com": row[8],
                "com_abs": row[9],
                "code_arr": row[10],
                "contenance": row[11],
                "code_insee": row[12],
                "geom_wkb_2154": bytes(row[13]) if row[13] is not None else b"",
            }
            for row in rows
        }
    except:
        existing_by_idu = {}
    existing_ids = set(existing_by_idu.keys())
    print(f"  ↳ {len(existing_ids)} parcelles en base")
    log_memory("apres chargement index base")

    api_ids   = set()
    nouveaux  = []
    modifies  = []
    inchanges = 0
    transformer_2154_to_3857 = Transformer.from_crs(2154, 3857, always_xy=True)

    def to_multi(geom):
        if geom is None or geom.is_empty:
            return None
        if geom.geom_type == "Polygon":
            return MultiPolygon([geom])
        return geom

    for _, row in gdf.iterrows():
        props = {k: v for k, v in row.items() if k != "geometry"}
        idu   = str(props.get("idu", "") or "").strip()
        if not idu:
            continue

        api_ids.add(idu)
        geom = to_multi(row.geometry)
        if geom is None:
            continue

        def si(v):
            try: return int(v) if v is not None else None
            except: return None

        record = {
            "id":         str(props.get("id", "") or ""),
            "idu":        idu,
            "gid":        si(props.get("gid")),
            "numero":     str(props.get("numero", "") or ""),
            "feuille":    si(props.get("feuille")),
            "section":    str(props.get("section", "") or ""),
            "code_dep":   str(props.get("code_dep", "") or ""),
            "nom_com":    str(props.get("nom_com", "") or ""),
            "code_com":   str(props.get("code_com", "") or ""),
            "com_abs":    str(props.get("com_abs", "") or ""),
            "code_arr":   str(props.get("code_arr", "") or ""),
            "contenance": si(props.get("contenance")),
            "code_insee": str(props.get("code_insee", "") or ""),
        }

        geom_2154 = geom
        geom_3857 = shapely_transform(transformer_2154_to_3857.transform, geom_2154)
        record["geom_wkb_2154"] = geom_2154.wkb
        record["geom_wkb_3857"] = geom_3857.wkb
        record_hash = build_record_hash(record, record["geom_wkb_2154"])

        if idu not in existing_by_idu:
            nouveaux.append(record)
        else:
            existing = existing_by_idu[idu]
            existing_hash = build_record_hash(existing, existing.get("geom_wkb_2154", b""))
            if record_hash != existing_hash:
                modifies.append(record)
            else:
                inchanges += 1

    supprimes = existing_ids - api_ids
    print(f"  → Nouveaux   : {len(nouveaux)}")
    print(f"  → Maj/Upsert : {len(modifies)}")
    print(f"  → Inchangés  : {inchanges}")
    print(f"  → Supprimés  : {len(supprimes)}")
    log_memory("apres calcul diff")

    has_diff = bool(nouveaux or modifies or supprimes)
    print(f"  → Diff détecté : {'OUI' if has_diff else 'NON'}")

    if dry_run:
        print("\n  [DRY-RUN] Aucune écriture en base (insert/update/delete ignorés).")
        return

    upsert_sql = text(f"""
        INSERT INTO {full_table}
            (id, idu, gid, numero, feuille, section, code_dep, nom_com, code_com,
             com_abs, code_arr, contenance, code_insee, geom_2154, geom_3857)
        VALUES (
            :id, :idu, :gid, :numero, :feuille, :section, :code_dep, :nom_com, :code_com,
            :com_abs, :code_arr, :contenance, :code_insee,
            ST_Multi(ST_GeomFromWKB(:geom_wkb_2154, 2154)),
            ST_Multi(ST_GeomFromWKB(:geom_wkb_3857, 3857))
        )
        ON CONFLICT (idu) DO UPDATE SET
            id         = EXCLUDED.id,
            gid        = EXCLUDED.gid,
            numero     = EXCLUDED.numero,
            feuille    = EXCLUDED.feuille,
            section    = EXCLUDED.section,
            contenance = EXCLUDED.contenance,
            geom_2154  = EXCLUDED.geom_2154,
            geom_3857  = EXCLUDED.geom_3857
    """)

    with engine.begin() as conn:
        to_upsert = nouveaux + modifies
        for i in range(0, len(to_upsert), UPSERT_BATCH_SIZE):
            chunk = to_upsert[i:i + UPSERT_BATCH_SIZE]
            conn.execute(upsert_sql, chunk)
            print(f"  ↳ Batch UPSERT {i + len(chunk)}/{len(to_upsert)}", flush=True)

        if supprimes:
            print(f"\n  ── Suppressions ({len(supprimes)}) ──")
            for sid in list(supprimes)[:10]:
                print(f"    🗑  {sid}")
            if len(supprimes) > 10:
                print(f"    ... et {len(supprimes)-10} autres")
            conn.execute(
                text(f"DELETE FROM {full_table} WHERE idu = ANY(:ids)"),
                {"ids": list(supprimes)}
            )

    print(f"\n  ✓ {len(nouveaux)} insérés  |  {len(modifies)} mis à jour  |  {len(supprimes)} supprimés")
    log_memory("apres ecriture base")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calcule le diff sans écrire en base",
    )
    args = parser.parse_args()
    dry_run = args.dry_run or os.getenv("DRY_RUN", "").lower() in {"1", "true", "yes", "y"}

    engine     = get_engine()
    gdf        = fetch_parcelles_wfs(CODE_INSEE)
    full_table = create_table(engine, NOM_TABLE)
    if len(gdf):
        run_etl(engine, gdf, full_table, dry_run=dry_run)
    mode_label = "DRY-RUN" if dry_run else "ETL"
    print(f"\n✅ {mode_label} parcelles {NOM_TABLE} terminé")