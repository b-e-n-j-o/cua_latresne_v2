#!/usr/bin/env python3
"""
ETL batch : liste_communes_cali.json → cali.parcelles (une table pour toutes les communes).
Pour chaque commune : fetch WFS (retry + pulse) puis UPSERT + DELETE limités au code_insee.
"""

import json
import os
import threading
import time
import hashlib
import resource
from pathlib import Path

import geopandas as gpd
import requests
from requests.adapters import HTTPAdapter
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform as shapely_transform
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pyproj import Transformer

load_dotenv()

WFS_URL = "https://data.geopf.fr/wfs/ows"
SCHEMA_NAME = "cali"
TABLE_NAME = "parcelles"
FULL_TABLE = f"{SCHEMA_NAME}.{TABLE_NAME}"

LISTE_COMMUNES_PATH = Path(__file__).resolve().parent / "liste_communes_cali.json"

PAGE_SIZE = 1000
PAGE_SLEEP_SECONDS = 1.5
PAGE_FETCH_MAX_ATTEMPTS = 8
PAGE_FETCH_BACKOFF_CAP_SECONDS = 90
WFS_TIMEOUT = (30, 120)
HTTP_PROGRESS_PULSE_SECONDS = 20
WFS_HEADERS = {"Connection": "close"}
UPSERT_BATCH_SIZE = 1000


def get_session():
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=0))
    return session


def log_memory(stage: str):
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
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


def fetch_parcelles_wfs(session, code_insee: str, label_commune: str) -> gpd.GeoDataFrame:
    print(f"\n  [WFS] PARCELLAIRE_EXPRESS — {label_commune} (INSEE {code_insee})")

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
            page_label=f"{code_insee} startIndex={start_index}",
        )

        features = data.get("features", [])
        all_features.extend(features)

        if total is None:
            total = data.get("numberMatched") or data.get("totalFeatures")

        print(f"  → {len(features)} entités  ({len(all_features)}/{total})")

        if len(features) < PAGE_SIZE:
            break

        start_index += PAGE_SIZE
        time.sleep(PAGE_SLEEP_SECONDS)

    print(f"\n  ✓ {len(all_features)} parcelles récupérées pour INSEE {code_insee}")

    if not all_features:
        return gpd.GeoDataFrame()

    gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:2154")
    print(f"  ✓ Colonnes : {list(gdf.columns)}")
    log_memory(f"{code_insee} apres fetch WFS")
    return gdf


def create_table_if_needed(engine):
    print(f"\n=== PRÉPARATION SCHÉMA / TABLE {FULL_TABLE} ===")
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
                idu         TEXT PRIMARY KEY,
                gid         BIGINT,
                numero      TEXT,
                feuille     INTEGER,
                section     TEXT,
                code_dep    TEXT,
                nom_com     TEXT,
                code_com    TEXT,
                com_abs     TEXT,
                code_arr    TEXT,
                contenance  INTEGER,
                code_insee  TEXT,
                geom_2154   GEOMETRY(MULTIPOLYGON, 2154),
                geom_3857   GEOMETRY(MULTIPOLYGON, 3857)
            );
        """))
        # Compatibilité avec table déjà existante: ajoute les colonnes manquantes sans casser l'existant
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS idu TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS gid BIGINT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS numero TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS feuille INTEGER"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS section TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS code_dep TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS nom_com TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS code_com TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS com_abs TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS code_arr TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS contenance INTEGER"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS code_insee TEXT"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS geom_2154 GEOMETRY"))
        conn.execute(text(f"ALTER TABLE {FULL_TABLE} ADD COLUMN IF NOT EXISTS geom_3857 GEOMETRY"))
        # Déduplication défensive avant index unique sur idu
        dedup_deleted = conn.execute(text(f"""
            WITH ranked AS (
                SELECT
                    ctid,
                    idu,
                    ROW_NUMBER() OVER (PARTITION BY idu ORDER BY ctid DESC) AS rn
                FROM {FULL_TABLE}
                WHERE idu IS NOT NULL AND idu <> ''
            )
            DELETE FROM {FULL_TABLE} t
            USING ranked r
            WHERE t.ctid = r.ctid
              AND r.rn > 1
            RETURNING t.idu;
        """)).fetchall()
        if dedup_deleted:
            print(
                f"  ⚠ Déduplication préalable: {len(dedup_deleted)} ligne(s) supprimée(s) "
                f"pour permettre l'index unique sur idu",
                flush=True,
            )
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS cali_parcelles_idx_idu_unique
            ON {FULL_TABLE} (idu);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS cali_parcelles_gix_2154
            ON {FULL_TABLE} USING GIST (geom_2154);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS cali_parcelles_gix_3857
            ON {FULL_TABLE} USING GIST (geom_3857);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS cali_parcelles_idx_section
            ON {FULL_TABLE} (section, numero);
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS cali_parcelles_idx_insee
            ON {FULL_TABLE} (code_insee);
        """))
    print(f"  ✓ Schéma / table / colonnes / index OK")


def run_etl_commune(engine, gdf, code_insee: str, nom_commune: str):
    """
    Diff + écriture pour UNE commune : existants = lignes en base avec ce code_insee uniquement.
    Suppressions : idus présents en base pour cet INSEE mais absents du flux WFS.
    """
    print(f"\n  [PG] DIFF & UPSERT → {FULL_TABLE} — {nom_commune} ({code_insee})")

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT
                        idu, gid, numero, feuille, section, code_dep, nom_com, code_com,
                        com_abs, code_arr, contenance, code_insee,
                        ST_AsBinary(geom_2154) AS geom_wkb
                    FROM {FULL_TABLE}
                    WHERE code_insee = :ci
                """),
                {"ci": code_insee},
            ).fetchall()
        existing_by_idu = {
            row[0]: {
                "gid": row[1],
                "numero": row[2],
                "feuille": row[3],
                "section": row[4],
                "code_dep": row[5],
                "nom_com": row[6],
                "code_com": row[7],
                "com_abs": row[8],
                "code_arr": row[9],
                "contenance": row[10],
                "code_insee": row[11],
                "geom_wkb_2154": bytes(row[12]) if row[12] is not None else b"",
            }
            for row in rows
        }
    except Exception:
        existing_by_idu = {}
    existing_ids = set(existing_by_idu.keys())

    print(f"  ↳ {len(existing_ids)} parcelles déjà en base pour cet INSEE")
    log_memory(f"{code_insee} apres chargement index base")

    api_ids = set()
    nouveaux = []
    modifies = []
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
        idu = str(props.get("idu", "") or "").strip()
        if not idu:
            continue

        api_ids.add(idu)
        geom = to_multi(row.geometry)
        if geom is None:
            continue

        def si(v):
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        record = {
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
            "code_insee": str(props.get("code_insee", "") or code_insee),
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
    print(f"  → Supprimés (INSEE {code_insee}) : {len(supprimes)}")
    log_memory(f"{code_insee} apres calcul diff")

    upsert_sql = text(f"""
        INSERT INTO {FULL_TABLE}
            (idu, gid, numero, feuille, section, code_dep, nom_com, code_com,
             com_abs, code_arr, contenance, code_insee, geom_2154, geom_3857)
        VALUES (
            :idu, :gid, :numero, :feuille, :section, :code_dep, :nom_com, :code_com,
            :com_abs, :code_arr, :contenance, :code_insee,
            ST_Multi(ST_GeomFromWKB(:geom_wkb_2154, 2154)),
            ST_Multi(ST_GeomFromWKB(:geom_wkb_3857, 3857))
        )
        ON CONFLICT (idu) DO UPDATE SET
            gid        = EXCLUDED.gid,
            numero     = EXCLUDED.numero,
            feuille    = EXCLUDED.feuille,
            section    = EXCLUDED.section,
            code_dep   = EXCLUDED.code_dep,
            nom_com    = EXCLUDED.nom_com,
            code_com   = EXCLUDED.code_com,
            com_abs    = EXCLUDED.com_abs,
            code_arr   = EXCLUDED.code_arr,
            contenance = EXCLUDED.contenance,
            code_insee = EXCLUDED.code_insee,
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
            print(f"\n  ── Suppressions ({min(10, len(supprimes))} affichées) ──")
            for sid in list(supprimes)[:10]:
                print(f"    🗑  {sid}")
            if len(supprimes) > 10:
                print(f"    ... et {len(supprimes) - 10} autres")
            conn.execute(
                text(
                    f"DELETE FROM {FULL_TABLE} "
                    "WHERE code_insee = :ci AND idu = ANY(:ids)"
                ),
                {"ci": code_insee, "ids": list(supprimes)},
            )

    print(
        f"  ✓ Commune {nom_commune} : {len(nouveaux)} ins. | {len(modifies)} maj. | {len(supprimes)} supp.\n",
        flush=True,
    )
    log_memory(f"{code_insee} apres ecriture base")


def main():
    if not LISTE_COMMUNES_PATH.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {LISTE_COMMUNES_PATH}")

    with open(LISTE_COMMUNES_PATH, encoding="utf-8") as f:
        communes = json.load(f)

    n = len(communes)
    print(f"\n{'='*60}")
    print(f"  ETL CALI — {n} communes → {FULL_TABLE}")
    print(f"{'='*60}")

    engine = get_engine()
    create_table_if_needed(engine)
    wfs_session = get_session()

    echecs = []
    for i, c in enumerate(communes, 1):
        nom = str(c.get("nom", "")).strip() or "?"
        code_insee = str(c.get("code_insee", "")).strip()
        if not code_insee:
            print(f"\n[!] [{i}/{n}] entrée sans code_insee ignorée : {c}")
            continue

        print(f"\n{'─'*60}")
        print(f"  [{i}/{n}] {nom}  —  INSEE {code_insee}")
        print(f"{'─'*60}")

        try:
            gdf = fetch_parcelles_wfs(wfs_session, code_insee, nom)
            if len(gdf) == 0:
                print(f"  ⚠ Aucune parcelle WFS pour {nom} — rien à écrire (table inchangée pour cet INSEE).")
                continue
            run_etl_commune(engine, gdf, code_insee, nom)
        except Exception as e:
            print(f"\n  ✗ ERREUR commune {nom} ({code_insee}) : {e!r}", flush=True)
            echecs.append({"nom": nom, "code_insee": code_insee, "erreur": str(e)})

    print(f"\n{'='*60}")
    print(f"  FIN — {n - len(echecs)}/{n} communes sans erreur fatale")
    if echecs:
        print(f"  Échecs ({len(echecs)}) :")
        for e in echecs:
            print(f"    - {e['nom']} ({e['code_insee']}): {e['erreur']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
