"""
ingest_ban.py
-------------
Ingestion des couches IGN BAN-PLUS (adresse + lien_adresse_parcelle)
pour une commune donnée, à partir du WFS de la Géoplateforme IGN.

Flux :
  1. Lit la bbox de <schema>.commune depuis PostGIS
  2. Fetche les deux couches WFS IGN avec cette bbox
  3. Intersecte les entités avec le polygone commune (Shapely)
  4. Crée les tables dans le schéma cible si elles n'existent pas
  5. Ingère les entités filtrées via psycopg (executemany)

Usage :
  python ingest_ban_adresse_et_lien_parcelles.py --schema latresne

  Connexion Supabase via cua_latresne_v4/.env (SUPABASE_HOST, SUPABASE_USER, …).
  Surcharge possible : --db-url "postgresql://…"

  Mode ETL (défaut) : mise à jour complète par commune (schéma dédié)
    - ban_adresse : upsert par id + suppression des adresses absentes du flux WFS
    - ban_lien_adresse_parcelle : vidage puis rechargement (liens parcelle ↔ adresse)

  Mode --append : conserve les anciennes lignes (insert liens sans purge, pas de delete adresses)

  Les tables créées :
    <schema>.ban_adresse
    <schema>.ban_lien_adresse_parcelle
"""

import argparse
import logging
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json

import psycopg
from psycopg.rows import dict_row
from shapely.geometry import shape, mapping
from shapely.wkt import loads as wkt_loads
from shapely.ops import transform
import pyproj

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

try:
    from services.ingestion_cadastre.env_loader import ENV_BACKEND, load_project_env
except ImportError:
    from env_loader import ENV_BACKEND, load_project_env


def connect_supabase(db_url: str | None = None):
    """
    Connexion PostGIS Supabase.
    Par défaut : variables SUPABASE_* depuis cua_latresne_v4/.env.
    """
    if db_url:
        return psycopg.connect(db_url, autocommit=False)

    load_project_env()
    host = os.getenv("SUPABASE_HOST")
    dbname = os.getenv("SUPABASE_DB")
    user = os.getenv("SUPABASE_USER")
    password = os.getenv("SUPABASE_PASSWORD")
    port = os.getenv("SUPABASE_PORT", "5432")
    sslmode = os.getenv("SUPABASE_SSLMODE", "require")

    if not all([host, dbname, user, password]):
        raise RuntimeError(
            f"Variables SUPABASE_* manquantes. Vérifier {ENV_BACKEND}"
        )

    log.info("Connexion Supabase : %s@%s:%s/%s", user, host, port, dbname)
    return psycopg.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
        autocommit=False,
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
WFS_URL = "https://data.geopf.fr/wfs/ows"
WFS_VERSION = "2.0.0"
WFS_SRSNAME = "EPSG:3857"
WFS_OUTPUT_FORMAT = "application/json"
WFS_COUNT = 10_000          # entités par page (paging)
MAX_PAGES = 500             # garde-fou

# Colonne géométrie de <schema>.commune (créée par ingest_commune.py)
COMMUNE_GEOM_COL = "geom_2154"

# DDL des tables cibles (géométries stockées en EPSG:4326 pour lisibilité)
DDL_ADRESSE = """
CREATE TABLE IF NOT EXISTS {schema}.ban_adresse (
    id          TEXT PRIMARY KEY,
    id_adr      TEXT,
    numero      BIGINT,
    rep         TEXT,
    nom_voie    TEXT,
    insee_com   TEXT,
    nom_com     TEXT,
    position    TEXT,
    geom        GEOMETRY(Point, 4326)
);
"""

DDL_LIEN = """
CREATE TABLE IF NOT EXISTS {schema}.ban_lien_adresse_parcelle (
    id_adr      TEXT,
    idu         TEXT,
    type_lien   TEXT,
    nb_adr      DOUBLE PRECISION,
    nb_parc     DOUBLE PRECISION,
    geom        GEOMETRY(LineString, 4326)
);
"""

INDEX_ADRESSE = """
CREATE INDEX IF NOT EXISTS ban_adresse_geom_idx
    ON {schema}.ban_adresse USING GIST (geom);
CREATE INDEX IF NOT EXISTS ban_adresse_insee_idx
    ON {schema}.ban_adresse (insee_com);
"""

INDEX_LIEN = """
CREATE INDEX IF NOT EXISTS ban_lien_geom_idx
    ON {schema}.ban_lien_adresse_parcelle USING GIST (geom);
CREATE INDEX IF NOT EXISTS ban_lien_adr_idx
    ON {schema}.ban_lien_adresse_parcelle (id_adr);
CREATE INDEX IF NOT EXISTS ban_lien_idu_idx
    ON {schema}.ban_lien_adresse_parcelle (idu);
"""

# ---------------------------------------------------------------------------
# Helpers WFS
# ---------------------------------------------------------------------------

def build_wfs_url(typename: str, bbox_3857: tuple, startindex: int = 0) -> str:
    """Construit l'URL WFS avec bbox en EPSG:3857 et pagination."""
    minx, miny, maxx, maxy = bbox_3857
    # WFS 2.0 : bbox=minx,miny,maxx,maxy,srsName
    bbox_str = f"{minx},{miny},{maxx},{maxy},{WFS_SRSNAME}"
    params = {
        "SERVICE": "WFS",
        "VERSION": WFS_VERSION,
        "REQUEST": "GetFeature",
        "TYPENAME": typename,
        "SRSNAME": WFS_SRSNAME,
        "OUTPUTFORMAT": WFS_OUTPUT_FORMAT,
        "COUNT": WFS_COUNT,
        "STARTINDEX": startindex,
        "BBOX": bbox_str,
    }
    return f"{WFS_URL}?{urlencode(params)}"


def fetch_wfs_page(url: str) -> dict:
    """Fetch une page WFS, retourne le GeoJSON parsé."""
    req = Request(url, headers={"Accept": "application/json"})
    with urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_all_features(typename: str, bbox_3857: tuple) -> list[dict]:
    """Fetche toutes les features d'une couche WFS par pagination."""
    all_features = []
    startindex = 0

    for page in range(MAX_PAGES):
        url = build_wfs_url(typename, bbox_3857, startindex)
        log.info("  GET %s (startindex=%d)", typename, startindex)
        data = fetch_wfs_page(url)

        features = data.get("features", [])
        if not features:
            log.info("  → Fin de pagination (page %d, 0 features)", page + 1)
            break

        all_features.extend(features)
        log.info("  → %d features (total: %d)", len(features), len(all_features))

        if len(features) < WFS_COUNT:
            break  # dernière page

        startindex += WFS_COUNT

    return all_features


# ---------------------------------------------------------------------------
# Reprojection 3857 → 4326
# ---------------------------------------------------------------------------
_proj_3857 = pyproj.CRS("EPSG:3857")
_proj_4326 = pyproj.CRS("EPSG:4326")
_transformer_to_4326 = pyproj.Transformer.from_crs(_proj_3857, _proj_4326, always_xy=True)


def reproject_geom_to_4326(geom_shape):
    """Reprojette une géométrie Shapely de 3857 vers 4326."""
    return transform(_transformer_to_4326.transform, geom_shape)


# ---------------------------------------------------------------------------
# Récupération de la commune
# ---------------------------------------------------------------------------

def get_commune_info(conn, schema: str) -> tuple:
    """
    Retourne (polygon_4326: Shapely, bbox_3857: tuple)
    depuis <schema>.commune (colonne geom_2154, EPSG:2154 — voir ingest_commune.py).
    """
    geom_col = COMMUNE_GEOM_COL
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT
                ST_AsText(ST_Transform({geom_col}, 4326)) AS wkt_4326,
                ST_XMin(ST_Transform({geom_col}, 3857)) AS xmin_3857,
                ST_YMin(ST_Transform({geom_col}, 3857)) AS ymin_3857,
                ST_XMax(ST_Transform({geom_col}, 3857)) AS xmax_3857,
                ST_YMax(ST_Transform({geom_col}, 3857)) AS ymax_3857,
                insee,
                nom
            FROM {schema}.commune
            WHERE {geom_col} IS NOT NULL
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"Aucune commune avec {geom_col} dans {schema}.commune "
                f"(lancer ingest_commune.py avant)."
            )
        log.info(
            "Commune chargée : %s (%s)",
            row.get("nom") or "—",
            row.get("insee") or "—",
        )

    polygon_4326 = wkt_loads(row["wkt_4326"])
    bbox_3857 = (
        row["xmin_3857"], row["ymin_3857"],
        row["xmax_3857"], row["ymax_3857"],
    )

    # Petite marge de 10 % pour la bbox WFS
    dx = (bbox_3857[2] - bbox_3857[0]) * 0.05
    dy = (bbox_3857[3] - bbox_3857[1]) * 0.05
    bbox_3857_buffered = (
        bbox_3857[0] - dx, bbox_3857[1] - dy,
        bbox_3857[2] + dx, bbox_3857[3] + dy,
    )

    log.info("Commune bbox (3857) : %.0f %.0f %.0f %.0f", *bbox_3857)
    return polygon_4326, bbox_3857_buffered


# ---------------------------------------------------------------------------
# Filtrage géographique
# ---------------------------------------------------------------------------

def filter_features_by_commune(features: list[dict], commune_poly_4326) -> list[dict]:
    """
    Retourne les features (en 3857) dont la géométrie, reprojetée en 4326,
    intersecte le polygone commune (4326).
    """
    result = []
    for feat in features:
        geom_raw = feat.get("geometry")
        if not geom_raw:
            continue
        try:
            geom_3857 = shape(geom_raw)
            geom_4326 = reproject_geom_to_4326(geom_3857)
            if commune_poly_4326.intersects(geom_4326):
                feat["_geom_4326"] = geom_4326   # on cache la géom convertie
                result.append(feat)
        except Exception as e:
            log.debug("Feature ignorée (erreur géom) : %s", e)
    return result


# ---------------------------------------------------------------------------
# DDL / création des tables
# ---------------------------------------------------------------------------

def create_tables(conn, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute(DDL_ADRESSE.format(schema=schema))
        cur.execute(DDL_LIEN.format(schema=schema))
        cur.execute(INDEX_ADRESSE.format(schema=schema))
        cur.execute(INDEX_LIEN.format(schema=schema))
    conn.commit()
    log.info("Tables créées/vérifiées dans le schéma '%s'", schema)


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def geom_to_ewkt(geom_4326, srid: int = 4326) -> str:
    """Shapely → EWKT pour psycopg."""
    return f"SRID={srid};{geom_4326.wkt}"


def purge_stale_adresses(conn, schema: str, kept_ids: list[str]) -> int:
    """Supprime les adresses du schéma qui ne sont plus dans le flux WFS courant."""
    with conn.cursor() as cur:
        if kept_ids:
            cur.execute(
                f"DELETE FROM {schema}.ban_adresse WHERE NOT (id = ANY(%s))",
                (kept_ids,),
            )
        else:
            cur.execute(f"DELETE FROM {schema}.ban_adresse")
        deleted = cur.rowcount
    conn.commit()
    return deleted


def refresh_adresses(conn, schema: str, features: list[dict], *, purge_stale: bool) -> None:
    """Upsert des adresses BAN, puis purge des ids obsolètes si demandé."""
    sql = f"""
        INSERT INTO {schema}.ban_adresse
            (id, id_adr, numero, rep, nom_voie, insee_com, nom_com, position, geom)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s))
        ON CONFLICT (id) DO UPDATE SET
            id_adr    = EXCLUDED.id_adr,
            numero    = EXCLUDED.numero,
            rep       = EXCLUDED.rep,
            nom_voie  = EXCLUDED.nom_voie,
            insee_com = EXCLUDED.insee_com,
            nom_com   = EXCLUDED.nom_com,
            position  = EXCLUDED.position,
            geom      = EXCLUDED.geom
    """
    rows = []
    for feat in features:
        p = feat.get("properties", {})
        geom_4326 = feat.get("_geom_4326")
        if geom_4326 is None:
            continue
        rows.append((
            p.get("id"),
            p.get("id_adr"),
            p.get("numero"),
            p.get("rep"),
            p.get("nom_voie"),
            p.get("insee_com"),
            p.get("nom_com"),
            p.get("position"),
            geom_to_ewkt(geom_4326),
        ))

    kept_ids: list[str] = []
    with conn.cursor() as cur:
        for row in rows:
            if row[0]:
                kept_ids.append(str(row[0]))
        if rows:
            cur.executemany(sql, rows)
    conn.commit()
    log.info("  → %d adresses upsertées", len(rows))

    if purge_stale:
        removed = purge_stale_adresses(conn, schema, kept_ids)
        log.info("  → %d adresses obsolètes supprimées", removed)


def truncate_liens(conn, schema: str) -> None:
    """Vide la table des liens avant rechargement (schéma = une commune)."""
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.ban_lien_adresse_parcelle")
    conn.commit()


def refresh_liens(conn, schema: str, features: list[dict], *, replace_all: bool) -> None:
    """Recharge les liens adresse/parcelle (TRUNCATE + INSERT si replace_all)."""
    if replace_all:
        truncate_liens(conn, schema)
        log.info("  → table liens vidée avant rechargement")

    sql = f"""
        INSERT INTO {schema}.ban_lien_adresse_parcelle
            (id_adr, idu, type_lien, nb_adr, nb_parc, geom)
        VALUES
            (%s, %s, %s, %s, %s, ST_GeomFromEWKT(%s))
    """
    rows = []
    for feat in features:
        p = feat.get("properties", {})
        geom_4326 = feat.get("_geom_4326")
        if geom_4326 is None:
            continue
        rows.append((
            p.get("id_adr"),
            p.get("idu"),
            p.get("type_lien"),
            p.get("nb_adr"),
            p.get("nb_parc"),
            geom_to_ewkt(geom_4326),
        ))

    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()
    log.info("  → %d liens adresse/parcelle chargés", len(rows))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingestion BAN / BAN-PLUS IGN pour une commune donnée"
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Schéma PostgreSQL = nom de la commune (ex: latresne)",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="DSN PostgreSQL (optionnel ; défaut : SUPABASE_* dans cua_latresne_v4/.env)",
    )
    parser.add_argument(
        "--skip-adresse",
        action="store_true",
        help="Ne pas ingérer la couche BAN-PLUS:adresse",
    )
    parser.add_argument(
        "--skip-lien",
        action="store_true",
        help="Ne pas ingérer la couche BAN-PLUS:lien_adresse_parcelle",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch et filtre les entités sans écrire en base",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Mode historique : pas de purge adresses obsolètes, pas de TRUNCATE liens (risque de doublons)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    schema = args.schema.strip().lower()

    refresh_mode = not args.append

    log.info("=== Ingestion BAN ===")
    log.info("Schéma   : %s", schema)
    log.info("Mode     : %s", "ETL mise à jour" if refresh_mode else "append (legacy)")

    # ── Connexion DB (Supabase via .env) ─────────────────────────────────
    log.info("Connexion à la base de données...")
    conn = connect_supabase(args.db_url)

    try:
        # ── Récupération bbox commune ──────────────────────────────────────
        log.info("Récupération de la géométrie commune depuis %s.commune...", schema)
        commune_poly_4326, bbox_3857 = get_commune_info(conn, schema)

        # ── Création des tables ────────────────────────────────────────────
        if not args.dry_run:
            create_tables(conn, schema)

        # ── Couche adresse ─────────────────────────────────────────────────
        if not args.skip_adresse:
            log.info("--- Couche BAN-PLUS:adresse ---")
            raw_adresses = fetch_all_features("BAN-PLUS:adresse", bbox_3857)
            log.info("Fetched %d features brutes", len(raw_adresses))

            log.info("Filtrage par intersection avec la commune...")
            adresses_filtered = filter_features_by_commune(raw_adresses, commune_poly_4326)
            log.info("%d adresses dans la commune", len(adresses_filtered))

            if not args.dry_run:
                log.info("Mise à jour des adresses en base...")
                refresh_adresses(conn, schema, adresses_filtered, purge_stale=refresh_mode)

        # ── Couche lien adresse/parcelle ───────────────────────────────────
        if not args.skip_lien:
            log.info("--- Couche BAN-PLUS:lien_adresse_parcelle ---")
            raw_liens = fetch_all_features("BAN-PLUS:lien_adresse_parcelle", bbox_3857)
            log.info("Fetched %d features brutes", len(raw_liens))

            log.info("Filtrage par intersection avec la commune...")
            liens_filtered = filter_features_by_commune(raw_liens, commune_poly_4326)
            log.info("%d liens dans la commune", len(liens_filtered))

            if not args.dry_run:
                log.info("Mise à jour des liens adresse/parcelle en base...")
                refresh_liens(conn, schema, liens_filtered, replace_all=refresh_mode)

    finally:
        conn.close()

    log.info("=== Terminé ===")


if __name__ == "__main__":
    main()