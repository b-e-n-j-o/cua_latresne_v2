#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_or_add_parcelles.py

Compare le cadastre Etalab d'une commune avec <schema>.parcelles (latest).

Architecture :
  - parcelles              → carte à l'écran (dernière version Etalab)
  - cadastre_photos        → une ligne par photo d'archive
  - parcelles_archives     → copie complète des parcelles à cette date
  - cadastre_veille_runs / cadastre_veille_evenements → divisions, fusions, etc.

Par défaut : lecture seule (diff + veille). Aucune écriture sur parcelles.

Usage :
  PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles --insee 66008 --schema argeles
  PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles --insee 66008 --schema argeles --ensure-schema
  PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles --insee 66008 --schema argeles --apply
  PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles --insee 66008 --schema argeles --apply --millesime 2026-06-01

--apply : s'il y a un vrai delta, prend une photo complète de parcelles,
puis met à jour latest, date les parcelles (created/updated Etalab + millésime PCI),
et enregistre la veille. Les colonnes sig_* des IDU conservés ne sont pas touchées.
Les IDU disparus sont dans l'archive (avec leur SIG) avant DELETE.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import unicodedata
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import shape
from sqlalchemy import create_engine, text

try:
    from api.veille_sig.cadastre.cadastre_schema import lookup_objects_ddl
except ImportError:
    lookup_objects_ddl = None

try:
    from api.veille_sig._env import BACKEND_ROOT, load_project_env
except ImportError:
    from services.ingestion_cadastre.env_loader import BACKEND_ROOT, load_project_env

load_project_env()

log = logging.getLogger("diff_parcelles")

SCRIPT_DIR = Path(__file__).resolve().parent
REPORTS_DIR = SCRIPT_DIR / "reports"
DEFAULT_COMMUNES_CSV = str(BACKEND_ROOT / "config" / "v_commune_2025.csv")
TARGET_TABLE = "parcelles"
ARCHIVE_TABLE = "parcelles_archives"
PHOTOS_TABLE = "cadastre_photos"
VEILLE_RUNS_TABLE = "cadastre_veille_runs"
VEILLE_EVENTS_TABLE = "cadastre_veille_evenements"
SURFACE_DIFF_SEUIL = 10.0
CONTENANCE_DIFF_SEUIL = 1.0
# Reprojection 4326→2154 décale les sommets de ~1 cm : en dessous, ce n'est pas un vrai changement.
HAUSDORFF_DIFF_SEUIL = 0.5
# Recouvrement min pour relier un IDU disparu à un IDU nouveau (filtrage des slivers de limite).
OVERLAP_MIN_M2 = 0.5
OVERLAP_MIN_RATIO = 0.12
BATCH_SIZE = 500
ETALAB_URL = (
    "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles"
)
ETALAB_CADASTRE_INDEX = "https://cadastre.data.gouv.fr/data/etalab-cadastre/"
PCI_VECTEUR_INDEX = "https://cadastre.data.gouv.fr/data/dgfip-pci-vecteur/"
SLACK_WEBHOOK = (
    os.getenv("SLACK_WEBHOOK")
    or os.getenv("SLACK_WEBHOOK_URL")
    or os.getenv("SLACK_DEPLOY_WEBHOOK")
    or ""
).strip()
SLACK_PREVIEW = 20


@dataclass
class SyncConfig:
    code_insee: str
    target_schema: str
    commune_label: str
    db_full_table: str
    archive_full_table: str
    photos_full_table: str
    veille_runs_full_table: str
    veille_events_full_table: str
    output_json: Path
    output_json_ts: Path
    log_file: Path

    @classmethod
    def build(
        cls,
        code_insee: str,
        target_schema: str,
        commune_label: str,
        run_id: str,
    ) -> SyncConfig:
        schema = sanitize_schema(target_schema)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        stem = f"diff_parcelles_{code_insee}_{schema}_{TARGET_TABLE}"
        return cls(
            code_insee=code_insee,
            target_schema=schema,
            commune_label=commune_label,
            db_full_table=f"{schema}.{TARGET_TABLE}",
            archive_full_table=f"{schema}.{ARCHIVE_TABLE}",
            photos_full_table=f"{schema}.{PHOTOS_TABLE}",
            veille_runs_full_table=f"{schema}.{VEILLE_RUNS_TABLE}",
            veille_events_full_table=f"{schema}.{VEILLE_EVENTS_TABLE}",
            output_json=SCRIPT_DIR / f"{stem}.json",
            output_json_ts=REPORTS_DIR / f"{stem}_{run_id}.json",
            log_file=REPORTS_DIR / f"{stem}_{run_id}.log",
        )


def setup_logging(log_file: Path) -> None:
    log.handlers.clear()
    log.setLevel(logging.INFO)
    log.propagate = False
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(ch)


def db_url() -> str:
    return (
        f"postgresql://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}"
        f"@{os.getenv('SUPABASE_HOST')}:{os.getenv('SUPABASE_PORT')}/{os.getenv('SUPABASE_DB')}"
    )


def sanitize_schema(schema: str) -> str:
    s = (schema or "").strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", s):
        raise ValueError(
            "Nom de schéma invalide. Utiliser [a-z0-9_] et commencer par une lettre/underscore."
        )
    return s


def sanitize_insee(insee: str) -> str:
    code = (insee or "").strip()
    if not re.fullmatch(r"[0-9A-Za-z]{5}", code):
        raise ValueError("--insee doit contenir exactement 5 caractères (ex: 33234, 66008).")
    return code.upper()


def strip_accents(value: str) -> str:
    s = unicodedata.normalize("NFKD", value or "")
    return "".join(c for c in s if not unicodedata.combining(c))


def slugify(name: str) -> str:
    """Ex: 'Saint-Émilion' -> 'saint_emilion'."""
    if not name:
        return ""
    name = strip_accents(name).lower()
    cleaned = []
    for c in name:
        cleaned.append(c if c.isalnum() else "_")
    slug = "".join(cleaned).strip("_")
    return "_".join(filter(None, slug.split("_")))


def schema_from_commune_row(row: dict) -> str:
    """
    Dérive un nom de schéma PostgreSQL court et stable.

    Priorité au champ NCC (ex: 'ARGELES SUR MER' -> argeles),
    repli sur NCCENR slugifié.
    """
    ncc = (row.get("NCC") or "").strip()
    nccenr = (row.get("NCCENR") or row.get("LIBELLE") or "").strip()

    if ncc:
        first = strip_accents(ncc).split()[0].lower()
        first = re.sub(r"[^a-z0-9]", "", first)
        if first:
            return sanitize_schema(first)

    slug = slugify(nccenr)
    if not slug:
        raise ValueError("Impossible de dériver un nom de schéma depuis le référentiel commune.")
    return sanitize_schema(slug.split("_")[0] if "_" in slug else slug)


def lookup_commune(csv_path: str, code_insee: str) -> dict:
    """Retourne la ligne CSV pour le code INSEE (TYPECOM=COM)."""
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(f"Référentiel communes introuvable: {csv_path}")

    matches: list[dict] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            com = (row.get("COM") or "").strip().upper()
            if com != code_insee:
                continue
            typecom = (row.get("TYPECOM") or "COM").strip().upper()
            if typecom and typecom != "COM":
                continue
            matches.append(row)

    if not matches:
        raise ValueError(f"Code INSEE {code_insee!r} introuvable dans {csv_path}")
    if len(matches) > 1:
        raise ValueError(f"Code INSEE {code_insee!r} ambigu ({len(matches)} lignes dans le CSV).")

    row = matches[0]
    label = (row.get("LIBELLE") or row.get("NCCENR") or row.get("NCC") or code_insee).strip()
    return {
        "insee": code_insee,
        "label": label,
        "dep": (row.get("DEP") or "").strip(),
        "schema": schema_from_commune_row(row),
        "row": row,
    }


def engine_from_url(db_url_str: str):
    return create_engine(db_url_str)


def table_exists(engine, schema: str, table: str) -> bool:
    q = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :table
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        return conn.execute(q, {"schema": schema, "table": table}).first() is not None


def list_columns(engine, schema: str, table: str) -> list[str]:
    q = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(q, {"schema": schema, "table": table})]


def sig_columns(columns: list[str]) -> list[str]:
    return [c for c in columns if c.startswith("sig_")]


def ensure_schema_and_table(db_url_str: str, cfg: SyncConfig) -> None:
    """Crée le schéma et la table cible si absents (uniquement en écriture)."""
    engine = engine_from_url(db_url_str)
    schema = cfg.target_schema
    full = cfg.db_full_table
    ddl = text(
        f"""
        CREATE SCHEMA IF NOT EXISTS {schema};

        CREATE TABLE IF NOT EXISTS {full} (
            id BIGSERIAL PRIMARY KEY,
            idu TEXT UNIQUE NOT NULL,
            numero TEXT,
            section TEXT,
            contenance DOUBLE PRECISION,
            code_insee TEXT,
            geom_2154 geometry(MultiPolygon, 2154),
            geom_3857 geometry(MultiPolygon, 3857)
        );

        CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_idu
            ON {full} (idu);
        CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_code_insee
            ON {full} (code_insee);
        CREATE INDEX IF NOT EXISTS idx_{TARGET_TABLE}_geom_2154_gist
            ON {full} USING GIST (geom_2154);
        """
    )
    with engine.begin() as conn:
        conn.execute(ddl)
    log.info("Structure assurée: %s", full)


def ensure_latest_columns(engine, cfg: SyncConfig) -> None:
    """Ajoute les colonnes de datation sur parcelles (latest)."""
    full = cfg.db_full_table
    stmts = [
        f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS created_etalab DATE",
        f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS updated_etalab DATE",
        f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS millesime_pci DATE",
        f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ",
    ]
    with engine.begin() as conn:
        for sql in stmts:
            conn.execute(text(sql))
    log.info("Colonnes de millésime assurées sur %s", full)


def ensure_cadastre_architecture(db_url_str: str, cfg: SyncConfig) -> None:
    """Photos + archives + veille. Ne modifie aucune ligne de parcelles."""
    engine = engine_from_url(db_url_str)
    schema = cfg.target_schema
    photos = cfg.photos_full_table
    archives = cfg.archive_full_table
    runs = cfg.veille_runs_full_table
    events = cfg.veille_events_full_table

    if table_exists(engine, schema, ARCHIVE_TABLE):
        cols = list_columns(engine, schema, ARCHIVE_TABLE)
        if "photo_id" not in cols:
            raise RuntimeError(
                f"{archives} existe déjà sans photo_id (ancien schéma). "
                "La renommer avant de relancer, ex: ALTER TABLE ... RENAME TO parcelles_archives_old."
            )

    ddl = text(
        f"""
        CREATE SCHEMA IF NOT EXISTS {schema};

        CREATE TABLE IF NOT EXISTS {photos} (
            id UUID PRIMARY KEY,
            archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            millesime_pci DATE,
            millesime_suivant DATE,
            motif TEXT NOT NULL,
            nb_parcelles INTEGER NOT NULL,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS {archives} (
            archive_id BIGSERIAL PRIMARY KEY,
            photo_id UUID NOT NULL REFERENCES {photos} (id) ON DELETE CASCADE,
            idu TEXT NOT NULL,
            numero TEXT,
            section TEXT,
            contenance DOUBLE PRECISION,
            code_insee TEXT,
            created_etalab DATE,
            updated_etalab DATE,
            millesime_pci DATE,
            imported_at TIMESTAMPTZ,
            geom_2154 geometry(MultiPolygon, 2154),
            geom_3857 geometry(MultiPolygon, 3857),
            sig_payload JSONB
        );

        CREATE INDEX IF NOT EXISTS idx_{ARCHIVE_TABLE}_photo
            ON {archives} (photo_id);
        CREATE INDEX IF NOT EXISTS idx_{ARCHIVE_TABLE}_idu
            ON {archives} (idu);
        CREATE INDEX IF NOT EXISTS idx_{ARCHIVE_TABLE}_geom_2154_gist
            ON {archives} USING GIST (geom_2154);

        CREATE TABLE IF NOT EXISTS {runs} (
            id UUID PRIMARY KEY,
            photo_id UUID REFERENCES {photos} (id),
            run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            millesime_pci DATE,
            mode TEXT,
            total_etalab INTEGER,
            total_db INTEGER,
            counts JSONB,
            rapport JSONB,
            apply_stats JSONB
        );

        CREATE TABLE IF NOT EXISTS {events} (
            id BIGSERIAL PRIMARY KEY,
            run_id UUID NOT NULL REFERENCES {runs} (id) ON DELETE CASCADE,
            type TEXT NOT NULL,
            nb_parents INTEGER,
            nb_enfants INTEGER,
            parents JSONB,
            enfants JSONB,
            liens JSONB
        );

        CREATE INDEX IF NOT EXISTS idx_{VEILLE_EVENTS_TABLE}_run
            ON {events} (run_id);
        CREATE INDEX IF NOT EXISTS idx_{VEILLE_EVENTS_TABLE}_type
            ON {events} (type);
        """
    )
    with engine.begin() as conn:
        conn.execute(ddl)
        if lookup_objects_ddl is not None:
            conn.execute(text(lookup_objects_ddl(schema)))
    ensure_latest_columns(engine, cfg)
    log.info(
        "Architecture cadastre assurée: %s | %s | %s | %s",
        photos,
        archives,
        runs,
        events,
    )


def ensure_archive_table(db_url_str: str, cfg: SyncConfig) -> None:
    """Alias conservé : crée l'architecture d'archive sans toucher aux lignes."""
    ensure_cadastre_architecture(db_url_str, cfg)


def _as_float(value) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_iso_date(value) -> Optional[date]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return None
    return date.fromisoformat(s)


def detect_millesime_pci() -> Optional[date]:
    """Dernier millésime publié (dossier Etalab / PCI DGFiP)."""
    for url in (ETALAB_CADASTRE_INDEX, PCI_VECTEUR_INDEX):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            found = re.findall(r"/(\d{4}-\d{2}-\d{2})/", r.text)
            if found:
                mill = max(date.fromisoformat(d) for d in found)
                log.info("Millésime PCI détecté (%s): %s", url, mill.isoformat())
                return mill
            log.warning("Aucun millésime YYYY-MM-DD dans %s", url)
        except Exception as e:
            log.warning("Détection millésime via %s échouée: %s", url, e)
    return None


def millesime_en_base(engine, cfg: SyncConfig, columns: list[str]) -> Optional[date]:
    if "millesime_pci" not in columns:
        return None
    q = text(
        f"""
        SELECT millesime_pci
        FROM {cfg.db_full_table}
        WHERE millesime_pci IS NOT NULL
        GROUP BY millesime_pci
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """
    )
    with engine.connect() as conn:
        row = conn.execute(q).first()
    return row[0] if row else None


def has_material_change(result: dict) -> bool:
    v = (result.get("veille") or {}).get("counts") or {}
    return bool(
        result["nouveaux"]["count"]
        or result["supprimes"]["count"]
        or result["contenance_diff"]["count"]
        or result["geom_diff"]["count"]
        or any(v.get(k) for k in v)
    )


def sig_payload_sql_expr(sig_cols: list[str]) -> str:
    if not sig_cols:
        return "NULL::jsonb"
    pairs = ", ".join(f"'{c}', t.{c}" for c in sig_cols)
    return f"jsonb_strip_nulls(jsonb_build_object({pairs}))"


def fetch_etalab(code_insee: str) -> tuple[gpd.GeoDataFrame, dict]:
    url = ETALAB_URL.format(insee=code_insee)
    log.info("[1/4] Téléchargement Etalab %s", url)
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    fc = r.json()
    features = fc.get("features") or []
    log.info("  GeoJSON reçu: %s features", len(features))

    rows = []
    updated_dates: list[str] = []
    created_dates: list[str] = []
    for f in features:
        p = f.get("properties") or {}
        geom = f.get("geometry")
        updated = p.get("updated")
        created = p.get("created")
        if updated:
            updated_dates.append(str(updated)[:10])
        if created:
            created_dates.append(str(created)[:10])
        rows.append(
            {
                "idu": p.get("id"),
                "commune": p.get("commune"),
                "section": p.get("section"),
                "numero": p.get("numero"),
                "contenance": p.get("contenance"),
                "updated": updated,
                "created": created,
                "geometry": shape(geom) if geom else None,
            }
        )

    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    before = len(gdf)
    gdf = gdf[gdf["idu"].notna()].copy()
    gdf = gdf.drop_duplicates(subset=["idu"], keep="first")
    if len(gdf) != before:
        log.warning("  Etalab: %s lignes ignorées (idu null ou doublon)", before - len(gdf))
    gdf = gdf.to_crs("EPSG:2154")
    log.info("  -> %s parcelles Etalab uniques", len(gdf))

    millesime = {
        "source": "etalab",
        "url": url,
        "updated_min": min(updated_dates) if updated_dates else None,
        "updated_max": max(updated_dates) if updated_dates else None,
        "updated_distinct": sorted(set(updated_dates)),
        "created_min": min(created_dates) if created_dates else None,
        "created_max": max(created_dates) if created_dates else None,
        "nb_features_with_updated": len(updated_dates),
    }
    log.info(
        "  Millésime Etalab updated: min=%s max=%s (%s dates distinctes, %s/%s features)",
        millesime["updated_min"],
        millesime["updated_max"],
        len(millesime["updated_distinct"]),
        millesime["nb_features_with_updated"],
        len(gdf),
    )
    return gdf, millesime


def fetch_db(db_url_str: str, cfg: SyncConfig, columns: list[str]) -> gpd.GeoDataFrame:
    log.info("[2/4] Lecture de %s (colonnes cadastre uniquement)", cfg.db_full_table)
    extra_sig = sig_columns(columns)
    if extra_sig:
        log.info("  Colonnes SIG détectées (conservées, non comparées): %s", ", ".join(extra_sig))

    has_sig = bool(extra_sig)
    sig_flag_sql = ""
    if has_sig:
        tests = " OR ".join(f"{col} IS NOT NULL" for col in extra_sig)
        sig_flag_sql = f", ({tests}) AS has_sig"

    engine = engine_from_url(db_url_str)
    q = text(
        f"""
        SELECT
            idu,
            numero,
            section,
            contenance,
            code_insee,
            ST_AsGeoJSON(geom_2154)::json AS geometry
            {sig_flag_sql}
        FROM {cfg.db_full_table}
        WHERE idu IS NOT NULL
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()

    records = []
    for row in rows:
        mapping = row._mapping
        geom_raw = mapping["geometry"]
        records.append(
            {
                "idu": mapping["idu"],
                "numero": mapping["numero"],
                "section": mapping["section"],
                "contenance": mapping["contenance"],
                "code_insee": mapping["code_insee"],
                "has_sig": bool(mapping["has_sig"]) if has_sig else False,
                "geometry": shape(geom_raw) if geom_raw is not None else None,
            }
        )
    if not records:
        gdf = gpd.GeoDataFrame(
            {
                "idu": [],
                "numero": [],
                "section": [],
                "contenance": [],
                "code_insee": [],
                "has_sig": [],
            },
            geometry=gpd.GeoSeries([], crs="EPSG:2154"),
            crs="EPSG:2154",
        )
    else:
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:2154")
        before = len(gdf)
        gdf = gdf.drop_duplicates(subset=["idu"], keep="first")
        if len(gdf) != before:
            log.warning("  Base: %s doublons idu ignorés", before - len(gdf))

    log.info("  -> %s parcelles en base", len(gdf))
    if len(gdf):
        insee_counts = Counter(
            (str(v) if v is not None and not pd.isna(v) else "(null)") for v in gdf["code_insee"]
        )
        log.info("  code_insee en base: %s", dict(insee_counts))
        unexpected = [k for k in insee_counts if k not in (cfg.code_insee, "(null)")]
        if unexpected:
            log.warning(
                "  Des parcelles ont un code_insee différent de %s: %s",
                cfg.code_insee,
                unexpected,
            )
        if has_sig:
            n_sig = int(gdf["has_sig"].sum())
            log.info("  Parcelles avec au moins un champ SIG non null: %s", n_sig)
    return gdf


def _geom_valid(geom):
    if geom is None or geom.is_empty:
        return None
    if geom.is_valid:
        return geom
    try:
        fixed = geom.buffer(0)
        if fixed and not fixed.is_empty:
            return fixed
    except Exception:
        return None
    return None


def idu_ref(idu: str) -> str:
    """66008000AL0050 -> AL:0050"""
    s = str(idu or "")
    if len(s) >= 14:
        return f"{s[8:10]}:{s[10:14]}"
    return s


def _series_get(row, col, default=None):
    try:
        if col not in row.index:
            return default
    except Exception:
        return default
    v = row[col]
    if v is None or pd.isna(v):
        return default
    return v


def snapshot_parcelle(idu: str, row, origin: str) -> dict:
    geom = _geom_valid(row["geometry"])
    updated = _series_get(row, "updated")
    created = _series_get(row, "created")
    return {
        "idu": idu,
        "ref": idu_ref(idu),
        "section": None if _series_get(row, "section") is None else str(_series_get(row, "section")),
        "numero": None if _series_get(row, "numero") is None else str(_series_get(row, "numero")),
        "contenance": _as_float(_series_get(row, "contenance")),
        "area_m2": round(float(geom.area), 1) if geom is not None else None,
        "has_sig": bool(_series_get(row, "has_sig", False)),
        "created": str(created)[:10] if created else None,
        "updated": str(updated)[:10] if updated else None,
        "origin": origin,
    }


def _sindex_query(gdf: gpd.GeoDataFrame, geom) -> list[int]:
    if gdf.empty or geom is None or geom.is_empty:
        return []
    sidx = gdf.sindex
    if hasattr(sidx, "query"):
        try:
            res = sidx.query(geom, predicate="intersects")
            return list(res) if res is not None else []
        except Exception:
            pass
    return list(sidx.intersection(geom.bounds))


def overlap_links(
    gdf_old: gpd.GeoDataFrame,
    gdf_new: gpd.GeoDataFrame,
    role_old: str,
    role_new: str,
) -> list[dict]:
    """Paires old→new dont le recouvrement dépasse les seuils (pas le même IDU)."""
    if gdf_old.empty or gdf_new.empty:
        return []
    old = gdf_old[gdf_old.geometry.notna()].copy().reset_index(drop=True)
    new = gdf_new[gdf_new.geometry.notna()].copy().reset_index(drop=True)
    if old.empty or new.empty:
        return []

    iterate_old = len(old) <= len(new)
    links: list[dict] = []
    src, tgt = (old, new) if iterate_old else (new, old)
    for i, row in src.iterrows():
        geom_s = _geom_valid(row.geometry)
        if geom_s is None:
            continue
        idu_s = row["idu"]
        for j in _sindex_query(tgt, geom_s):
            geom_t = _geom_valid(tgt.geometry.iloc[j])
            if geom_t is None:
                continue
            idu_t = tgt.at[j, "idu"]
            if iterate_old:
                idu_old, idu_new, geom_o, geom_n = idu_s, idu_t, geom_s, geom_t
            else:
                idu_old, idu_new, geom_o, geom_n = idu_t, idu_s, geom_t, geom_s
            if idu_old == idu_new:
                continue
            try:
                inter = geom_o.intersection(geom_n)
            except Exception:
                continue
            if inter is None or inter.is_empty:
                continue
            area_i = float(inter.area)
            area_o = float(geom_o.area) or 0.0
            area_n = float(geom_n.area) or 0.0
            if area_i < OVERLAP_MIN_M2:
                continue
            pct_old = area_i / area_o if area_o else 0.0
            pct_new = area_i / area_n if area_n else 0.0
            if max(pct_old, pct_new) < OVERLAP_MIN_RATIO:
                continue
            links.append(
                {
                    "idu_old": idu_old,
                    "idu_new": idu_new,
                    "role_old": role_old,
                    "role_new": role_new,
                    "area_old": round(area_o, 1),
                    "area_new": round(area_n, 1),
                    "area_inter": round(area_i, 1),
                    "pct_old": round(pct_old, 3),
                    "pct_new": round(pct_new, 3),
                }
            )
    return links


def _classify_component(old_ids: set[str], new_ids: set[str]) -> str:
    only_old = old_ids - new_ids
    only_new = new_ids - old_ids
    shared = old_ids & new_ids
    n_old, n_new = len(old_ids), len(new_ids)
    if shared and only_new and not only_old and n_old == 1:
        return "division"
    if shared and only_old and not only_new and n_new == 1:
        return "fusion"
    if shared:
        return "remaniement"
    if n_old == 1 and n_new >= 2:
        return "division"
    if n_old >= 2 and n_new == 1:
        return "fusion"
    if n_old == 1 and n_new == 1:
        return "recodage"
    return "remaniement"


def classify_veille_parcellaire(
    etalab: gpd.GeoDataFrame,
    db: gpd.GeoDataFrame,
    nouveaux: list[str],
    supprimes: list[str],
    communs: set[str],
) -> dict:
    """Relie IDU disparus / apparus par recouvrement géométrique."""
    log.info("[3b] Veille parcellaire (fusions / divisions / recodages)...")
    gone_db = db[db["idu"].isin(supprimes)].copy()
    born_et = etalab[etalab["idu"].isin(nouveaux)].copy()
    stayed_db = db[db["idu"].isin(communs)].copy()
    stayed_et = etalab[etalab["idu"].isin(communs)].copy()

    links = []
    links.extend(overlap_links(gone_db, born_et, "gone", "born"))
    links.extend(overlap_links(gone_db, stayed_et, "gone", "stayed"))
    links.extend(overlap_links(stayed_db, born_et, "stayed", "born"))
    log.info("  Liens de recouvrement significatifs: %s", len(links))

    parent = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    involved = set()
    old_of: dict[str, set[str]] = defaultdict(set)
    new_of: dict[str, set[str]] = defaultdict(set)
    links_of: dict[str, list[dict]] = defaultdict(list)

    for lk in links:
        union(lk["idu_old"], lk["idu_new"])
        involved.add(lk["idu_old"])
        involved.add(lk["idu_new"])

    for lk in links:
        root = find(lk["idu_old"])
        old_of[root].add(lk["idu_old"])
        new_of[root].add(lk["idu_new"])
        links_of[root].append(lk)

    et_idx = etalab.set_index("idu")
    db_idx = db.set_index("idu")

    def snap_old(idu: str) -> dict:
        origin = "base_supprimee" if idu in set(supprimes) else "base_conservee"
        return snapshot_parcelle(idu, db_idx.loc[idu], origin)

    def snap_new(idu: str) -> dict:
        origin = "etalab_nouvelle" if idu in set(nouveaux) else "etalab_conservee"
        return snapshot_parcelle(idu, et_idx.loc[idu], origin)

    evenements = []
    used_gone: set[str] = set()
    used_born: set[str] = set()

    for root, old_ids in old_of.items():
        new_ids = new_of[root]
        kind = _classify_component(old_ids, new_ids)
        used_gone.update(old_ids & set(supprimes))
        used_born.update(new_ids & set(nouveaux))
        ev_links = links_of[root]
        cover_old = []
        for oid in sorted(old_ids):
            inter = sum(lk["area_inter"] for lk in ev_links if lk["idu_old"] == oid)
            area = next((lk["area_old"] for lk in ev_links if lk["idu_old"] == oid), None)
            cover_old.append(
                round(inter / area, 3) if area else None
            )
        evenements.append(
            {
                "type": kind,
                "parents": [snap_old(i) for i in sorted(old_ids) if i in db_idx.index],
                "enfants": [snap_new(i) for i in sorted(new_ids) if i in et_idx.index],
                "liens": ev_links,
                "nb_parents": len(old_ids),
                "nb_enfants": len(new_ids),
            }
        )

    suppressions = []
    for idu in supprimes:
        if idu in used_gone:
            continue
        suppressions.append(snap_old(idu))
        evenements.append(
            {
                "type": "suppression",
                "parents": [snap_old(idu)],
                "enfants": [],
                "liens": [],
                "nb_parents": 1,
                "nb_enfants": 0,
            }
        )

    creations = []
    for idu in nouveaux:
        if idu in used_born:
            continue
        creations.append(snap_new(idu))
        evenements.append(
            {
                "type": "creation",
                "parents": [],
                "enfants": [snap_new(idu)],
                "liens": [],
                "nb_parents": 0,
                "nb_enfants": 1,
            }
        )

    order = {
        "division": 0,
        "fusion": 1,
        "recodage": 2,
        "remaniement": 3,
        "suppression": 4,
        "creation": 5,
    }
    evenements.sort(key=lambda e: (order.get(e["type"], 9), e["parents"][0]["idu"] if e["parents"] else e["enfants"][0]["idu"]))

    counts = Counter(e["type"] for e in evenements)
    for kind in ("division", "fusion", "recodage", "remaniement", "suppression", "creation"):
        log.info("  %s: %s", kind.capitalize().ljust(12), counts.get(kind, 0))

    for ev in evenements:
        parents = ", ".join(p["ref"] for p in ev["parents"]) or "—"
        enfants = ", ".join(c["ref"] for c in ev["enfants"]) or "—"
        log.info("    %s: %s  →  %s", ev["type"], parents, enfants)

    return {
        "seuils": {"overlap_min_m2": OVERLAP_MIN_M2, "overlap_min_ratio": OVERLAP_MIN_RATIO},
        "counts": {
            "division": counts.get("division", 0),
            "fusion": counts.get("fusion", 0),
            "recodage": counts.get("recodage", 0),
            "remaniement": counts.get("remaniement", 0),
            "suppression": counts.get("suppression", 0),
            "creation": counts.get("creation", 0),
        },
        "evenements": evenements,
    }


def format_veille_text(result: dict) -> str:
    veille = result.get("veille") or {}
    counts = veille.get("counts") or {}
    lines = [
        "=" * 64,
        f"  VEILLE PARCELLAIRE — {result.get('code_insee')} ({result.get('commune', '')})",
        "=" * 64,
        f"  Divisions       : {counts.get('division', 0)}",
        f"  Fusions         : {counts.get('fusion', 0)}",
        f"  Recodages 1:1   : {counts.get('recodage', 0)}",
        f"  Remaniements    : {counts.get('remaniement', 0)}",
        f"  Suppressions    : {counts.get('suppression', 0)}",
        f"  Créations       : {counts.get('creation', 0)}",
        "",
    ]
    labels = {
        "division": "DIVISION (1 parent → n enfants)",
        "fusion": "FUSION (n parents → 1 enfant)",
        "recodage": "RECODAGE (1 → 1, nouvel IDU)",
        "remaniement": "REMANIEMENT (n → m)",
        "suppression": "SUPPRESSION NETTE (aucun successeur)",
        "creation": "CRÉATION NETTE (aucun prédécesseur)",
    }
    for kind, title in labels.items():
        evs = [e for e in (veille.get("evenements") or []) if e["type"] == kind]
        if not evs:
            continue
        lines.append(f"--- {title} ({len(evs)}) ---")
        for ev in evs:
            def fmt(p):
                area = f"{p['area_m2']} m²" if p.get("area_m2") is not None else "?"
                sig = " SIG" if p.get("has_sig") else ""
                return f"{p['ref']} ({area}{sig})"

            parents = ", ".join(fmt(p) for p in ev["parents"]) or "—"
            enfants = ", ".join(fmt(c) for c in ev["enfants"]) or "—"
            lines.append(f"  {parents}")
            lines.append(f"      → {enfants}")
        lines.append("")
    lines.append("=" * 64)
    return "\n".join(lines)


def diff(etalab: gpd.GeoDataFrame, db: gpd.GeoDataFrame, cfg: SyncConfig, millesime: dict) -> dict:
    log.info("[3/4] Calcul du diff (aucun écriture sur %s)", cfg.db_full_table)
    idu_etalab = set(etalab["idu"])
    idu_db = set(db["idu"])

    nouveaux = sorted(idu_etalab - idu_db)
    supprimes = sorted(idu_db - idu_etalab)
    communs = idu_etalab & idu_db

    et_idx = etalab.set_index("idu")
    db_idx = db.set_index("idu")

    contenance_diff = []
    geom_diff = []
    geom_invalides = []

    for idu in communs:
        et_row = et_idx.loc[idu]
        db_row = db_idx.loc[idu]

        et_cont = _as_float(et_row["contenance"])
        db_cont = _as_float(db_row["contenance"])
        if et_cont is None and db_cont is None:
            pass
        elif et_cont is None or db_cont is None:
            contenance_diff.append(
                {
                    "idu": idu,
                    "contenance_etalab": et_cont,
                    "contenance_db": db_cont,
                    "ecart_m2": None,
                    "note": "valeur manquante d'un côté",
                }
            )
        else:
            ecart_cont = et_cont - db_cont
            if abs(ecart_cont) > CONTENANCE_DIFF_SEUIL:
                contenance_diff.append(
                    {
                        "idu": idu,
                        "contenance_etalab": et_cont,
                        "contenance_db": db_cont,
                        "ecart_m2": round(ecart_cont, 2),
                    }
                )

        et_geom = _geom_valid(et_row["geometry"])
        db_geom = _geom_valid(db_row["geometry"])
        if et_geom is None or db_geom is None:
            geom_invalides.append(idu)
            continue

        et_area = et_geom.area
        db_area = db_geom.area
        ecart = abs(et_area - db_area)
        hausdorff = None
        try:
            hausdorff = float(et_geom.hausdorff_distance(db_geom))
        except Exception:
            hausdorff = None

        if ecart <= SURFACE_DIFF_SEUIL and (
            hausdorff is None or hausdorff <= HAUSDORFF_DIFF_SEUIL
        ):
            continue

        geom_diff.append(
            {
                "idu": idu,
                "area_etalab": round(et_area, 2),
                "area_db": round(db_area, 2),
                "ecart_m2": round(ecart, 2),
                "hausdorff_m": round(hausdorff, 3) if hausdorff is not None else None,
            }
        )

    contenance_diff.sort(key=lambda x: abs(x["ecart_m2"] or 0), reverse=True)
    geom_diff.sort(
        key=lambda x: (x["ecart_m2"], x["hausdorff_m"] or 0),
        reverse=True,
    )

    idu_maj_communs = sorted({d["idu"] for d in contenance_diff} | {d["idu"] for d in geom_diff})

    supprimes_avec_sig = []
    if "has_sig" in db.columns and supprimes:
        db_sig = db.set_index("idu")
        for idu in supprimes:
            if bool(db_sig.loc[idu]["has_sig"]):
                supprimes_avec_sig.append(idu)

    log.info("  Communs         : %s", len(communs))
    log.info("  Nouveaux (Etalab - base) : %s", len(nouveaux))
    log.info("  Supprimés (base - Etalab): %s", len(supprimes))
    if supprimes_avec_sig:
        log.warning(
            "  Parmi les supprimés, %s ont des champs SIG renseignés (perdus si DELETE)",
            len(supprimes_avec_sig),
        )
    log.info(
        "  Contenance diff (> %s m2) : %s",
        CONTENANCE_DIFF_SEUIL,
        len(contenance_diff),
    )
    log.info(
        "  Géométrie diff (surface > %s m2 ou hausdorff > %s m) : %s",
        SURFACE_DIFF_SEUIL,
        HAUSDORFF_DIFF_SEUIL,
        len(geom_diff),
    )
    if geom_invalides:
        log.warning("  Géométries invalides ignorées: %s", len(geom_invalides))
    if nouveaux:
        log.info("  Exemples nouveaux: %s", ", ".join(nouveaux[:SLACK_PREVIEW]))
    if supprimes:
        log.info("  Exemples supprimés: %s", ", ".join(supprimes[:SLACK_PREVIEW]))
    if geom_diff:
        log.info(
            "  Top écarts geom: %s",
            ", ".join(
                f"{d['idu']} (aire {d['ecart_m2']} m2, H {d['hausdorff_m']} m)"
                for d in geom_diff[:10]
            ),
        )

    veille = classify_veille_parcellaire(etalab, db, nouveaux, supprimes, communs)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "mode": "compare_only",
        "code_insee": cfg.code_insee,
        "commune": cfg.commune_label,
        "schema": cfg.target_schema,
        "table_cible": cfg.db_full_table,
        "millesime_etalab": millesime,
        "note_date_base": (
            "millesime_pci / created_etalab / updated_etalab sur parcelles "
            "après --ensure-schema et --apply."
        ),
        "seuils": {
            "contenance_m2": CONTENANCE_DIFF_SEUIL,
            "surface_m2": SURFACE_DIFF_SEUIL,
            "hausdorff_m": HAUSDORFF_DIFF_SEUIL,
        },
        "total_etalab": len(idu_etalab),
        "total_db": len(idu_db),
        "total_communs": len(communs),
        "nouveaux": {"count": len(nouveaux), "idu": nouveaux},
        "supprimes": {
            "count": len(supprimes),
            "idu": supprimes,
            "avec_sig": {"count": len(supprimes_avec_sig), "idu": supprimes_avec_sig},
        },
        "contenance_diff": {"count": len(contenance_diff), "details": contenance_diff},
        "geom_diff": {"count": len(geom_diff), "details": geom_diff},
        "geom_invalides": {"count": len(geom_invalides), "idu": geom_invalides},
        "idu_maj_communs": idu_maj_communs,
        "veille": veille,
        "log_file": str(cfg.log_file),
        "rapport_json": str(cfg.output_json_ts),
    }


def format_rapport_text(result: dict) -> str:
    mill = result.get("millesime_etalab") or {}
    lines = [
        "=" * 64,
        f"  DIFF PARCELLES — {result['code_insee']} ({result.get('commune', '')})",
        "=" * 64,
        f"  Mode            : {result.get('mode', 'compare_only')} (pas de remplacement)",
        f"  Schéma          : {result.get('schema', '')}",
        f"  Table cible     : {result['table_cible']}",
        f"  Etalab updated  : {mill.get('updated_min')} → {mill.get('updated_max')}",
        f"  Millésime PCI   : {result.get('millesime_pci') or 'non détecté'}",
        f"  Millésime base  : {result.get('millesime_base') or 'inconnu'}",
        f"  Etalab          : {result['total_etalab']} parcelles",
        f"  Base            : {result['total_db']} parcelles",
        f"  Communs         : {result['total_communs']} parcelles",
        "",
        f"  Nouveaux (Etalab - Base) : {result['nouveaux']['count']}",
        f"  Supprimés (Base - Etalab): {result['supprimes']['count']}",
        f"    dont avec SIG          : {result['supprimes'].get('avec_sig', {}).get('count', 0)}",
        (
            f"  Contenance diff (> {CONTENANCE_DIFF_SEUIL} m2): "
            f"{result['contenance_diff']['count']}"
        ),
        (
            f"  Géométrie diff (> {SURFACE_DIFF_SEUIL} m2 ou hausdorff > {HAUSDORFF_DIFF_SEUIL} m): "
            f"{result['geom_diff']['count']}"
        ),
        f"  IDU à réaligner : {len(result.get('idu_maj_communs') or [])}",
        "",
        "  VEILLE",
        f"    Divisions     : {(result.get('veille') or {}).get('counts', {}).get('division', 0)}",
        f"    Fusions       : {(result.get('veille') or {}).get('counts', {}).get('fusion', 0)}",
        f"    Recodages 1:1 : {(result.get('veille') or {}).get('counts', {}).get('recodage', 0)}",
        f"    Remaniements  : {(result.get('veille') or {}).get('counts', {}).get('remaniement', 0)}",
        f"    Suppressions  : {(result.get('veille') or {}).get('counts', {}).get('suppression', 0)}",
        f"    Créations     : {(result.get('veille') or {}).get('counts', {}).get('creation', 0)}",
        "=" * 64,
    ]
    return "\n".join(lines)


def notify_slack_diff(
    result: dict,
    cfg: SyncConfig,
    *,
    dry_run: bool = False,
    apply_stats: Optional[dict] = None,
) -> None:
    if dry_run:
        log.info("Mode --dry-run, notification Slack ignorée.")
        return
    if not SLACK_WEBHOOK:
        log.info("Webhook Slack absent, notification ignorée.")
        return

    date_str = datetime.now().strftime("%d/%m/%Y à %Hh%M")
    n_new = result["nouveaux"]["count"]
    n_del = result["supprimes"]["count"]
    n_cont = result["contenance_diff"]["count"]
    n_geom = result["geom_diff"]["count"]
    has_ecart = n_new or n_del or n_cont or n_geom

    apply_note = ""
    if apply_stats:
        apply_note = (
            f"\n\nBase mise à jour (--apply, photo d'abord): "
            f"+{apply_stats.get('inserted', 0)} / "
            f"maj {apply_stats.get('updated', 0)} / "
            f"-{apply_stats.get('deleted', 0)}"
        )
        if apply_stats.get("errors"):
            apply_note += f" | erreurs: {apply_stats['errors']}"

    rapport = format_rapport_text(result)
    body = f"```{rapport}```{apply_note}"
    commune = result.get("commune") or cfg.commune_label

    if not has_ecart:
        payload = {
            "text": (
                f"OK Diff Etalab vs base — {commune} — INSEE {result['code_insee']} — {date_str}"
            ),
            "attachments": [
                {
                    "color": "good",
                    "mrkdwn_in": ["text"],
                    "text": body,
                    "footer": cfg.db_full_table,
                }
            ],
        }
    else:
        color = "warning" if (n_del or n_geom) else "#439FE0"
        payload = {
            "text": (
                f"Diff Etalab vs base — {commune} — INSEE {result['code_insee']} — {date_str}"
            ),
            "attachments": [
                {
                    "color": color,
                    "mrkdwn_in": ["text"],
                    "text": body,
                    "footer": f"{cfg.db_full_table} | JSON: {cfg.output_json_ts.name}",
                }
            ],
        }

    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Notification Slack envoyée.")
    except Exception as e:
        log.warning("Envoi Slack échoué: %s", e)


def etalab_row_to_values(idu: str, row, code_insee: str) -> dict:
    geom_2154 = row["geometry"]

    numero = _series_get(row, "numero")
    section = _series_get(row, "section")
    contenance = _as_float(_series_get(row, "contenance"))
    commune = _series_get(row, "commune")

    numero = None if numero is None else str(numero)
    section = None if section is None else str(section)
    code_insee_val = str(commune) if commune is not None else code_insee

    vals = {
        "idu": idu,
        "numero": numero,
        "section": section,
        "contenance": contenance,
        "code_insee": code_insee_val,
        "created_etalab": parse_iso_date(_series_get(row, "created")),
        "updated_etalab": parse_iso_date(_series_get(row, "updated")),
        "geom_2154_wkt": None,
    }

    if geom_2154 is not None and not geom_2154.is_empty:
        vals["geom_2154_wkt"] = geom_2154.wkt
    return vals


def create_staging_table(engine, staging_table: str) -> None:
    sql = text(
        f"""
        CREATE TABLE {staging_table} (
            idu TEXT PRIMARY KEY,
            numero TEXT,
            section TEXT,
            contenance DOUBLE PRECISION,
            code_insee TEXT,
            created_etalab DATE,
            updated_etalab DATE,
            geom_2154 geometry(MultiPolygon, 2154)
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(sql)


def load_staging_data(engine, staging_table: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    insert_staging_sql = text(
        f"""
        INSERT INTO {staging_table}
            (idu, numero, section, contenance, code_insee,
             created_etalab, updated_etalab, geom_2154)
        VALUES
            (:idu, :numero, :section, :contenance, :code_insee,
             :created_etalab, :updated_etalab,
             ST_Multi(ST_GeomFromText(:geom_2154_wkt, 2154)))
        """
    )

    loaded = 0
    total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx, i in enumerate(range(0, len(rows), BATCH_SIZE), 1):
        batch = rows[i : i + BATCH_SIZE]
        with engine.begin() as conn:
            conn.execute(insert_staging_sql, batch)
        loaded += len(batch)
        log.info("  -> Staging batch %s/%s: %s lignes", batch_idx, total_batches, len(batch))
    return loaded


def snapshot_parcelles(
    engine,
    cfg: SyncConfig,
    *,
    millesime_actuel: Optional[date],
    millesime_suivant: Optional[date],
    motif: str,
    note: str,
    sig_cols: list[str],
) -> tuple[str, int]:
    """Photo complète de parcelles → cadastre_photos + parcelles_archives."""
    photo_id = str(uuid.uuid4())
    payload_expr = sig_payload_sql_expr(sig_cols)
    date_cols = list_columns(engine, cfg.target_schema, TARGET_TABLE)
    created_sel = "created_etalab" if "created_etalab" in date_cols else "NULL"
    updated_sel = "updated_etalab" if "updated_etalab" in date_cols else "NULL"
    mill_sel = "millesime_pci" if "millesime_pci" in date_cols else "NULL"
    imported_sel = "imported_at" if "imported_at" in date_cols else "NULL"

    insert_photo = text(
        f"""
        INSERT INTO {cfg.photos_full_table}
            (id, millesime_pci, millesime_suivant, motif, nb_parcelles, note)
        VALUES
            (:id, :millesime_pci, :millesime_suivant, :motif, 0, :note)
        """
    )
    insert_rows = text(
        f"""
        INSERT INTO {cfg.archive_full_table} (
            photo_id, idu, numero, section, contenance, code_insee,
            created_etalab, updated_etalab, millesime_pci, imported_at,
            geom_2154, geom_3857, sig_payload
        )
        SELECT
            :photo_id, t.idu, t.numero, t.section, t.contenance, t.code_insee,
            {created_sel}, {updated_sel}, {mill_sel}, {imported_sel},
            t.geom_2154, t.geom_3857, {payload_expr}
        FROM {cfg.db_full_table} t
        """
    )
    count_sql = text(
        f"SELECT COUNT(*) FROM {cfg.archive_full_table} WHERE photo_id = :photo_id"
    )
    update_nb = text(
        f"UPDATE {cfg.photos_full_table} SET nb_parcelles = :nb WHERE id = :id"
    )

    with engine.begin() as conn:
        conn.execute(
            insert_photo,
            {
                "id": photo_id,
                "millesime_pci": millesime_actuel,
                "millesime_suivant": millesime_suivant,
                "motif": motif,
                "note": note,
            },
        )
        conn.execute(insert_rows, {"photo_id": photo_id})
        nb = conn.execute(count_sql, {"photo_id": photo_id}).scalar() or 0
        conn.execute(update_nb, {"nb": nb, "id": photo_id})

    log.info(
        "Photo d'archive %s : %s parcelles (millésime %s → %s)",
        photo_id,
        nb,
        millesime_actuel or "inconnu",
        millesime_suivant or "?",
    )
    return photo_id, int(nb)


def persist_veille(
    engine,
    cfg: SyncConfig,
    result: dict,
    *,
    photo_id: Optional[str],
    millesime_pci: Optional[date],
    apply_stats: Optional[dict],
) -> str:
    run_id = str(uuid.uuid4())
    counts = (result.get("veille") or {}).get("counts") or {}
    evenements = (result.get("veille") or {}).get("evenements") or []
    insert_run = text(
        f"""
        INSERT INTO {cfg.veille_runs_full_table} (
            id, photo_id, millesime_pci, mode, total_etalab, total_db,
            counts, rapport, apply_stats
        )
        VALUES (
            :id, :photo_id, :millesime_pci, :mode, :total_etalab, :total_db,
            CAST(:counts AS jsonb), CAST(:rapport AS jsonb), CAST(:apply_stats AS jsonb)
        )
        """
    )
    insert_ev = text(
        f"""
        INSERT INTO {cfg.veille_events_full_table} (
            run_id, type, nb_parents, nb_enfants, parents, enfants, liens
        )
        VALUES (
            :run_id, :type, :nb_parents, :nb_enfants,
            CAST(:parents AS jsonb), CAST(:enfants AS jsonb), CAST(:liens AS jsonb)
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_run,
            {
                "id": run_id,
                "photo_id": photo_id,
                "millesime_pci": millesime_pci,
                "mode": result.get("mode"),
                "total_etalab": result.get("total_etalab"),
                "total_db": result.get("total_db"),
                "counts": json.dumps(counts, ensure_ascii=False),
                "rapport": json.dumps(result, ensure_ascii=False, default=str),
                "apply_stats": json.dumps(apply_stats or {}, ensure_ascii=False, default=str),
            },
        )
        for ev in evenements:
            conn.execute(
                insert_ev,
                {
                    "run_id": run_id,
                    "type": ev.get("type"),
                    "nb_parents": ev.get("nb_parents") or 0,
                    "nb_enfants": ev.get("nb_enfants") or 0,
                    "parents": json.dumps(ev.get("parents") or [], ensure_ascii=False, default=str),
                    "enfants": json.dumps(ev.get("enfants") or [], ensure_ascii=False, default=str),
                    "liens": json.dumps(ev.get("liens") or [], ensure_ascii=False, default=str),
                },
            )
    log.info("Veille enregistrée: %s (%s événements)", run_id, len(evenements))
    return run_id


def apply_etalab_to_postgres(
    db_url_str: str,
    etalab: gpd.GeoDataFrame,
    result: dict,
    cfg: SyncConfig,
    *,
    millesime_pci: Optional[date],
    imported_at: datetime,
) -> dict:
    log.info("[4/4] Mise à jour latest %s (millésime %s)", cfg.db_full_table, millesime_pci)
    stats = {"inserted": 0, "updated": 0, "deleted": 0, "errors": 0, "dated": 0}
    et_idx = etalab.set_index("idu")
    engine = engine_from_url(db_url_str)
    staging_table = f"{cfg.target_schema}._stg_parcelles_{uuid.uuid4().hex[:10]}"
    full = cfg.db_full_table

    staging_rows = []
    for idu in etalab["idu"]:
        try:
            v = etalab_row_to_values(idu, et_idx.loc[idu], cfg.code_insee)
            if not v["geom_2154_wkt"]:
                raise ValueError("geom_2154 indisponible")
            staging_rows.append(v)
        except Exception as e:
            log.warning("    ! PREP STAGING %s: %s", idu, e)
            stats["errors"] += 1

    try:
        log.info("  -> Création staging: %s", staging_table)
        create_staging_table(engine, staging_table)
        loaded = load_staging_data(engine, staging_table, staging_rows)
        log.info("  -> Staging chargée: %s lignes", loaded)

        delete_sql = text(
            f"""
            DELETE FROM {full} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {staging_table} s WHERE s.idu = t.idu
            )
            """
        )
        insert_sql = text(
            f"""
            INSERT INTO {full}
                (idu, numero, section, contenance, code_insee,
                 geom_2154, geom_3857,
                 created_etalab, updated_etalab, millesime_pci, imported_at)
            SELECT
                s.idu, s.numero, s.section, s.contenance, s.code_insee,
                s.geom_2154,
                ST_Transform(s.geom_2154, 3857),
                s.created_etalab, s.updated_etalab,
                :millesime_pci, :imported_at
            FROM {staging_table} s
            LEFT JOIN {full} t ON t.idu = s.idu
            WHERE t.idu IS NULL
            """
        )
        update_sql = text(
            f"""
            UPDATE {full} t
            SET
                numero = s.numero,
                section = s.section,
                contenance = s.contenance,
                code_insee = s.code_insee,
                geom_2154 = s.geom_2154,
                geom_3857 = ST_Transform(s.geom_2154, 3857)
            FROM {staging_table} s
            WHERE t.idu = s.idu
              AND (
                    t.numero IS DISTINCT FROM s.numero
                 OR t.section IS DISTINCT FROM s.section
                 OR t.contenance IS DISTINCT FROM s.contenance
                 OR t.code_insee IS DISTINCT FROM s.code_insee
                 OR NOT ST_Equals(t.geom_2154, s.geom_2154)
              )
            """
        )
        stamp_sql = text(
            f"""
            UPDATE {full} t
            SET
                created_etalab = s.created_etalab,
                updated_etalab = s.updated_etalab,
                millesime_pci = :millesime_pci,
                imported_at = :imported_at
            FROM {staging_table} s
            WHERE t.idu = s.idu
            """
        )
        params = {"millesime_pci": millesime_pci, "imported_at": imported_at}

        with engine.begin() as conn:
            stats["deleted"] = conn.execute(delete_sql).rowcount or 0
            stats["inserted"] = conn.execute(insert_sql, params).rowcount or 0
            stats["updated"] = conn.execute(update_sql).rowcount or 0
            stats["dated"] = conn.execute(stamp_sql, params).rowcount or 0
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        log.info("  -> Staging supprimée")

    log.info(
        "  -> Insérées %s, maj géom %s, supprimées %s, datées %s, erreurs %s",
        stats["inserted"],
        stats["updated"],
        stats["deleted"],
        stats["dated"],
        stats["errors"],
    )
    if stats["inserted"] or stats["deleted"] or stats["updated"]:
        log.warning(
            "  SIG: les IDU conservés gardent leur JSON ; les nouveaux n'ont pas de SIG. "
            "Relancer l'enrichissement (intersections) sur les IDU touchés."
        )
    return stats


def write_reports(result: dict, cfg: SyncConfig) -> None:
    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    cfg.output_json.write_text(payload, encoding="utf-8")
    cfg.output_json_ts.write_text(payload, encoding="utf-8")
    veille_txt = format_veille_text(result)
    veille_path = cfg.output_json_ts.with_name(cfg.output_json_ts.stem + "_veille.txt")
    veille_path.write_text(veille_txt, encoding="utf-8")
    log.info("Rapport JSON: %s", cfg.output_json)
    log.info("Rapport horodaté: %s", cfg.output_json_ts)
    log.info("Rapport veille: %s", veille_path)
    log.info("Log fichier: %s", cfg.log_file)
    log.info("\n%s", veille_txt)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Diff Etalab vs <schema>.parcelles (lecture seule par défaut). "
            "Photo d'archive + mise à jour latest uniquement avec --apply."
        )
    )
    parser.add_argument(
        "--insee",
        required=True,
        help="Code INSEE commune (5 caractères), ex: 33234, 66008, 2A004",
    )
    parser.add_argument(
        "--schema",
        default=None,
        help="Schéma PostgreSQL cible (sinon dérivé du CSV, ex: argeles, latresne)",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Libellé commune (si --schema est fourni, le CSV n'est plus obligatoire)",
    )
    parser.add_argument(
        "--csv",
        default=DEFAULT_COMMUNES_CSV,
        help="Chemin vers v_commune_2025.csv",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Diff uniquement, pas d'écriture DB ni notification Slack",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "S'il y a un delta métier : photo complète de parcelles, "
            "puis mise à jour latest + datation + enregistrement de la veille"
        ),
    )
    parser.add_argument(
        "--insert",
        action="store_true",
        help="Alias de --apply (archive d'abord, puis latest)",
    )
    parser.add_argument(
        "--force-insert",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--ensure-schema",
        action="store_true",
        help="Crée colonnes de dates + tables photos/archives/veille, sans modifier les lignes de parcelles",
    )
    parser.add_argument(
        "--ensure-archive-table",
        action="store_true",
        help="Alias de --ensure-schema",
    )
    parser.add_argument(
        "--millesime",
        default=None,
        help="Millésime PCI (YYYY-MM-DD). Sinon détecté sur cadastre.data.gouv.fr",
    )
    parser.add_argument(
        "--no-slack",
        action="store_true",
        help="Pas de notification Slack (ex. orchestré par run_etl_commune.py)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    code_insee = sanitize_insee(args.insee)
    schema_arg = (args.schema or "").strip() or None
    label_arg = (args.label or "").strip() or None
    csv_available = Path(args.csv).is_file()

    if schema_arg and (label_arg or not csv_available):
        commune_info = {
            "insee": code_insee,
            "label": label_arg or schema_arg,
            "dep": code_insee[:2] if code_insee[:2].isdigit() else "",
            "schema": schema_arg,
            "row": {},
        }
    else:
        commune_info = lookup_commune(args.csv, code_insee)

    schema = schema_arg or commune_info["schema"]
    cfg = SyncConfig.build(
        code_insee=code_insee,
        target_schema=schema,
        commune_label=label_arg or commune_info["label"],
        run_id=run_id,
    )
    setup_logging(cfg.log_file)

    want_apply = bool(args.apply or args.insert)
    want_schema = bool(args.ensure_schema or args.ensure_archive_table or want_apply)
    mode = "APPLY" if want_apply and not args.dry_run else "COMPARE"
    log.info(
        "Commune: %s (INSEE %s, dép. %s) → schéma %s",
        cfg.commune_label,
        cfg.code_insee,
        commune_info["dep"],
        cfg.target_schema,
    )
    log.info("Run %s — mode %s", run_id, mode)

    url = db_url()
    engine = engine_from_url(url)

    if want_schema and args.dry_run:
        log.info("DDL (--ensure-schema / --apply) ignoré car --dry-run.")
    elif want_schema:
        if want_apply:
            ensure_schema_and_table(url, cfg)
        if not table_exists(engine, cfg.target_schema, TARGET_TABLE):
            log.error("Table absente: %s", cfg.db_full_table)
            raise SystemExit(1)
        ensure_cadastre_architecture(url, cfg)
        if not want_apply:
            log.info("Schéma assuré. Arrêt (--ensure-schema sans --apply, pas de diff).")
            return

    if not table_exists(engine, cfg.target_schema, TARGET_TABLE):
        log.error("Table absente: %s — rien à comparer.", cfg.db_full_table)
        raise SystemExit(1)

    columns = list_columns(engine, cfg.target_schema, TARGET_TABLE)
    if not (want_apply and not args.dry_run):
        log.info("Lecture seule de %s — aucune écriture sur cette table.", cfg.db_full_table)

    millesime_pci = parse_iso_date(args.millesime) if args.millesime else detect_millesime_pci()
    millesime_base = millesime_en_base(engine, cfg, columns)
    log.info("Millésime PCI Etalab: %s | millésime en base: %s", millesime_pci, millesime_base)

    etalab, millesime = fetch_etalab(cfg.code_insee)
    millesime["millesime_pci"] = millesime_pci.isoformat() if millesime_pci else None
    db = fetch_db(url, cfg, columns)
    result = diff(etalab, db, cfg, millesime)
    result["millesime_pci"] = millesime_pci.isoformat() if millesime_pci else None
    result["millesime_base"] = millesime_base.isoformat() if millesime_base else None
    if want_apply and not args.dry_run:
        result["mode"] = "apply"
    log.info("\n%s", format_rapport_text(result))
    write_reports(result, cfg)

    apply_stats = None
    if want_apply and args.dry_run:
        log.info("--apply ignoré car --dry-run actif.")
    elif want_apply:
        if not has_material_change(result):
            log.info("Aucun delta métier : pas de photo, pas de réécriture de parcelles.")
        else:
            extra_sig = sig_columns(columns)
            photo_id, nb_photo = snapshot_parcelles(
                engine,
                cfg,
                millesime_actuel=millesime_base,
                millesime_suivant=millesime_pci,
                motif="avant_maj" if millesime_base else "etat_initial",
                note=(
                    f"Photo avant passage au millésime {millesime_pci} "
                    f"({cfg.commune_label} {cfg.code_insee})"
                ),
                sig_cols=extra_sig,
            )
            result["photo_id"] = photo_id
            result["photo_nb_parcelles"] = nb_photo
            apply_stats = apply_etalab_to_postgres(
                url,
                etalab,
                result,
                cfg,
                millesime_pci=millesime_pci,
                imported_at=datetime.now(),
            )
            result["apply_stats"] = apply_stats
            veille_id = persist_veille(
                engine,
                cfg,
                result,
                photo_id=photo_id,
                millesime_pci=millesime_pci,
                apply_stats=apply_stats,
            )
            result["veille_run_id"] = veille_id
            write_reports(result, cfg)

    if not args.no_slack:
        notify_slack_diff(result, cfg, dry_run=args.dry_run, apply_stats=apply_stats)

    log.info("Terminé.")


if __name__ == "__main__":
    main()
