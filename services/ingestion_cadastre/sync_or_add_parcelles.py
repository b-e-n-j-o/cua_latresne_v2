#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_or_add_parcelles.py

Synchronise les parcelles Etalab d'une commune vers <schema>.parcelles.

Usage:
  python sync_or_add_parcelles.py --insee 66008
  python sync_or_add_parcelles.py --insee 33234 --insert
  python sync_or_add_parcelles.py --insee 66008 --schema argeles --insert
  python sync_or_add_parcelles.py --insee 66008 --dry-run

Le schéma cible est dérivé du référentiel INSEE (v_commune_2025.csv) à partir
du code --insee, sauf si --schema est fourni explicitement.

Par défaut (sans --insert): diff uniquement + JSON de rapport + notification Slack.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import geopandas as gpd
import requests
from shapely.geometry import shape
from sqlalchemy import create_engine, text

from env_loader import BACKEND_ROOT, load_project_env

load_project_env()

DEFAULT_COMMUNES_CSV = str(BACKEND_ROOT / "config" / "v_commune_2025.csv")
TARGET_TABLE = "parcelles"
SURFACE_DIFF_SEUIL = 10.0
CONTENANCE_DIFF_SEUIL = 1.0
BATCH_SIZE = 500
SLACK_WEBHOOK = (
    os.getenv("SLACK_WEBHOOK")
    or os.getenv("SLACK_WEBHOOK_URL")
    or os.getenv("SLACK_DEPLOY_WEBHOOK")
    or ""
).strip()


@dataclass
class SyncConfig:
    code_insee: str
    target_schema: str
    commune_label: str
    db_full_table: str
    output_json: str

    @classmethod
    def build(
        cls,
        code_insee: str,
        target_schema: str,
        commune_label: str,
    ) -> SyncConfig:
        schema = sanitize_schema(target_schema)
        return cls(
            code_insee=code_insee,
            target_schema=schema,
            commune_label=commune_label,
            db_full_table=f"{schema}.{TARGET_TABLE}",
            output_json=f"diff_parcelles_{code_insee}_{schema}_{TARGET_TABLE}.json",
        )


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
        raise ValueError("--insee doit contenir exactement 5 caractères (ex: 33234, 2A004).")
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


def ensure_schema_and_table(db_url_str: str, cfg: SyncConfig) -> None:
    """Crée le schéma et la table cible si absents."""
    engine = create_engine(db_url_str)
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
    print(f"[OK] Structure assurée: {full}")


def fetch_etalab(code_insee: str) -> gpd.GeoDataFrame:
    print(f"[1/4] Téléchargement Etalab pour {code_insee}...")
    r = requests.get(
        f"https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{code_insee}/geojson/parcelles",
        timeout=60,
    )
    r.raise_for_status()
    fc = r.json()

    rows = []
    for f in fc["features"]:
        p = f["properties"]
        rows.append(
            {
                "idu": p["id"],
                "commune": p["commune"],
                "section": p["section"],
                "numero": p["numero"],
                "contenance": p.get("contenance"),
                "geometry": shape(f["geometry"]),
            }
        )

    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326").to_crs("EPSG:2154")
    print(f"  -> {len(gdf)} parcelles Etalab")
    return gdf


def fetch_db(db_url_str: str, cfg: SyncConfig) -> gpd.GeoDataFrame:
    print(f"[2/4] Lecture de {cfg.db_full_table}...")
    engine = create_engine(db_url_str)
    q = text(
        f"""
        SELECT
            idu,
            numero,
            section,
            contenance,
            code_insee,
            ST_AsGeoJSON(geom_2154)::json AS geometry
        FROM {cfg.db_full_table}
        WHERE idu IS NOT NULL
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()

    records = []
    for row in rows:
        records.append(
            {
                "idu": row.idu,
                "numero": row.numero,
                "section": row.section,
                "contenance": row.contenance,
                "code_insee": row.code_insee,
                "geometry": shape(row.geometry) if row.geometry is not None else None,
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
            },
            geometry=gpd.GeoSeries([], crs="EPSG:2154"),
            crs="EPSG:2154",
        )
    else:
        gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:2154")
    print(f"  -> {len(gdf)} parcelles en base")
    return gdf


def diff(etalab: gpd.GeoDataFrame, db: gpd.GeoDataFrame, cfg: SyncConfig) -> dict:
    print("[3/4] Calcul du diff...")
    idu_etalab = set(etalab["idu"])
    idu_db = set(db["idu"])

    nouveaux = sorted(idu_etalab - idu_db)
    supprimes = sorted(idu_db - idu_etalab)
    communs = idu_etalab & idu_db

    et_idx = etalab.set_index("idu")
    db_idx = db.set_index("idu")

    contenance_diff = []
    geom_diff = []

    for idu in communs:
        et_row = et_idx.loc[idu]
        db_row = db_idx.loc[idu]

        et_cont = et_row["contenance"]
        db_cont = db_row["contenance"]
        if et_cont is not None and db_cont is not None:
            ecart_cont = float(et_cont) - float(db_cont)
            if abs(ecart_cont) > CONTENANCE_DIFF_SEUIL:
                contenance_diff.append(
                    {
                        "idu": idu,
                        "contenance_etalab": et_cont,
                        "contenance_db": db_cont,
                        "ecart_m2": round(ecart_cont, 2),
                    }
                )

        et_geom = et_row["geometry"]
        db_geom = db_row["geometry"]
        if et_geom and db_geom and et_geom.is_valid and db_geom.is_valid:
            et_area = et_geom.area
            db_area = db_geom.area
            ecart = abs(et_area - db_area)
            if ecart > SURFACE_DIFF_SEUIL:
                geom_diff.append(
                    {
                        "idu": idu,
                        "area_etalab": round(et_area, 2),
                        "area_db": round(db_area, 2),
                        "ecart_m2": round(ecart, 2),
                    }
                )

    contenance_diff.sort(key=lambda x: abs(x["ecart_m2"]), reverse=True)
    geom_diff.sort(key=lambda x: x["ecart_m2"], reverse=True)

    idu_maj_communs = sorted({d["idu"] for d in contenance_diff} | {d["idu"] for d in geom_diff})

    return {
        "code_insee": cfg.code_insee,
        "commune": cfg.commune_label,
        "schema": cfg.target_schema,
        "table_cible": cfg.db_full_table,
        "total_etalab": len(idu_etalab),
        "total_db": len(idu_db),
        "total_communs": len(communs),
        "nouveaux": {"count": len(nouveaux), "idu": nouveaux},
        "supprimes": {"count": len(supprimes), "idu": supprimes},
        "contenance_diff": {"count": len(contenance_diff), "details": contenance_diff[:50]},
        "geom_diff": {"count": len(geom_diff), "details": geom_diff[:50]},
        "idu_maj_communs": idu_maj_communs,
    }


def format_rapport_text(result: dict) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append(f"  DIFF PARCELLES — {result['code_insee']} ({result.get('commune', '')})")
    lines.append("=" * 60)
    lines.append(f"  Schéma          : {result.get('schema', '')}")
    lines.append(f"  Table cible     : {result['table_cible']}")
    lines.append(f"  Etalab          : {result['total_etalab']} parcelles")
    lines.append(f"  Base            : {result['total_db']} parcelles")
    lines.append(f"  Communs         : {result['total_communs']} parcelles")
    lines.append("")
    lines.append(f"  Nouveaux (Etalab - Base) : {result['nouveaux']['count']}")
    lines.append(f"  Supprimés (Base - Etalab): {result['supprimes']['count']}")
    lines.append(
        f"  Contenance diff (> {CONTENANCE_DIFF_SEUIL} m2): "
        f"{result['contenance_diff']['count']}"
    )
    lines.append(
        f"  Géométrie diff (> {SURFACE_DIFF_SEUIL} m2): "
        f"{result['geom_diff']['count']}"
    )
    lines.append(f"  IDU à réaligner : {len(result.get('idu_maj_communs') or [])}")
    lines.append("=" * 60)
    return "\n".join(lines)


def notify_slack_diff(
    result: dict,
    cfg: SyncConfig,
    *,
    dry_run: bool = False,
    apply_stats: Optional[dict] = None,
) -> None:
    if dry_run:
        print("Info: mode --dry-run, notification Slack ignorée.")
        return
    if not SLACK_WEBHOOK:
        print("Info: webhook Slack absent, notification ignorée.")
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
            f"\n\nBase mise à jour (--insert): "
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
                    "footer": f"{cfg.db_full_table} | JSON: {cfg.output_json}",
                }
            ],
        }

    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        resp.raise_for_status()
        print("Notification Slack envoyée.")
    except Exception as e:
        print(f"Alerte: envoi Slack échoué: {e}")


def etalab_row_to_values(idu: str, row, code_insee: str) -> dict:
    geom_2154 = row["geometry"]

    numero = row.get("numero")
    section = row.get("section")
    contenance = row.get("contenance")
    commune = row.get("commune")

    numero = None if numero is None else str(numero)
    section = None if section is None else str(section)
    try:
        contenance = None if contenance is None else float(contenance)
    except Exception:
        contenance = None
    code_insee_val = str(commune) if commune is not None else code_insee

    vals = {
        "idu": idu,
        "numero": numero,
        "section": section,
        "contenance": contenance,
        "code_insee": code_insee_val,
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
            (idu, numero, section, contenance, code_insee, geom_2154)
        VALUES
            (:idu, :numero, :section, :contenance, :code_insee,
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
        print(f"  -> Staging batch {batch_idx}/{total_batches}: {len(batch)} lignes")
    return loaded


def apply_etalab_to_postgres(
    db_url_str: str,
    etalab: gpd.GeoDataFrame,
    result: dict,
    cfg: SyncConfig,
) -> dict:
    print("[4/4] Application en base (--insert)...")
    stats = {"inserted": 0, "updated": 0, "deleted": 0, "errors": 0}
    if not (
        result["nouveaux"]["count"]
        or result["supprimes"]["count"]
        or result["contenance_diff"]["count"]
        or result["geom_diff"]["count"]
    ):
        print("  -> Rien à écrire (diff vide).")
        return stats

    et_idx = etalab.set_index("idu")
    engine = create_engine(db_url_str)
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
            print(f"    ! PREP STAGING {idu}: {e}")
            stats["errors"] += 1

    try:
        print(f"  -> Création staging: {staging_table}")
        create_staging_table(engine, staging_table)
        loaded = load_staging_data(engine, staging_table, staging_rows)
        print(f"  -> Staging chargée: {loaded} lignes")

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
                (idu, numero, section, contenance, code_insee, geom_2154, geom_3857)
            SELECT
                s.idu, s.numero, s.section, s.contenance, s.code_insee,
                s.geom_2154,
                ST_Transform(s.geom_2154, 3857)
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

        with engine.begin() as conn:
            stats["deleted"] = conn.execute(delete_sql).rowcount or 0
            stats["inserted"] = conn.execute(insert_sql).rowcount or 0
            stats["updated"] = conn.execute(update_sql).rowcount or 0
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
        print("  -> Staging supprimée")

    print(
        f"  -> Insérées {stats['inserted']}, maj {stats['updated']}, "
        f"supprimées {stats['deleted']}, erreurs {stats['errors']}"
    )
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync parcelles Etalab -> <schema>.parcelles (schéma dérivé du CSV INSEE)"
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
        "--insert",
        action="store_true",
        help="Applique les changements en base",
    )
    parser.add_argument(
        "--no-slack",
        action="store_true",
        help="Pas de notification Slack (ex. orchestré par run_etl_commune.py)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    code_insee = sanitize_insee(args.insee)
    commune_info = lookup_commune(args.csv, code_insee)

    schema = args.schema.strip() if args.schema else commune_info["schema"]
    cfg = SyncConfig.build(
        code_insee=code_insee,
        target_schema=schema,
        commune_label=commune_info["label"],
    )

    print(
        f"Commune: {cfg.commune_label} (INSEE {cfg.code_insee}, dép. {commune_info['dep']}) "
        f"→ schéma {cfg.target_schema}"
    )

    url = db_url()
    ensure_schema_and_table(url, cfg)

    etalab = fetch_etalab(cfg.code_insee)
    db = fetch_db(url, cfg)
    result = diff(etalab, db, cfg)
    print("\n" + format_rapport_text(result))

    with open(cfg.output_json, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Rapport JSON: {cfg.output_json}")

    apply_stats = None
    if args.insert and not args.dry_run:
        apply_stats = apply_etalab_to_postgres(url, etalab, result, cfg)
    elif args.insert and args.dry_run:
        print("Info: --insert ignoré car --dry-run actif.")

    if not args.no_slack:
        notify_slack_diff(result, cfg, dry_run=args.dry_run, apply_stats=apply_stats)


if __name__ == "__main__":
    main()
