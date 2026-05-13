"""
Liste les dalles IGN MNT et LiDAR HD intersectant la géométrie de la commune
stockée dans Supabase (table argeles.commune, une seule ligne).

Par défaut : références, URLs, estimation de taille (25 Mo/dalle MNT,
125 Mo/dalle LiDAR). Avec --telecharger : téléchargement IGN puis envoi vers
Supabase Storage (buckets dalles-lidar et mnt-dalles), avec journalisation
de la progression et du volume téléversé.

Variables Storage (--telecharger) : SUPABASE_URL et
SUPABASE_SERVICE_ROLE_KEY ou SERVICE_KEY (rôle service). Les variables
SUPABASE_* DB restent nécessaires pour la géométrie commune.

API / pipeline : voir ``services.ingestion_mnt_lidar.router_ingestion_mnt_lidar`` (POST /admin/mnt-lidar/ingest).

Exemples CLI :
    python lister_dalles_de_argeles.py
    python lister_dalles_de_argeles.py --discover
    python lister_dalles_de_argeles.py --export-json /tmp/dalles.json --export-csv /tmp/dalles.csv
    python lister_dalles_de_argeles.py --telecharger --lidar
    python lister_dalles_de_argeles.py --telecharger --mnt --lidar --download-timeout 900
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

from urllib.parse import urlparse, unquote

import geopandas as gpd
import psycopg2
import requests
from dotenv import load_dotenv
from shapely import wkb
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


IGN_WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
IGN_LIDAR_TILES_LAYER = "IGNF_NUAGES-DE-POINTS-LIDAR-HD:dalle"
SRS = "EPSG:2154"

# Pattern d'URL pour le téléchargement MNT LiDAR HD via WMS GetMap (1km x 1km, 0.5m)
MNT_WMS_URL_TEMPLATE = (
    "https://data.geopf.fr/wms-r?SERVICE=WMS&VERSION=1.3.0&EXCEPTIONS=text/xml"
    "&REQUEST=GetMap&LAYERS=IGNF_LIDAR-HD_MNT_ELEVATION.ELEVATIONGRIDCOVERAGE.LAMB93"
    "&FORMAT=image/geotiff&STYLES=&CRS=EPSG:2154"
    "&BBOX={minx},{miny},{maxx},{maxy}&WIDTH=2000&HEIGHT=2000"
    "&FILENAME={filename}"
)

SIZE_MNT_MB = 25.0
SIZE_LIDAR_MB = 125.0

BUCKET_LIDAR = "dalles-lidar"
BUCKET_MNT = "mnt-dalles"

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Tile:
    layer_kind: str
    id: str | None
    name: str | None
    name_download: str | None
    url: str
    estimated_size_mb: float


def load_supabase_env() -> dict:
    load_dotenv()
    cfg = {
        "host": os.getenv("SUPABASE_HOST"),
        "user": os.getenv("SUPABASE_USER"),
        "dbname": os.getenv("SUPABASE_DB"),
        "password": os.getenv("SUPABASE_PASSWORD"),
        "port": int(os.getenv("SUPABASE_PORT", "5432")),
    }
    missing = [k for k, v in cfg.items() if not v]
    if missing:
        raise RuntimeError(f"Variables Supabase manquantes dans .env : {missing}")
    return cfg


def fetch_commune_geometry(schema: str, table: str):
    cfg = load_supabase_env()
    logger.info("Connexion Supabase %s/%s", cfg["host"], cfg["dbname"])
    conn = psycopg2.connect(
        host=cfg["host"],
        user=cfg["user"],
        dbname=cfg["dbname"],
        password=cfg["password"],
        port=cfg["port"],
        sslmode="require",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT insee, nom, ST_AsBinary(geom_2154) AS wkb_2154
                FROM {schema}.{table}
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Aucune commune trouvée dans {schema}.{table}")
            insee_val, nom, wkb_bytes = row
    finally:
        conn.close()

    geom = wkb.loads(bytes(wkb_bytes))
    logger.info(
        "Commune %s (insee=%s) | surface=%.2f km2 | bbox=%s",
        nom, insee_val, geom.area / 1_000_000, geom.bounds,
    )
    return geom, nom, insee_val


def parse_tile_coords(name: str) -> tuple[int, int] | None:
    """Extrait (X, Y) en km depuis un nom de dalle LHD_FXX_0702_6161_..."""
    import re
    m = re.search(r"LHD_FXX_(\d{4})_(\d{4})", name or "")
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def derive_mnt_url(name: str) -> tuple[str, str] | None:
    """Depuis 'LHD_FXX_0702_6161_PTS_C_LAMB93_IGN69', dérive l'URL WMS MNT + filename."""
    coords = parse_tile_coords(name)
    if not coords:
        return None
    x_km, y_km = coords  # coin Nord-Ouest de la tuile
    minx = x_km * 1000 - 0.25
    maxx = (x_km + 1) * 1000 - 0.25
    miny = (y_km - 1) * 1000 + 0.25
    maxy = y_km * 1000 + 0.25
    filename = f"LHD_FXX_{x_km:04d}_{y_km:04d}_MNT_O_0M50_LAMB93_IGN69.tif"
    url = MNT_WMS_URL_TEMPLATE.format(
        minx=minx, miny=miny, maxx=maxx, maxy=maxy, filename=filename
    )
    return url, filename


def fetch_lidar_tiles(geom) -> list[dict]:
    """Récupère les dalles LiDAR HD intersectant la géométrie via WFS IGN."""
    logger.info("WFS IGN | layer=%s", IGN_LIDAR_TILES_LAYER)
    minx, miny, maxx, maxy = geom.bounds
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LIDAR_TILES_LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "bbox": f"{minx},{miny},{maxx},{maxy},EPSG:2154",
    }
    response = requests.get(IGN_WFS_ENDPOINT, params=params, timeout=180)
    response.raise_for_status()

    gdf = gpd.read_file(io.BytesIO(response.content))
    if gdf.empty:
        logger.info("  -> aucune dalle LiDAR dans la bbox")
        return []

    inter = gdf[gdf.geometry.intersects(geom)].copy()
    logger.info("  -> bbox=%d, intersectant commune=%d", len(gdf), len(inter))

    tiles: list[dict] = []
    for _, row in inter.iterrows():
        url = row.get("url")
        name = row.get("name")
        if not url or not name:
            continue
        tiles.append({
            "layer_kind": "lidar",
            "id": row.get("id") or row.get("gml_id"),
            "name": name,
            "name_download": row.get("name_download"),
            "url": url,
        })
    return tiles


def derive_mnt_tiles_from_lidar(lidar_tiles: list[dict]) -> list[dict]:
    """Pour chaque dalle LiDAR, dérive la dalle MNT correspondante (même grille)."""
    tiles: list[dict] = []
    skipped = 0
    for t in lidar_tiles:
        derived = derive_mnt_url(t["name"])
        if not derived:
            skipped += 1
            continue
        url, filename = derived
        tiles.append({
            "layer_kind": "mnt",
            "id": t["id"],
            "name": filename.replace(".tif", ""),
            "name_download": filename,
            "url": url,
        })
    if skipped:
        logger.warning("MNT : %d dalle(s) LiDAR sans coordonnees parsables (ignorees)", skipped)
    logger.info("MNT derivees depuis LiDAR : %d", len(tiles))
    return tiles


def discover_layers():
    logger.info("Decouverte des couches IGN via WFS GetCapabilities")
    response = requests.get(
        IGN_WFS_ENDPOINT,
        params={"service": "WFS", "version": "2.0.0", "request": "GetCapabilities"},
        timeout=60,
    )
    response.raise_for_status()
    text = response.text
    candidates_lidar, candidates_mnt = [], []
    for line in text.splitlines():
        ll = line.lower()
        if "<name>" not in ll:
            continue
        name = line.strip().replace("<Name>", "").replace("</Name>", "").strip()
        if "lidar" in ll:
            candidates_lidar.append(name)
        if "mnt" in ll or "mns" in ll or "rgealti" in ll:
            candidates_mnt.append(name)
    logger.info("Candidats LiDAR : %s", candidates_lidar)
    logger.info("Candidats MNT   : %s", candidates_mnt)


def geojson_to_geometry_2154(geo: dict[str, Any], input_crs: str) -> BaseGeometry:
    """
    Accepte une GeoJSON Geometry, ou une Feature avec clé « geometry ».
    Reprojette vers EPSG:2154 (Lambert-93) pour WFS / WMS IGN.
    """
    if geo.get("type") == "Feature":
        inner = geo.get("geometry")
        if not inner:
            raise ValueError("Feature sans geometry")
        geo = inner
    g = shape(geo)
    if g.is_empty:
        raise ValueError("Géométrie vide")
    s = gpd.GeoSeries([g], crs=input_crs)
    out = s.to_crs("EPSG:2154").iloc[0]
    if out is None or out.is_empty:
        raise ValueError("Géométrie invalide après reprojection")
    return out


def list_tiles_for_geometry(
    geom: BaseGeometry,
    *,
    size_lidar_mb: float | None = None,
    size_mnt_mb: float | None = None,
) -> tuple[list[Tile], list[Tile]]:
    """Intersections IGN : dalles LiDAR HD + MNT dérivées (même grille)."""
    sl = size_lidar_mb if size_lidar_mb is not None else SIZE_LIDAR_MB
    sm = size_mnt_mb if size_mnt_mb is not None else SIZE_MNT_MB
    raw_lidar = fetch_lidar_tiles(geom)
    tiles_lidar = build_tiles(raw_lidar, sl)
    raw_mnt = derive_mnt_tiles_from_lidar(raw_lidar)
    tiles_mnt = build_tiles(raw_mnt, sm)
    return tiles_lidar, tiles_mnt


def summarize_tiles(tiles_lidar: list[Tile], tiles_mnt: list[Tile]) -> dict[str, Any]:
    total_lidar_mb = sum(t.estimated_size_mb for t in tiles_lidar)
    total_mnt_mb = sum(t.estimated_size_mb for t in tiles_mnt)
    return {
        "n_lidar": len(tiles_lidar),
        "n_mnt": len(tiles_mnt),
        "estimated_mb_lidar": round(total_lidar_mb, 1),
        "estimated_mb_mnt": round(total_mnt_mb, 1),
        "estimated_mb_total": round(total_lidar_mb + total_mnt_mb, 1),
    }


def run_pipeline_geometry_to_supabase(
    geom: BaseGeometry,
    storage_prefix: str,
    *,
    upload_lidar: bool = True,
    upload_mnt: bool = True,
    dry_run: bool = False,
    download_timeout: int = 600,
    size_lidar_mb: float | None = None,
    size_mnt_mb: float | None = None,
) -> dict[str, Any]:
    """
    Liste les dalles intersectant ``geom`` (EPSG:2154) ; en option télécharge et
    téléverse vers les buckets Supabase (dalles-lidar, mnt-dalles).

    ``storage_prefix`` : dossier dans chaque bucket (ex. code INSEE), sans slash.
    """
    prefix = str(storage_prefix).strip().strip("/").replace("..", "_")
    if not prefix:
        raise ValueError("storage_prefix vide ou invalide")

    tiles_lidar, tiles_mnt = list_tiles_for_geometry(
        geom, size_lidar_mb=size_lidar_mb, size_mnt_mb=size_mnt_mb
    )
    summary = summarize_tiles(tiles_lidar, tiles_mnt)
    out: dict[str, Any] = {
        "storage_prefix": prefix,
        "buckets": {"lidar": BUCKET_LIDAR, "mnt": BUCKET_MNT},
        "summary": summary,
        "dry_run": dry_run,
        "upload": {},
    }

    if dry_run:
        out["tiles_lidar"] = [asdict(t) for t in tiles_lidar]
        out["tiles_mnt"] = [asdict(t) for t in tiles_mnt]
        return out

    if not upload_lidar and not upload_mnt:
        raise ValueError("Au moins un des flags upload_lidar / upload_mnt doit être vrai")

    sb = load_supabase_storage_client()
    tot_ok = tot_err = tot_b = 0
    if upload_lidar and tiles_lidar:
        o, e, b = telecharger_et_envoyer_dalles(
            sb,
            tiles_lidar,
            BUCKET_LIDAR,
            prefix,
            timeout=download_timeout,
            label="LiDAR",
        )
        out["upload"]["lidar"] = {"ok": o, "errors": e, "bytes": b}
        tot_ok += o
        tot_err += e
        tot_b += b
    elif upload_lidar:
        out["upload"]["lidar"] = {"ok": 0, "errors": 0, "bytes": 0, "note": "aucune dalle"}

    if upload_mnt and tiles_mnt:
        o, e, b = telecharger_et_envoyer_dalles(
            sb,
            tiles_mnt,
            BUCKET_MNT,
            prefix,
            timeout=download_timeout,
            label="MNT",
        )
        out["upload"]["mnt"] = {"ok": o, "errors": e, "bytes": b}
        tot_ok += o
        tot_err += e
        tot_b += b
    elif upload_mnt:
        out["upload"]["mnt"] = {"ok": 0, "errors": 0, "bytes": 0, "note": "aucune dalle"}

    out["upload"]["totals"] = {
        "ok": tot_ok,
        "errors": tot_err,
        "bytes": tot_b,
    }
    if tot_err:
        out["status"] = "partial_failure"
    else:
        out["status"] = "ok"
    return out


def build_tiles(raw: list[dict], size_mb: float) -> list[Tile]:
    return [
        Tile(
            layer_kind=t["layer_kind"],
            id=t.get("id"),
            name=t.get("name"),
            name_download=t.get("name_download"),
            url=t["url"],
            estimated_size_mb=size_mb,
        )
        for t in raw
    ]


def load_supabase_storage_client():
    """Client Supabase pour Storage (clé service)."""
    load_dotenv()
    url = (os.getenv("SUPABASE_URL") or "").strip().strip('"').strip("'")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SERVICE_KEY")
        or os.getenv("SUPABASE_KEY")
        or ""
    ).strip().strip('"').strip("'")
    if not url or not key:
        raise RuntimeError(
            "Pour --telecharger : définir SUPABASE_URL et "
            "SUPABASE_SERVICE_ROLE_KEY (ou SERVICE_KEY / SUPABASE_KEY avec droits Storage)."
        )
    try:
        from supabase import create_client
    except ImportError as e:
        raise RuntimeError(
            "Paquet 'supabase' requis pour le téléversement. "
            "Installez-le : pip install supabase"
        ) from e
    return create_client(url, key)


def _basename_for_tile(t: Tile) -> str:
    if t.name_download:
        return Path(str(t.name_download)).name
    parsed = urlparse(t.url)
    name = unquote(Path(parsed.path).name)
    if name:
        return name
    base = (t.name or str(t.id or "tile")).replace("/", "_")
    return f"{base}.bin"


def _content_type_for_name(filename: str) -> str:
    lower = filename.lower()
    if lower.endswith(".tif") or lower.endswith(".tiff"):
        return "image/tiff"
    if lower.endswith(".laz") or lower.endswith(".las"):
        return "application/octet-stream"
    if lower.endswith(".zip"):
        return "application/zip"
    return "application/octet-stream"


def _storage_item_size_bytes(item: dict) -> int:
    meta = item.get("metadata")
    if not isinstance(meta, dict):
        return 0
    sz = meta.get("size")
    if sz is None:
        return 0
    try:
        return int(sz)
    except (TypeError, ValueError):
        return 0


def sum_listed_objects_bytes(client, bucket: str, prefix: str) -> int:
    """
    Somme des tailles (metadata.size) des objets listés sous un préfixe (non récursif au-delà d'un niveau).
    Utile pour estimer l'occupation du dossier commune/{insee}/ dans le bucket.
    """
    total = 0
    offset = 0
    page = 1000
    api = client.storage.from_(bucket)
    while True:
        opts = {"limit": page, "offset": offset, "sortBy": {"column": "name", "order": "asc"}}
        chunk = api.list(prefix or "", opts)
        if not chunk:
            break
        for item in chunk:
            total += _storage_item_size_bytes(item)
        if len(chunk) < page:
            break
        offset += len(chunk)
    return total


def download_url_bytes(url: str, timeout: int) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


def telecharger_et_envoyer_dalles(
    client,
    tiles: list[Tile],
    bucket: str,
    insee_val: str,
    *,
    timeout: int,
    label: str,
) -> tuple[int, int, int]:
    """
    Télécharge chaque dalle depuis l'URL IGN et l'upload dans `bucket`.
    Clé objet : {insee}/{basename}.

    Retourne (nombre_ok, nombre_erreurs, octets_téléversés_session).
    """
    ok = 0
    err = 0
    session_bytes = 0
    n = len(tiles)
    prefix = str(insee_val)

    try:
        avant = sum_listed_objects_bytes(client, bucket, prefix)
    except Exception as e:
        logger.warning(
            "[%s] Impossible de lire l'occupation Storage avant (%s) — poursuite.",
            label,
            e,
        )
        avant = -1

    if avant >= 0:
        logger.info(
            "[%s] Bucket '%s' | préfixe '%s/' | ~%.2f Mo listés avant run",
            label,
            bucket,
            prefix,
            avant / (1024 * 1024),
        )

    for i, t in enumerate(tiles, start=1):
        base = _basename_for_tile(t)
        remote = f"{prefix}/{base}"
        logger.info(
            "[%s] (%d/%d) %s | téléchargement IGN…",
            label,
            i,
            n,
            remote,
        )
        t_dl = time.perf_counter()
        try:
            data = download_url_bytes(t.url, timeout)
        except Exception as e:
            err += 1
            logger.error("[%s] Échec téléchargement %s : %s", label, remote, e)
            continue
        dl_s = time.perf_counter() - t_dl
        sz = len(data)
        logger.info(
            "[%s] (%d/%d) %s | %.1f Mo en %.1f s",
            label,
            i,
            n,
            remote,
            sz / (1024 * 1024),
            dl_s,
        )
        t_up = time.perf_counter()
        try:
            client.storage.from_(bucket).upload(
                remote,
                data,
                {
                    "content-type": _content_type_for_name(base),
                    "upsert": "true",
                },
            )
        except Exception as e:
            err += 1
            logger.error("[%s] Échec téléversement %s : %s", label, remote, e)
            del data
            continue
        up_s = time.perf_counter() - t_up
        session_bytes += sz
        ok += 1
        del data
        logger.info(
            "[%s] (%d/%d) %s | téléversé en %.1f s | cumul session : %.2f Mo",
            label,
            i,
            n,
            remote,
            up_s,
            session_bytes / (1024 * 1024),
        )

    try:
        apres = sum_listed_objects_bytes(client, bucket, prefix)
    except Exception as e:
        logger.warning("[%s] Impossible de relire l'occupation Storage après (%s).", label, e)
        apres = -1

    if avant >= 0 and apres >= 0:
        logger.info(
            "[%s] Bucket '%s' | préfixe '%s/' | ~%.2f Mo listés après run (delta ~%.2f Mo)",
            label,
            bucket,
            prefix,
            apres / (1024 * 1024),
            (apres - avant) / (1024 * 1024),
        )
    logger.info(
        "[%s] Terminé : %d OK, %d erreurs | %.2f Mo téléversés cette session",
        label,
        ok,
        err,
        session_bytes / (1024 * 1024),
    )
    return ok, err, session_bytes


def main():
    parser = argparse.ArgumentParser(
        description="Liste les dalles IGN MNT/LiDAR couvrant la commune stockée dans Supabase."
    )
    parser.add_argument("--schema", default="argeles")
    parser.add_argument("--table", default="commune")
    parser.add_argument("--discover", action="store_true",
                        help="Liste les couches WFS IGN disponibles puis quitte")
    parser.add_argument("--size-mnt-mb", type=float, default=SIZE_MNT_MB)
    parser.add_argument("--size-lidar-mb", type=float, default=SIZE_LIDAR_MB)
    parser.add_argument("--export-json", help="Exporte le résultat en JSON")
    parser.add_argument("--export-csv", help="Exporte les URLs en CSV")
    parser.add_argument(
        "--telecharger",
        action="store_true",
        help="Télécharge les dalles IGN et les envoie vers Supabase Storage (buckets dalles-lidar / mnt-dalles).",
    )
    parser.add_argument(
        "--mnt",
        action="store_true",
        help="Avec --telecharger : inclut les dalles MNT.",
    )
    parser.add_argument(
        "--lidar",
        action="store_true",
        help="Avec --telecharger : inclut les dalles LiDAR HD.",
    )
    parser.add_argument(
        "--download-timeout",
        type=int,
        default=600,
        help="Timeout HTTP par dalle (secondes), défaut 600.",
    )
    args = parser.parse_args()

    if args.discover:
        discover_layers()
        return

    geom, nom, insee_val = fetch_commune_geometry(args.schema, args.table)

    logger.info("==== Dalles LiDAR HD ====")
    raw_lidar = fetch_lidar_tiles(geom)
    tiles_lidar = build_tiles(raw_lidar, args.size_lidar_mb)

    logger.info("==== Dalles MNT (derivees depuis grille LiDAR) ====")
    raw_mnt = derive_mnt_tiles_from_lidar(raw_lidar)
    tiles_mnt = build_tiles(raw_mnt, args.size_mnt_mb)

    total_lidar_mb = sum(t.estimated_size_mb for t in tiles_lidar)
    total_mnt_mb = sum(t.estimated_size_mb for t in tiles_mnt)
    total_mb = total_lidar_mb + total_mnt_mb

    logger.info("==== Resume %s (%s) ====", nom, insee_val)
    logger.info("LiDAR HD : %d dalles | ~%.1f Go", len(tiles_lidar), total_lidar_mb / 1024)
    logger.info("MNT      : %d dalles | ~%.1f Go", len(tiles_mnt), total_mnt_mb / 1024)
    logger.info("TOTAL    : ~%.1f Go (%.0f Mo)", total_mb / 1024, total_mb)

    for kind, tiles in (("LiDAR", tiles_lidar), ("MNT", tiles_mnt)):
        logger.info("--- Apercu %s (5 premieres URLs) ---", kind)
        for t in tiles[:5]:
            logger.info("  %s", t.url)

    if args.export_json:
        out = {
            "commune": {"insee": insee_val, "nom": nom},
            "summary": {
                "n_lidar": len(tiles_lidar),
                "n_mnt": len(tiles_mnt),
                "total_estimated_mb": round(total_mb, 1),
                "total_estimated_gb": round(total_mb / 1024, 2),
                "size_lidar_mb_per_tile": args.size_lidar_mb,
                "size_mnt_mb_per_tile": args.size_mnt_mb,
            },
            "tiles_lidar": [asdict(t) for t in tiles_lidar],
            "tiles_mnt": [asdict(t) for t in tiles_mnt],
        }
        Path(args.export_json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.export_json).write_text(json.dumps(out, indent=2, ensure_ascii=False))
        logger.info("JSON ecrit : %s", args.export_json)

    if args.export_csv:
        import csv
        Path(args.export_csv).parent.mkdir(parents=True, exist_ok=True)
        with open(args.export_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "layer_kind",
                    "id",
                    "name",
                    "name_download",
                    "url",
                    "estimated_size_mb",
                    "probed_size_bytes",
                ]
            )
            for t in tiles_lidar + tiles_mnt:
                w.writerow([t.layer_kind, t.id, t.name, t.name_download, t.url, t.estimated_size_mb])
        logger.info("CSV ecrit : %s", args.export_csv)

    if args.telecharger:
        if not args.mnt and not args.lidar:
            args.mnt = True
            args.lidar = True
            logger.info("--telecharger sans --mnt/--lidar : les deux types sont inclus.")
        result = run_pipeline_geometry_to_supabase(
            geom,
            str(insee_val),
            upload_lidar=args.lidar,
            upload_mnt=args.mnt,
            dry_run=False,
            download_timeout=args.download_timeout,
            size_lidar_mb=args.size_lidar_mb,
            size_mnt_mb=args.size_mnt_mb,
        )
        totals = result.get("upload", {}).get("totals", {})
        logger.info(
            "==== Bilan téléversement ==== | OK=%s erreurs=%s | session total ~%.2f Mo",
            totals.get("ok"),
            totals.get("errors"),
            (totals.get("bytes") or 0) / (1024 * 1024),
        )
        if totals.get("errors", 0):
            raise SystemExit(1)


if __name__ == "__main__":
    main()