"""
Affiche la parcelle cible en 3D de deux façons :
- mode mnt  : points dérivés des rasters MNT locaux
- mode laz  : vrai nuage LiDAR (LAS/LAZ) filtré sur le polygone parcellaire

Exemples :
python affichage_nuage_parcelle.py --mode mnt --code-insee 33234 --section AC --numero 0042
python affichage_nuage_parcelle.py --mode laz --code-insee 33234 --section AE --numero 0364 --laz-url "https://.../0416_6423.laz"
"""

from __future__ import annotations

import argparse
from pathlib import Path
import io
import tempfile
import logging
import threading
import time

import geopandas as gpd
import laspy
import numpy as np
import pandas as pd
import pydeck as pdk
import pyvista as pv
import rasterio
from rasterio.mask import mask
from rasterio.merge import merge
from rasterio.transform import xy
import requests
from shapely.geometry import Point, box, mapping


IGN_WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
IGN_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"
IGN_LIDAR_TILES_LAYER = "IGNF_NUAGES-DE-POINTS-LIDAR-HD:dalle"
SRS = "EPSG:2154"
MNT_DIR = Path("/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/LIDAR/LiDAR HD MNT")

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
try:
    import psutil
except Exception:
    psutil = None

LAS_CLASS_LABELS = {
    0: "0 - Cree / Jamais classe",
    1: "1 - Non classe",
    2: "2 - Sol",
    3: "3 - Vegetation basse",
    4: "4 - Vegetation moyenne",
    5: "5 - Vegetation haute",
    6: "6 - Batiment",
    7: "7 - Bruit",
    9: "9 - Eau",
    17: "17 - Pont",
    18: "18 - Catenaire",
    64: "64 - Sursol perenne",
    65: "65 - Artefact",
    66: "66 - Point virtuel",
    67: "67 - Divers",
}

LAS_CLASS_COLORS = {
    0: "rgb(160,160,160)",
    1: "rgb(200,200,200)",
    2: "rgb(255,140,0)",
    3: "rgb(0,255,255)",
    4: "rgb(0,255,0)",
    5: "rgb(0,180,0)",
    6: "rgb(255,0,0)",
    7: "rgb(180,180,180)",
    9: "rgb(0,200,255)",
    17: "rgb(255,255,0)",
    18: "rgb(255,100,255)",
    64: "rgb(255,210,120)",
    65: "rgb(255,150,150)",
    66: "rgb(170,170,255)",
    67: "rgb(180,100,255)",
}


def format_class_legend_text(classes: np.ndarray) -> str:
    uniques = sorted(np.unique(classes).tolist())
    lines = ["Classes LAS visibles :"]
    for cls in uniques:
        lines.append(f"{LAS_CLASS_LABELS.get(int(cls), f'{int(cls)} - Inconnue')}")
    return "\n".join(lines)


class RamMonitor:
    def __init__(self, interval_s: float = 2.0):
        self.interval_s = interval_s
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self):
        if psutil is None:
            logger.warning("psutil non disponible: monitoring RAM desactive.")
            return
        process = psutil.Process()

        def _run():
            logger.info("Monitoring RAM actif (intervalle %.1fs)", self.interval_s)
            while not self._stop_event.is_set():
                try:
                    rss_mb = process.memory_info().rss / (1024 * 1024)
                    vm = psutil.virtual_memory()
                    used_gb = vm.used / (1024 ** 3)
                    total_gb = vm.total / (1024 ** 3)
                    logger.info(
                        "RAM process=%.1f MB | RAM systeme=%.1f/%.1f GB (%.1f%%)",
                        rss_mb, used_gb, total_gb, vm.percent
                    )
                except Exception as e:
                    logger.warning("Monitoring RAM erreur: %s", e)
                if self._stop_event.wait(self.interval_s):
                    break

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self):
        if self._thread is None:
            return
        self._stop_event.set()
        self._thread.join(timeout=1.0)
        logger.info("Monitoring RAM arrete.")


def fetch_parcelle_geometry(code_insee: str, section: str, numero: str):
    logger.info("Etape 1/4 - Recuperation de la geometrie parcellaire IGN")
    cql = f"code_insee='{code_insee}' AND section='{section}' AND numero='{numero}'"
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "CQL_FILTER": cql,
    }
    response = requests.get(IGN_WFS_ENDPOINT, params=params, timeout=60)
    response.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(response.content))
    if gdf.empty:
        raise ValueError("Parcelle introuvable via l'IGN.")
    logger.info("Parcelle trouvee, surface=%.2f m2", gdf.geometry.iloc[0].area)
    return gdf.geometry.iloc[0]


def select_intersecting_tiles(parcelle_geom, tif_paths: list[Path]) -> list[Path]:
    selected = []
    for tif in tif_paths:
        with rasterio.open(tif) as src:
            tile_poly = box(*src.bounds)
        if tile_poly.intersects(parcelle_geom):
            selected.append(tif)
    return selected


def build_clipped_mnt(parcelle_geom, tif_paths: list[Path]):
    if not tif_paths:
        raise ValueError("Aucune dalle MNT .tif trouvée dans le dossier LiDAR HD MNT.")

    selected_tiles = select_intersecting_tiles(parcelle_geom, tif_paths)
    if not selected_tiles:
        raise ValueError("Aucune dalle MNT n'intersecte la parcelle cible.")
    logger.info("Etape 2/4 - Dalles MNT intersectees: %d", len(selected_tiles))

    srcs = [rasterio.open(path) for path in selected_tiles]
    try:
        if len(srcs) > 1:
            mosaic, mosaic_transform = merge(srcs)
            memfile = rasterio.io.MemoryFile()
            with memfile.open(
                driver="GTiff",
                height=mosaic.shape[1],
                width=mosaic.shape[2],
                count=1,
                dtype=mosaic.dtype,
                crs=srcs[0].crs,
                transform=mosaic_transform,
                nodata=srcs[0].nodata,
            ) as ds:
                ds.write(mosaic)
                clipped, clipped_transform = mask(
                    ds,
                    [mapping(parcelle_geom)],
                    crop=True,
                    all_touched=True,
                )
                nodata = ds.nodata
        else:
            clipped, clipped_transform = mask(
                srcs[0],
                [mapping(parcelle_geom)],
                crop=True,
                all_touched=True,
            )
            nodata = srcs[0].nodata
    finally:
        for src in srcs:
            src.close()

    arr = clipped[0].astype(float)
    if nodata is not None:
        arr[arr == nodata] = np.nan
    return arr, clipped_transform


def mnt_to_point_cloud(arr: np.ndarray, transform) -> pv.PolyData:
    rows, cols = np.where(np.isfinite(arr))
    if rows.size == 0:
        raise ValueError("Aucun pixel valide après clip de la parcelle.")

    xs, ys = xy(transform, rows, cols, offset="center")
    zs = arr[rows, cols]
    points = np.column_stack([np.asarray(xs), np.asarray(ys), zs])
    logger.info("Etape 3/4 - Nuage MNT prepare: %d points", points.shape[0])

    cloud = pv.PolyData(points)
    cloud["altitude"] = zs
    return cloud


def download_laz(url: str) -> Path:
    logger.info("Etape 2/4 - Telechargement LAZ depuis Supabase")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    suffix = ".laz" if url.lower().endswith(".laz") else ".las"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    with open(tmp.name, "wb") as f:
        f.write(response.content)
    logger.info("LAZ temporaire ecrit: %s (%.2f Mo)", tmp.name, len(response.content) / (1024 * 1024))
    return Path(tmp.name)


def resolve_laz_input(laz_url: str | None, laz_file: str | None) -> tuple[Path, bool]:
    if laz_file:
        path = Path(laz_file).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Fichier LAZ introuvable: {path}")
        logger.info("Etape 2/4 - Utilisation d'un fichier LAZ local: %s", path)
        return path, False

    if laz_url:
        if laz_url.startswith("file://"):
            path = Path(laz_url.replace("file://", "")).expanduser().resolve()
            if not path.exists():
                raise ValueError(f"Fichier LAZ introuvable: {path}")
            logger.info("Etape 2/4 - Utilisation d'un fichier LAZ local (file://): %s", path)
            return path, False
        return download_laz(laz_url), True

    raise ValueError("En mode laz, fournir --laz-file ou --laz-url.")


def fetch_lidar_tiles_for_parcelle(parcelle_geom) -> list[dict]:
    logger.info("Etape 2/4 - Interrogation couche IGN des dalles LiDAR HD")
    minx, miny, maxx, maxy = parcelle_geom.bounds
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LIDAR_TILES_LAYER,
        "srsName": SRS,
        "outputFormat": "application/json",
        "bbox": f"{minx},{miny},{maxx},{maxy},EPSG:2154",
    }
    response = requests.get(IGN_WFS_ENDPOINT, params=params, timeout=120)
    response.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(response.content))
    if gdf.empty:
        logger.info("Aucune dalle retournee par l'API dans la bbox.")
        return []

    intersects = gdf[gdf.geometry.intersects(parcelle_geom)].copy()
    if intersects.empty:
        logger.info("Aucune dalle n'intersecte exactement la parcelle.")
        return []

    tiles: list[dict] = []
    for _, row in intersects.iterrows():
        url = row.get("url")
        if not url:
            continue
        tiles.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "name_download": row.get("name_download"),
                "url": url,
            }
        )

    logger.info("Dalles LiDAR intersectees: %d", len(tiles))
    for i, tile in enumerate(tiles, start=1):
        logger.info("  [%d] %s", i, tile["url"])
    return tiles


def download_lidar_tiles(tiles: list[dict], output_dir: Path, limit: int | None = None) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    picked = tiles[:limit] if limit and limit > 0 else tiles
    logger.info("Telechargement des dalles (%d fichier(s)) dans %s", len(picked), output_dir)
    downloaded_paths: list[Path] = []

    for i, tile in enumerate(picked, start=1):
        url = str(tile["url"])
        filename = tile.get("name_download") or Path(url).name or f"dalle_{i}.laz"
        target_path = output_dir / filename

        logger.info("  [%d/%d] %s", i, len(picked), filename)
        with requests.get(url, stream=True, timeout=300) as response:
            response.raise_for_status()
            with open(target_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        logger.info("  -> OK (%s)", target_path)
        downloaded_paths.append(target_path)

    return downloaded_paths


def export_cloud_to_html_deckgl(
    cloud: pv.PolyData,
    html_path: Path,
    title: str,
    html_max_points: int,
    deck_point_size: float,
):
    pts = np.asarray(cloud.points)
    if pts.size == 0:
        raise ValueError("Nuage vide: export HTML impossible.")

    # Par defaut, pas de sous-echantillonnage (precision maximale).
    # Une limite > 0 permet un fallback manuel pour HTML tres volumineux.
    if html_max_points > 0 and pts.shape[0] > html_max_points:
        idx = np.random.choice(pts.shape[0], size=html_max_points, replace=False)
        pts = pts[idx]
        cls = np.asarray(cloud["classification"])[idx] if "classification" in cloud.array_names else None
        alt = pts[:, 2]
        logger.info("Export HTML: sous-echantillonnage %d -> %d points", cloud.n_points, pts.shape[0])
    else:
        cls = np.asarray(cloud["classification"]) if "classification" in cloud.array_names else None
        alt = pts[:, 2]

    df = pd.DataFrame(pts, columns=["x", "y", "z"])

    if cls is not None:
        rgb = [LAS_CLASS_COLORS.get(int(c), "rgb(255,255,255)") for c in cls]
        # "rgb(255,140,0)" -> [255, 140, 0]
        df["r"] = [int(c[4:].split(",")[0]) for c in rgb]
        df["g"] = [int(c[4:].split(",")[1]) for c in rgb]
        df["b"] = [int(c[4:].split(",")[2].rstrip(")")) for c in rgb]
        df["label"] = [LAS_CLASS_LABELS.get(int(c), f"{int(c)} - Inconnue") for c in cls]
        get_color = "[r, g, b]"
    else:
        z_min, z_max = float(np.nanmin(alt)), float(np.nanmax(alt))
        if z_max <= z_min:
            z_max = z_min + 1.0
        t = (alt - z_min) / (z_max - z_min)
        df["r"] = (20 + 235 * t).astype(int)
        df["g"] = (80 + 120 * (1 - t)).astype(int)
        df["b"] = (255 - 200 * t).astype(int)
        get_color = "[r, g, b]"

    layer = pdk.Layer(
        "PointCloudLayer",
        data=df,
        get_position="[x, y, z]",
        get_color=get_color,
        point_size=deck_point_size,
        pickable=True,
    )

    view_state = pdk.ViewState(
        target=[float(df["x"].mean()), float(df["y"].mean()), float(df["z"].mean())],
        zoom=0,
        pitch=45,
        bearing=0,
    )

    tooltip = {
        "html": (
            "<b>X</b>: {x}<br/><b>Y</b>: {y}<br/><b>Z</b>: {z}<br/>"
            + ("<b>Classe</b>: {label}" if "label" in df.columns else "")
        ),
        "style": {"backgroundColor": "rgba(0,0,0,0.75)", "color": "white"},
    }

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        views=[pdk.View(type="OrbitView", controller=True)],
        tooltip=tooltip,
        parameters={"clearColor": [0, 0, 0, 255]},
        description=title,
    )

    html_path.parent.mkdir(parents=True, exist_ok=True)
    deck.to_html(str(html_path), css_background_color="black")
    logger.info("Export HTML deck.gl termine: %s", html_path)


def build_pyvista_plotter(
    cloud: pv.PolyData,
    point_size: float,
    title: str,
) -> pv.Plotter:
    plotter = pv.Plotter()
    plotter.set_background("black")
    if "classification" in cloud.array_names:
        plotter.add_points(
            cloud,
            scalars="classification",
            cmap="tab20",
            render_points_as_spheres=True,
            point_size=point_size,
        )
        plotter.add_scalar_bar(title="Classe LAS")
        class_legend = format_class_legend_text(np.asarray(cloud["classification"]))
        plotter.add_text(
            class_legend,
            position="lower_left",
            font_size=9,
            color="white",
        )
    else:
        plotter.add_points(
            cloud,
            scalars="altitude",
            cmap="terrain",
            render_points_as_spheres=True,
            point_size=point_size,
        )
        plotter.add_scalar_bar(title="Altitude (m)")
    plotter.add_text(
        title.replace(",", " "),
        position="upper_left",
        font_size=10,
        color="white",
    )
    plotter.show_grid()
    return plotter


def laz_to_point_cloud(laz_path: Path, parcelle_geom, max_points: int) -> pv.PolyData:
    logger.info("Etape 3/4 - Lecture et filtrage du nuage LAZ")
    las = laspy.read(laz_path)
    xs = np.asarray(las.x)
    ys = np.asarray(las.y)
    zs = np.asarray(las.z)

    if xs.size == 0:
        raise ValueError("Le fichier LAZ est vide.")

    # Préfiltre bbox pour éviter de tester tous les points au polygone
    minx, miny, maxx, maxy = parcelle_geom.bounds
    bbox_mask = (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)
    if not np.any(bbox_mask):
        raise ValueError("Aucun point LAZ dans la bbox de la parcelle.")
    logger.info("Points dans bbox parcelle: %d / %d", int(np.count_nonzero(bbox_mask)), xs.size)

    xb = xs[bbox_mask]
    yb = ys[bbox_mask]
    zb = zs[bbox_mask]

    # Test point-in-polygon exact sur les points de bbox
    pip_mask = np.fromiter(
        (parcelle_geom.covers(Point(x, y)) for x, y in zip(xb, yb)),
        dtype=bool,
        count=xb.size,
    )
    if not np.any(pip_mask):
        raise ValueError("Aucun point LAZ à l'intérieur de la parcelle.")
    logger.info("Points dans polygone parcelle: %d", int(np.count_nonzero(pip_mask)))

    x_in = xb[pip_mask]
    y_in = yb[pip_mask]
    z_in = zb[pip_mask]
    points = np.column_stack([x_in, y_in, z_in])

    # Par defaut, pas de sous-echantillonnage (precision maximale).
    # Une limite > 0 permet un fallback manuel si necessaire.
    if max_points > 0 and points.shape[0] > max_points:
        idx = np.random.choice(points.shape[0], size=max_points, replace=False)
        points = points[idx]
        selected_global = np.where(bbox_mask)[0][pip_mask][idx]
        logger.info("Sous-echantillonnage applique: %d points conserves (max=%d)", points.shape[0], max_points)
    else:
        selected_global = np.where(bbox_mask)[0][pip_mask]
        logger.info("Pas de sous-echantillonnage: %d points", points.shape[0])

    cloud = pv.PolyData(points)
    cloud["altitude"] = points[:, 2]

    if hasattr(las, "classification"):
        classes = np.asarray(las.classification)[selected_global].astype(np.uint8)
        cloud["classification"] = classes

    return cloud


def main():
    parser = argparse.ArgumentParser(description="Visualisation nuage 3D d'une parcelle depuis LiDAR HD MNT")
    parser.add_argument("--mode", choices=["mnt", "laz", "ign-tiles"], default="mnt", help="Source de données 3D")
    parser.add_argument("--code-insee", required=True, help="Code INSEE (ex: 33234)")
    parser.add_argument("--section", required=True, help="Section cadastrale (ex: AC)")
    parser.add_argument("--numero", required=True, help="Numéro cadastral (ex: 0042)")
    parser.add_argument("--laz-url", help="URL Supabase publique du fichier .laz/.las (mode laz)")
    parser.add_argument("--laz-file", help="Chemin local vers un fichier .laz/.las/.copc.laz (mode laz)")
    parser.add_argument("--max-points", type=int, default=0, help="Nombre max de points affiches (mode laz). 0 = tous les points")
    parser.add_argument("--download-tiles-dir", help="Dossier de destination pour telecharger les dalles IGN (mode ign-tiles)")
    parser.add_argument("--download-limit", type=int, default=0, help="Limiter le nombre de dalles telechargees (0 = toutes)")
    parser.add_argument("--export-html", help="Chemin fichier HTML pour export deck.gl du nuage final")
    parser.add_argument("--html-engine", choices=["pyvista", "deckgl"], default="deckgl", help="Moteur export HTML")
    parser.add_argument("--html-max-points", type=int, default=0, help="Nombre max de points dans l'export HTML. 0 = tous les points")
    parser.add_argument("--deck-point-size", type=float, default=1.0, help="Taille des points dans l'export HTML deck.gl")
    parser.add_argument("--point-size", type=float, default=3.0, help="Taille des points PyVista")
    args = parser.parse_args()
    logger.info("==== Demarrage visualisation parcelle %s %s%s (mode=%s) ====", args.code_insee, args.section, args.numero, args.mode)

    ram_monitor = RamMonitor(interval_s=2.0)
    ram_monitor.start()

    parcelle_geom = fetch_parcelle_geometry(args.code_insee, args.section, args.numero)
    laz_path: Path | None = None
    laz_is_temp = False

    try:
        if args.mode == "ign-tiles":
            tiles = fetch_lidar_tiles_for_parcelle(parcelle_geom)
            if not tiles:
                return
            if args.download_tiles_dir:
                limit = args.download_limit if args.download_limit > 0 else None
                downloaded = download_lidar_tiles(tiles, Path(args.download_tiles_dir), limit=limit)
                logger.info("Telechargement termine: %d fichier(s).", len(downloaded))
            else:
                logger.info("Aucun telechargement lance (utiliser --download-tiles-dir pour telecharger).")
            return

        if args.mode == "laz":
            laz_path, laz_is_temp = resolve_laz_input(args.laz_url, args.laz_file)
            cloud = laz_to_point_cloud(laz_path, parcelle_geom, args.max_points)
        else:
            tif_paths = sorted(
                p for p in MNT_DIR.glob("*.tif")
                if not p.name.startswith("._")
            )
            logger.info("Etape 2/4 - Dalles MNT candidates: %d", len(tif_paths))
            clipped_arr, clipped_transform = build_clipped_mnt(parcelle_geom, tif_paths)
            cloud = mnt_to_point_cloud(clipped_arr, clipped_transform)

        plot_title = (
            f"Parcelle {args.code_insee} {args.section}{args.numero} ({args.mode.upper()})\n"
            f"Points: {cloud.n_points:,}"
        )
        logger.info("Etape 4/4 - Preparation du rendu 3D (%d points)", cloud.n_points)
        plotter = build_pyvista_plotter(
            cloud=cloud,
            point_size=args.point_size,
            title=plot_title,
        )

        if args.export_html:
            html_path = Path(args.export_html).expanduser().resolve()
            html_path.parent.mkdir(parents=True, exist_ok=True)
            if args.html_engine == "pyvista":
                try:
                    plotter.export_html(str(html_path))
                    logger.info("Export HTML PyVista termine: %s", html_path)
                except Exception as e:
                    logger.warning("Export PyVista HTML indisponible (%s). Fallback deck.gl.", e)
                    export_cloud_to_html_deckgl(
                        cloud=cloud,
                        html_path=html_path,
                        title=plot_title.replace(",", " "),
                        html_max_points=args.html_max_points,
                        deck_point_size=args.deck_point_size,
                    )
            else:
                export_cloud_to_html_deckgl(
                    cloud=cloud,
                    html_path=html_path,
                    title=plot_title.replace(",", " "),
                    html_max_points=args.html_max_points,
                    deck_point_size=args.deck_point_size,
                )

        logger.info("Etape 5/5 - Ouverture de la fenetre PyVista")
        plotter.show()
    finally:
        ram_monitor.stop()
        if laz_is_temp and laz_path is not None and laz_path.exists():
            laz_path.unlink()
            logger.info("Fichier temporaire supprime: %s", laz_path)


if __name__ == "__main__":
    main()