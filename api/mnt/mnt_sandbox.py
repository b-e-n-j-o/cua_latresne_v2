#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
mnt_sandbox_backend.py
----------------------
Adapté de parcelle_topo_3d.py : au lieu d'un export Plotly figé, on produit un
PAYLOAD (grille de hauteurs + masque parcelle) consommé par le bac à sable 3D
déblai/remblai (plateforme_3d.html).

Pipeline (réutilise ta logique existante) :
  1) géométrie(s) parcellaire(s) cible(s)  -> argeles.parcelles
  2) emprise = union(cibles).buffer(buffer_m)   (contexte spatial)
  3) dalles MNT en storage  -> ST_Intersects sur l'emprise
  4) téléchargement + merge + clip sur l'emprise
  5) -> payload : { nrows, ncols, pixel, z[], active[], perimetre_m, ref }
        . z      : altitudes (null = nodata, coins hors buffer)
        . active : 1 = à l'intérieur des parcelles CIBLES (= où on terrasse)
                   0 = buffer de contexte (rendu seulement, pas de cubature)

Différence clé avec le script d'origine :
  - multi-parcelles + buffer de contexte
  - on sépare le RENDU (toute l'emprise) du CALCUL (uniquement la cible)
  - sortie réutilisable côté front (et downsamplée pour rester fluide)
"""

import os
import json
import tempfile
import warnings
import logging

import numpy as np
import requests
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask
from rasterio.features import geometry_mask
from affine import Affine
from shapely.geometry import mapping
from shapely.ops import unary_union
from shapely import wkt as shapely_wkt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

DB_ENGINE = create_engine(
    f"postgresql+psycopg2://{os.getenv('SUPABASE_USER')}:"
    f"{os.getenv('SUPABASE_PASSWORD')}@{os.getenv('SUPABASE_HOST')}:"
    f"{os.getenv('SUPABASE_PORT', '5432')}/{os.getenv('SUPABASE_DB')}",
    connect_args={"sslmode": "require"}, pool_pre_ping=True,
)
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SCHEMA = os.getenv("PARCELLE_SCHEMA", "argeles")
MNT_TABLE = os.getenv("MNT_TABLE", "public.mnt_dalles")     # nom_fichier, storage_url, emprise
DEFAULT_INSEE = "66008"                                     # Argelès-sur-Mer


# ----------------------------------------------------------------------------- #
#  1) Géométries parcellaires cibles
# ----------------------------------------------------------------------------- #
def fetch_parcelles(refs: list[dict], code_insee: str = DEFAULT_INSEE):
    """
    refs : [{"section":"AB","numero":"0123"}, ...]  (numero/section tolérants).
    Retourne (liste[(geom, label)], union_cibles).
    """
    conds, params = [], {"insee": (code_insee or "").strip()}
    for i, r in enumerate(refs):
        conds.append(f"(upper(trim(section))=:s{i} AND trim(numero)=:n{i})")
        params[f"s{i}"] = (r.get("section") or "").strip().upper()
        params[f"n{i}"] = (r.get("numero") or "").strip()

    sql = f"""
        SELECT ST_AsText(ST_MakeValid(geom_2154)) AS wkt, section, numero, idu
        FROM {SCHEMA}.parcelles
        WHERE code_insee = :insee AND ({' OR '.join(conds)});
    """
    with DB_ENGINE.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    if not rows:
        raise ValueError(f"Aucune parcelle trouvée ({SCHEMA}.parcelles) pour {refs}")

    out = []
    for r in rows:
        g = shapely_wkt.loads(r["wkt"])
        label = f"{(r['section'] or '').strip()}{(r['numero'] or '').strip()}"
        out.append((g, label))
    union = unary_union([g for g, _ in out])
    logger.info("%d parcelle(s) cible(s) : %s | aire union ≈ %.0f m²",
                len(out), ", ".join(l for _, l in out), union.area)
    return out, union


# ----------------------------------------------------------------------------- #
#  2-4) MNT : dalles -> téléchargement -> merge -> clip  (repris de ton script)
# ----------------------------------------------------------------------------- #
def fetch_mnt(emprise):
    sql = f"""
        SELECT nom_fichier, storage_url
        FROM {MNT_TABLE}
        WHERE ST_Intersects(emprise, ST_GeomFromText(:geom, 2154))
        ORDER BY nom_fichier;
    """
    with DB_ENGINE.connect() as conn:
        dalles = [dict(r._mapping) for r in conn.execute(text(sql), {"geom": emprise.wkt})]
    if not dalles:
        raise ValueError("Aucune dalle MNT ne couvre l'emprise")
    logger.info("%d dalle(s) MNT : %s", len(dalles), [d["nom_fichier"] for d in dalles])

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        for d in dalles:
            resp = requests.get(d["storage_url"], headers=headers)
            resp.raise_for_status()
            p = os.path.join(tmp, d["nom_fichier"])
            with open(p, "wb") as f:
                f.write(resp.content)
            paths.append(p)
        srcs = [rasterio.open(p) for p in paths]
        if len(srcs) > 1:
            mosaic, transform = merge(srcs)
            src = rasterio.io.MemoryFile().open(
                driver="GTiff", height=mosaic.shape[1], width=mosaic.shape[2],
                count=1, dtype=mosaic.dtype, crs=srcs[0].crs, transform=transform)
            src.write(mosaic)
        else:
            src = srcs[0]
        out, transform = mask(src, [mapping(emprise)], crop=True, all_touched=True)
        data = out[0].astype("float64")
        if src.nodata is not None:
            data = np.where(data == src.nodata, np.nan, data)
        res = float(src.res[0])
        for s in srcs:
            s.close()
    logger.info("MNT clippé : %s @ %.2f m | z=[%.2f, %.2f]",
                data.shape, res, np.nanmin(data), np.nanmax(data))
    return data, transform, res


# ----------------------------------------------------------------------------- #
#  5) Raster -> payload  (partie testable sans base)
# ----------------------------------------------------------------------------- #
def _block_reduce(data, active, f):
    """Sous-échantillonne d'un facteur f : nanmean sur z, OR sur le masque actif."""
    nr, nc = data.shape
    pr, pc = (-nr) % f, (-nc) % f
    if pr or pc:
        data = np.pad(data, ((0, pr), (0, pc)), constant_values=np.nan)
        active = np.pad(active, ((0, pr), (0, pc)), constant_values=False)
    nr, nc = data.shape
    with warnings.catch_warnings():            # nanmean de bloc tout-NaN -> NaN (normal)
        warnings.simplefilter("ignore", category=RuntimeWarning)
        dz = np.nanmean(data.reshape(nr // f, f, nc // f, f), axis=(1, 3))
    da = active.reshape(nr // f, f, nc // f, f).any(axis=(1, 3))
    return dz, da


def build_payload(data, transform, res, cibles_union, perimetre_m, ref,
                  max_grid: int = 160):
    """Construit le payload JSON consommé par le bac à sable 3D."""
    active = geometry_mask([mapping(cibles_union)], out_shape=data.shape,
                           transform=transform, invert=True, all_touched=True)
    factor = max(1, int(np.ceil(max(data.shape) / max_grid)))
    if factor > 1:
        data, active = _block_reduce(data, active, factor)
        res = res * factor
        logger.info("Downsample ×%d -> %s @ %.2f m", factor, data.shape, res)

    valid = data[active & ~np.isnan(data)]
    z = [None if np.isnan(v) else round(float(v), 3) for v in data.ravel()]
    return {
        "ref": ref,
        "nrows": int(data.shape[0]),
        "ncols": int(data.shape[1]),
        "pixel": round(float(res), 3),
        "perimetre_m": round(float(perimetre_m), 1),
        "zmin": round(float(valid.min()), 2) if valid.size else None,
        "zmax": round(float(valid.max()), 2) if valid.size else None,
        "z": z,
        "active": [int(bool(a)) for a in active.ravel()],
    }


def analyser_parcelles(refs, code_insee=DEFAULT_INSEE, buffer_m=12.0, max_grid=160):
    """Bout-en-bout : refs parcellaires -> payload prêt pour le front."""
    cibles, union = fetch_parcelles(refs, code_insee)
    emprise = union.buffer(buffer_m)
    data, transform, res = fetch_mnt(emprise)
    ref = " + ".join(l for _, l in cibles)
    if len(cibles) > 1:
        ref = f"{code_insee} · {len(cibles)} parcelles"
    else:
        ref = f"{code_insee} {cibles[0][1]}"
    return build_payload(data, transform, res, union, union.length, ref, max_grid)


# ----------------------------------------------------------------------------- #
#  FastAPI (optionnel) — à brancher sur ton app existante
# ----------------------------------------------------------------------------- #
def make_app():
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel

    class ParcelleRef(BaseModel):
        section: str
        numero: str

    class Req(BaseModel):
        parcelles: list[ParcelleRef]
        code_insee: str = DEFAULT_INSEE
        buffer_m: float = 12.0
        max_grid: int = 160

    app = FastAPI(title="MNT sandbox")
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                       allow_headers=["*"])

    @app.post("/plateforme/mnt")
    def mnt(req: Req):
        return analyser_parcelles([r.model_dump() for r in req.parcelles],
                                  req.code_insee, req.buffer_m, req.max_grid)
    return app


# ----------------------------------------------------------------------------- #
#  CLI : dump un payload JSON (à déposer dans le bac à sable pour tester offline)
# ----------------------------------------------------------------------------- #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--parcelles", nargs="+", required=True,
                    help="couples SECTION:NUMERO, ex: AB:0123 AB:0124")
    ap.add_argument("--insee", default=DEFAULT_INSEE)
    ap.add_argument("--buffer", type=float, default=12.0)
    ap.add_argument("--max-grid", type=int, default=160)
    ap.add_argument("--out", default="mnt_payload.json")
    a = ap.parse_args()
    refs = [{"section": p.split(":")[0], "numero": p.split(":")[1]} for p in a.parcelles]
    payload = analyser_parcelles(refs, a.insee, a.buffer, a.max_grid)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    logger.info("Payload écrit : %s (%d×%d, pixel %.2f m)",
                a.out, payload["nrows"], payload["ncols"], payload["pixel"])