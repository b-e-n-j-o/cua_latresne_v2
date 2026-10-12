# -*- coding: utf-8 -*-
"""
Contributeurs terrain (MNT IGN LiDAR HD) :
  - Terrain2DContributor : emprise MNT, courbes de niveau + cotes, points cotés
  - Terrain3DContributor : maillage TIN simplifié

TerrainModel encapsule la grille du payload figé (Lambert-93 relatif au centre)
et sait échantillonner l'altitude en coordonnées de sortie (pour le drapage).
"""

from __future__ import annotations

import base64
import math
from dataclasses import dataclass
from typing import Iterator, Literal

import contourpy
import numpy as np
from ezdxf.enums import TextEntityAlignment
from ezdxf.render import MeshBuilder

from .core import Contributor, ExportContext, LayerSpec, add_text, ensure_layer, readable_angle

TERRAIN_EPSG = 2154


# --------------------------------------------------------------------------- #
# Modèle de terrain
# --------------------------------------------------------------------------- #

def _neighbour_mean(z: np.ndarray) -> np.ndarray:
    h, w = z.shape
    p = np.pad(z, 1, constant_values=np.nan)
    neigh = np.stack([p[1 + dy:1 + dy + h, 1 + dx:1 + dx + w]
                      for dy in (-1, 0, 1) for dx in (-1, 0, 1)])
    fin = np.isfinite(neigh)
    n = fin.sum(0)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(n > 0, np.where(fin, neigh, 0).sum(0) / n, np.nan)


class TerrainModel:
    def __init__(self, payload: dict, pixel_is_center: bool = True):
        raw = base64.b64decode(payload["elevations_b64"])
        self.w, self.h = int(payload["width"]), int(payload["height"])
        z = np.frombuffer(raw, dtype="<f4", count=self.w * self.h).astype(np.float64)
        z = z.reshape(self.h, self.w)
        nodata = payload.get("nodata")
        if nodata is not None:
            z[np.isclose(z, float(nodata))] = np.nan
        z[~np.isfinite(z)] = np.nan
        if np.isnan(z).all():
            raise ValueError("MNT vide : aucune altitude valide.")
        self.z = z
        self.res = float(payload["resolution_m"])
        self.cx = float(payload.get("center_x") or 0.0)
        self.cy = float(payload.get("center_y") or 0.0)
        self.off = 0.5 if pixel_is_center else 0.0
        self.W, self.H = self.w * self.res, self.h * self.res
        self.payload = payload

    # -- grille -> relatif -------------------------------------------------- #
    def col_to_rx(self, col):
        return -self.W / 2.0 + (np.asarray(col, float) + self.off) * self.res

    def row_to_ry(self, row):
        return self.H / 2.0 - (np.asarray(row, float) + self.off) * self.res

    def rel_bbox(self):
        return (float(self.col_to_rx(0)), float(self.row_to_ry(self.h - 1)),
                float(self.col_to_rx(self.w - 1)), float(self.row_to_ry(0)))

    # -- relatif <-> sortie -------------------------------------------------- #
    def rel_to_out(self, georef, rx, ry):
        return georef.forward(self.cx + np.asarray(rx, float),
                              self.cy + np.asarray(ry, float), TERRAIN_EPSG)

    def bbox_out(self, georef):
        x1, y1, x2, y2 = self.rel_bbox()
        xs, ys = self.rel_to_out(georef, [x1, x2, x2, x1], [y1, y1, y2, y2])
        return float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max())

    # -- échantillonnage ---------------------------------------------------- #
    def sample_rel(self, rx, ry, z: np.ndarray | None = None) -> np.ndarray:
        z = self.z if z is None else z
        rx = np.atleast_1d(np.asarray(rx, float))
        ry = np.atleast_1d(np.asarray(ry, float))
        c = np.clip((rx + self.W / 2.0) / self.res - self.off, 0, self.w - 1)
        r = np.clip((self.H / 2.0 - ry) / self.res - self.off, 0, self.h - 1)
        c0, r0 = np.floor(c).astype(int), np.floor(r).astype(int)
        c1, r1 = np.minimum(c0 + 1, self.w - 1), np.minimum(r0 + 1, self.h - 1)
        fc, fr = c - c0, r - r0
        vals = np.stack([z[r0, c0], z[r0, c1], z[r1, c0], z[r1, c1]])
        wts = np.stack([(1 - fc) * (1 - fr), fc * (1 - fr), (1 - fc) * fr, fc * fr])
        fin = np.isfinite(vals)
        v = np.where(fin, vals, 0.0)
        wsum = (wts * fin).sum(0)
        nfin = fin.sum(0)
        with np.errstate(invalid="ignore", divide="ignore"):
            bil = (wts * v * fin).sum(0) / wsum
            avg = (v * fin).sum(0) / nfin
        return np.where(wsum > 1e-9, bil, np.where(nfin > 0, avg, np.nan))

    def sample_out(self, georef, x, y) -> np.ndarray:
        lx, ly = georef.inverse(x, y, TERRAIN_EPSG)
        return self.sample_rel(lx - self.cx, ly - self.cy)

    def uf_rings_rel(self) -> list[np.ndarray]:
        """Contour UF du payload terrain (repli si pas de contexte carto)."""
        rings = []
        for gj in self.payload.get("contours") or []:
            t, coords = gj.get("type"), gj.get("coordinates") or []
            polys = [coords] if t == "Polygon" else coords if t == "MultiPolygon" else []
            for poly in polys:
                for ring in poly:
                    pts = np.array([p[:2] for p in ring if len(p) >= 2], float)
                    if len(pts) >= 3:
                        rings.append(pts)
        return rings

    def fill_nan(self, max_iter: int = 200) -> np.ndarray:
        out = self.z.copy()
        for _ in range(max_iter):
            mask = np.isnan(out)
            if not mask.any():
                return out
            m = _neighbour_mean(out)
            out[mask] = m[mask]
        out[np.isnan(out)] = np.nanmean(self.z)
        return out

    def smoothed(self, passes: int) -> np.ndarray:
        out = self.z.copy()
        nan = np.isnan(self.z)
        for _ in range(max(0, passes)):
            out = _neighbour_mean(out)
            out[nan] = np.nan
        return out


# --------------------------------------------------------------------------- #
# Options
# --------------------------------------------------------------------------- #

@dataclass
class TerrainOptions:
    pixel_is_center: bool = True          # À VÉRIFIER contre le geotransform du payload
    # 2D
    flatten_2d: bool = True               # plan de masse : tout à Z=0 (évite les Z parasites)
    contour_interval_m: float | None = None
    contour_master_m: float | None = None
    contour_smoothing_passes: int = 2
    contour_simplify_m: float = 0.05
    contour_min_length_m: float = 3.0
    contour_label_spacing_mm: float = 160.0   # espacement des cotes de courbes (mm papier)
    spot_spacing_m: float = 10.0
    # 3D
    mesh_max_error_m: float = 0.05
    mesh_entity: Literal["polyface", "mesh", "3dface"] = "polyface"
    max_vertices_per_entity: int = 30000
    source_label: str = "IGN – LiDAR HD, MNT 50 cm"


def auto_interval(z: np.ndarray) -> tuple[float, float]:
    relief = float(np.nanpercentile(z, 99) - np.nanpercentile(z, 1))
    for limit, eq, master in ((3, 0.25, 1.0), (10, 0.5, 2.5), (40, 1.0, 5.0), (150, 2.0, 10.0)):
        if relief < limit:
            return eq, master
    return 5.0, 25.0


def _rdp(pts: np.ndarray, eps: float) -> np.ndarray:
    if eps <= 0 or len(pts) < 3:
        return pts
    keep = np.zeros(len(pts), bool)
    keep[[0, -1]] = True
    stack = [(0, len(pts) - 1)]
    while stack:
        i, j = stack.pop()
        if j <= i + 1:
            continue
        a, b, seg = pts[i], pts[j], pts[i + 1:j]
        d = b - a
        L = math.hypot(*d)
        dist = (np.hypot(*(seg - a).T) if L == 0 else
                np.abs(d[0] * (seg[:, 1] - a[1]) - d[1] * (seg[:, 0] - a[0])) / L)
        k = int(np.argmax(dist))
        if dist[k] > eps:
            m = i + 1 + k
            keep[m] = True
            stack += [(i, m), (m, j)]
    return pts[keep]


def _fmt_z(z: float) -> str:
    return f"{z:.2f}".rstrip("0").rstrip(".") or "0"


# --------------------------------------------------------------------------- #
# Contributeur 2D
# --------------------------------------------------------------------------- #

class Terrain2DContributor(Contributor):
    name = "terrain_2d"

    def __init__(self, options: TerrainOptions | None = None):
        self.o = options or TerrainOptions()
        self.eq = self.master = None

    def _levels(self, t: TerrainModel):
        eq, master = auto_interval(t.z)
        if self.o.contour_interval_m:
            eq = self.o.contour_interval_m
            master = self.o.contour_master_m or eq * 5
        elif self.o.contour_master_m:
            master = self.o.contour_master_m
        return eq, master

    def write(self, doc, ctx: ExportContext) -> dict:
        t: TerrainModel = ctx.terrain
        if t is None:
            return {}
        o, g, msp = self.o, ctx.georef, doc.modelspace()
        for spec in (
            LayerSpec("TN_EMPRISE_MNT", 8, lineweight=13, linetype="KER_TIRETS",
                      description="Emprise du MNT exporté"),
            LayerSpec("TN_COURBES_MAITRESSES", 30, lineweight=35,
                      description="Courbes de niveau maîtresses (TN, MNT LiDAR)"),
            LayerSpec("TN_COURBES_INTERMEDIAIRES", 32, lineweight=13,
                      description="Courbes de niveau intermédiaires (TN, MNT LiDAR)"),
            LayerSpec("TN_COURBES_COTES", 30, lineweight=18, description="Cotes des courbes maîtresses"),
            LayerSpec("TN_POINTS_COTES", 5, lineweight=13, description="Points cotés du terrain naturel"),
        ):
            ensure_layer(doc, spec)

        x1, y1, x2, y2 = t.rel_bbox()
        bx, by = t.rel_to_out(g, [x1, x2, x2, x1], [y1, y1, y2, y2])
        msp.add_lwpolyline(list(zip(bx, by)), close=True, dxfattribs={"layer": "TN_EMPRISE_MNT"})

        # -- courbes -------------------------------------------------------- #
        eq, master = self.eq, self.master = self._levels(t)
        zs = t.smoothed(o.contour_smoothing_passes)
        xs = t.col_to_rx(np.arange(t.w))
        ys = t.row_to_ry(np.arange(t.h))[::-1]
        gen = contourpy.contour_generator(xs, ys, np.ma.masked_invalid(zs[::-1]),
                                          line_type=contourpy.LineType.Separate)
        zmin, zmax = np.nanmin(zs), np.nanmax(zs)
        th = ctx.mm(1.8)
        label_spacing = ctx.mm(o.contour_label_spacing_mm)
        n_lines = 0
        for lvl in np.round(np.arange(math.ceil(zmin / eq) * eq, zmax + 1e-9, eq), 3):
            is_master = abs(lvl / master - round(lvl / master)) < 1e-6
            layer = "TN_COURBES_MAITRESSES" if is_master else "TN_COURBES_INTERMEDIAIRES"
            elev = 0.0 if o.flatten_2d else float(lvl)
            for line in gen.lines(float(lvl)):
                if len(line) < 2:
                    continue
                if np.hypot(*np.diff(line, axis=0).T).sum() < o.contour_min_length_m:
                    continue
                line = _rdp(line, o.contour_simplify_m)
                px, py = t.rel_to_out(g, line[:, 0], line[:, 1])
                pts = np.c_[px, py]
                closed = len(pts) > 3 and np.allclose(pts[0], pts[-1])
                if closed:
                    pts = pts[:-1]
                msp.add_lwpolyline(pts.tolist(), close=closed,
                                   dxfattribs={"layer": layer, "elevation": elev})
                n_lines += 1
                if is_master:
                    self._label_line(msp, pts, _fmt_z(lvl), th, label_spacing, elev)

        # -- points cotés : grille ronde + sommets UF ----------------------- #
        sp = o.spot_spacing_m
        gx = np.arange(math.ceil((t.cx + x1) / sp) * sp, t.cx + x2 + 1e-9, sp) - t.cx
        gy = np.arange(math.ceil((t.cy + y1) / sp) * sp, t.cy + y2 + 1e-9, sp) - t.cy
        GX, GY = np.meshgrid(gx, gy)
        ox, oy = t.rel_to_out(g, GX.ravel(), GY.ravel())
        pts = [np.c_[ox, oy]]
        if ctx.uf is not None:
            import shapely
            for poly in shapely.get_parts(ctx.uf):
                pts.append(np.asarray(poly.exterior.coords)[:-1])
        pts = np.vstack(pts)
        zz = t.sample_out(g, pts[:, 0], pts[:, 1])
        n_spots = 0
        for (x, y), z in zip(pts, zz):
            if not np.isfinite(z):
                continue
            zp = 0.0 if o.flatten_2d else float(z)
            msp.add_point((x, y, zp), dxfattribs={"layer": "TN_POINTS_COTES"})
            add_text(msp, _fmt_z(z), (x + ctx.mm(0.6), y + ctx.mm(0.6)), ctx.mm(1.6),
                     "TN_POINTS_COTES", align=TextEntityAlignment.BOTTOM_LEFT, z=zp)
            n_spots += 1

        return {"contour_lines": n_lines, "interval_m": eq, "master_m": master, "spots": n_spots}

    @staticmethod
    def _label_line(msp, pts, text, th, spacing, z):
        seg = np.hypot(*np.diff(pts, axis=0).T)
        cum = np.concatenate([[0], np.cumsum(seg)])
        total = cum[-1]
        if total < 6 * th:
            return
        n = max(1, int(total // spacing))
        for k in range(n):
            s = total * (k + 0.5) / n
            i = int(np.clip(np.searchsorted(cum, s) - 1, 0, len(pts) - 2))
            u = (s - cum[i]) / max(seg[i], 1e-9)
            p = pts[i] + u * (pts[i + 1] - pts[i])
            add_text(msp, text, p, th, "TN_COURBES_COTES",
                     rotation=readable_angle(*(pts[i + 1] - pts[i])), z=z)

    def cartouche_lines(self, ctx):
        t = ctx.terrain
        if t is None:
            return []
        millesime = t.payload.get("source_millesime")
        vdatum = t.payload.get("vertical_datum", "NGF-IGN69")
        lines = [f"Terrain naturel : {self.o.source_label}" + (f" ({millesime})" if millesime else "")
                 + f" – altitudes {vdatum} (m) – résolution {t.res:g} m"]
        if self.eq:
            lines.append(f"Courbes : équidistance {self.eq:g} m, maîtresses {self.master:g} m "
                         f"(grille lissée pour les courbes uniquement)")
        lines.append("MNT = sol nu interpolé sous bâti et végétation dense ; ne remplace pas "
                     "un levé de géomètre-expert.")
        return lines


# --------------------------------------------------------------------------- #
# Contributeur 3D
# --------------------------------------------------------------------------- #

def _build_tin(t: TerrainModel, max_error: float):
    valid = np.isfinite(t.z)
    try:
        from pydelatin import Delatin
        tin = Delatin(t.fill_nan().astype(np.float32), height=t.h, width=t.w, max_error=max_error)
        cols = tin.vertices[:, 0].astype(int)
        rows = (t.h - 1 - tin.vertices[:, 1]).astype(int)  # pydelatin : axe Y inversé
        tris = tin.triangles.astype(np.int64)
        method = f"TIN simplifié (tolérance {max_error} m)"
    except ImportError:
        rr, cc = np.mgrid[0:t.h, 0:t.w]
        rows, cols = rr.ravel(), cc.ravel()
        idx = np.arange(t.h * t.w).reshape(t.h, t.w)
        i0, i1 = idx[:-1, :-1].ravel(), idx[:-1, 1:].ravel()
        i2, i3 = idx[1:, :-1].ravel(), idx[1:, 1:].ravel()
        zf = t.z.ravel()
        d03 = np.nan_to_num(np.abs(zf[i0] - zf[i3]), nan=np.inf)
        d12 = np.nan_to_num(np.abs(zf[i1] - zf[i2]), nan=np.inf)
        use03 = (d03 <= d12)[:, None]
        tris = np.concatenate([np.where(use03, np.c_[i0, i1, i3], np.c_[i0, i1, i2]),
                               np.where(use03, np.c_[i0, i3, i2], np.c_[i1, i3, i2])])
        method = "grille complète (pydelatin absent)"
    ok = valid[rows, cols][tris].all(axis=1)
    cr = np.clip(np.round(rows[tris].mean(1)).astype(int), 0, t.h - 1)
    cc_ = np.clip(np.round(cols[tris].mean(1)).astype(int), 0, t.w - 1)
    ok &= valid[cr, cc_]
    return cols, rows, tris[ok], method


def _chunks(tris, rows, cols, max_v: int) -> Iterator[np.ndarray]:
    order = np.lexsort((cols[tris].mean(1), rows[tris].mean(1) // 64))
    tris = tris[order]
    step, i = max(1, max_v // 2), 0
    while i < len(tris):
        n = step
        while n > 1 and len(np.unique(tris[i:i + n])) > max_v:
            n //= 2
        yield tris[i:i + n]
        i += n


class Terrain3DContributor(Contributor):
    name = "terrain_3d"

    def __init__(self, options: TerrainOptions | None = None):
        self.o = options or TerrainOptions()
        self.method = None

    def write(self, doc, ctx: ExportContext) -> dict:
        t: TerrainModel = ctx.terrain
        if t is None:
            return {}
        o, msp = self.o, doc.modelspace()
        if o.mesh_entity == "mesh" and ctx.options.dxf_version < "R2010":
            raise ValueError("L'entité MESH nécessite DXF R2010 ou plus.")
        ensure_layer(doc, LayerSpec("TN_MAILLAGE_3D", 8, lineweight=9,
                                    description="Maillage du terrain naturel (MNT LiDAR)"))
        cols, rows, tris, self.method = _build_tin(t, o.mesh_max_error_m)
        vx, vy = t.rel_to_out(ctx.georef, t.col_to_rx(cols), t.row_to_ry(rows))
        verts = np.c_[vx, vy, t.z[rows, cols]]
        attrs = {"layer": "TN_MAILLAGE_3D"}
        n = 0
        if o.mesh_entity == "3dface":
            for a, b, c in tris:
                msp.add_3dface([verts[a], verts[b], verts[c], verts[c]], dxfattribs=attrs)
            n = len(tris)
        else:
            for chunk in _chunks(tris, rows, cols, o.max_vertices_per_entity):
                used, inv = np.unique(chunk, return_inverse=True)
                mb = MeshBuilder()
                mb.add_mesh(vertices=[tuple(v) for v in verts[used]],
                            faces=[tuple(f) for f in inv.reshape(-1, 3)])
                (mb.render_mesh if o.mesh_entity == "mesh" else mb.render_polyface)(
                    msp, dxfattribs=attrs)
                n += 1
        return {"method": self.method, "triangles": int(len(tris)), "entities": n}

    def cartouche_lines(self, ctx):
        return [f"Maillage 3D : {self.method}"] if self.method else []
