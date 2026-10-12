# -*- coding: utf-8 -*-
"""
Cote de seuil PPRI (altitude absolue en m NGF) croisée avec le MNT.

  CoteSeuilAnalysis        : calcul partagé (zones, grille de cote, surélévation, stats UF)
  CoteSeuil2DContributor   : zones de cote + étiquettes, terrain sous la cote (hachuré),
                             ligne TN = cote, courbes d'iso-surélévation, bloc prescriptions
  CoteSeuil3DContributor   : plan translucide à la cote, volume sous la cote sur l'UF,
                             ligne d'intersection plan / terrain

Surélévation = cote de seuil - TN  (> 0 : terrain sous la cote, plancher à surélever).
Données attendues : features GeoJSON (EPSG:2154 par défaut) avec la cote dans `codezone`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import contourpy
import numpy as np
import shapely
from ezdxf.enums import TextEntityAlignment
from ezdxf.math.triangulation import mapbox_earcut_2d
from ezdxf.render import MeshVertexMerger
from shapely.geometry import Polygon, shape
from shapely.ops import polylabel

from .core import (Contributor, ExportContext, LayerSpec, LegendEntry, add_text,
                   ensure_layer, label_along_line)

BLUE = (37, 99, 235)
BLUE_LIGHT = (96, 165, 250)
BLUE_DARK = (29, 78, 216)

PRESCRIPTIONS_LATRESNE = """AU-DESSUS DE LA COTE DE SEUIL :
- planchers des pièces d'habitation (séjour, chambre, bureau, cuisine, SdB) et hébergement hôtelier
- planchers des locaux d'activité avec présence humaine ou biens sensibles à l'eau
- locaux techniques ; équipements vulnérables (chauffage, ascenseurs, tableaux électriques, réseaux)
- dispositifs de coupure des réseaux ; évents des citernes ; hauteur à l'égout du toit
- accès et aérations des garages souterrains (admis en zone bleu clair uniquement)
EXEMPTÉS : abris de jardin, hangars agricoles, locaux de piscine, halls d'entrée, garages,
aires de stationnement, caves cuvelées (zone bleue), vestiaires sportifs, sanitaires publics,
locaux OM (conteneurs résilients)
SOUS LA COTE : parties résilientes à l'inondation, matériaux hydrofuges et hydrophobes
(fondations comprises), structure dimensionnée pour la poussée de l'eau
Rampes d'accès sur remblai admises si strictement nécessaires, avec mesures compensatoires.
AUCUNE DÉROGATION pour les habitations et les bureaux.
OAE / OAEIP : retenir la cote la plus haute entre le PPR et l'étude hydraulique 2100."""


@dataclass
class CoteSeuilOptions:
    value_field: str = "codezone"
    src_epsg: int = 2154
    plan_label: str = "PPRi de Latresne (règlement février 2022)"
    prescriptions_text: str = PRESCRIPTIONS_LATRESNE
    iso_step_m: float = 0.5
    smoothing_passes: int = 2          # lissage du TN pour les courbes (pas pour les stats)
    near_threshold_m: float = 0.20     # précision verticale indicative du MNT LiDAR
    ramp_slope: float = 0.05           # pente de rampe PMR indicative
    plane_transparency: float = 0.6
    volume_transparency: float = 0.75
    hatch_transparency: float = 0.75
    draw_zone_limits: bool = True      # False si la couche est déjà exportée via le catalogue


def load_cote_seuil_features(cursor, uf_wkt_2154: str, buffer_m: float = 200.0,
                             table: str = "latresne.cote_de_seuil_ppri") -> list[dict]:
    """Lit les secteurs autour de l'UF (curseur DB-API, ex. psycopg). Géométries en 2154."""
    cursor.execute(
        f"""SELECT codezone, nom, typereg, nomfic, urlfic, srce_annee, precision,
                   ST_AsGeoJSON(geom_2154, 3)
            FROM {table}
            WHERE geom_2154 && ST_Expand(ST_GeomFromText(%s, 2154), %s)
              AND ST_DWithin(geom_2154, ST_GeomFromText(%s, 2154), %s)""",
        (uf_wkt_2154, buffer_m, uf_wkt_2154, buffer_m),
    )
    keys = ["codezone", "nom", "typereg", "nomfic", "urlfic", "srce_annee", "precision"]
    return [{"type": "Feature", "geometry": json.loads(r[-1]), "properties": dict(zip(keys, r[:-1]))}
            for r in cursor.fetchall() if r[-1]]


def _fmt(v: float, nd: int = 2) -> str:
    return f"{v:.{nd}f}".replace(".", ",")


# --------------------------------------------------------------------------- #
# Analyse partagée
# --------------------------------------------------------------------------- #

class CoteSeuilAnalysis:
    def __init__(self, features: list[dict], options: CoteSeuilOptions | None = None):
        self.features = features or []
        self.o = options or CoteSeuilOptions()
        self._ready = False

    def prepare(self, ctx: ExportContext):
        if self._ready:
            return self
        o, g, t = self.o, ctx.georef, ctx.terrain
        self.res = t.res if t is not None else 1.0

        # emprise utile = MNT ∩ périmètre d'étude
        if t is not None:
            x1, y1, x2, y2 = t.rel_bbox()
            cx, cy = t.rel_to_out(g, [x1, x2, x2, x1], [y1, y1, y2, y2])
            area = Polygon(np.c_[cx, cy])
        else:
            area = shapely.box(*ctx.emprise)
        if ctx.study_area is not None:
            area = area.intersection(ctx.study_area)

        # zones (géométrie de sortie, cote)
        self.zones: list[tuple] = []
        self.meta: dict = {}
        for f in self.features:
            props = f.get("properties") or {}
            try:
                cote = float(props[o.value_field])
                geom = g.geom_forward(shape(f["geometry"]), o.src_epsg)
            except (KeyError, TypeError, ValueError):
                continue
            if not np.isfinite(cote):
                continue
            if not geom.is_valid:
                geom = shapely.make_valid(geom)
            geom = geom.intersection(area)
            if geom.is_empty:
                continue
            self.zones.append((geom, cote, props))
            for k in ("nomfic", "urlfic", "srce_annee"):
                if props.get(k) and k not in self.meta:
                    self.meta[k] = props[k]

        self.lines: list[tuple[float, float, np.ndarray]] = []   # (niveau, cote, pts sortie)
        self.under: list[tuple] = []                              # (polygone sortie, cote)
        self.stats: dict = {}
        if t is None or not self.zones:
            self._ready = True
            return self

        # grille de cote (cote la plus haute en cas de recouvrement)
        RX, RY = np.meshgrid(t.col_to_rx(np.arange(t.w)), t.row_to_ry(np.arange(t.h)))
        X, Y = t.rel_to_out(g, RX, RY)
        cote_grid = np.full(t.z.shape, np.nan)
        for geom, cote, _ in self.zones:
            m = shapely.contains_xy(geom, X, Y)
            cote_grid[m] = np.fmax(cote_grid[m], cote)
        self.cote_grid = cote_grid
        sur_s = cote_grid - t.smoothed(o.smoothing_passes)

        def rel2out(c: np.ndarray) -> np.ndarray:
            return np.column_stack(t.rel_to_out(g, c[:, 0], c[:, 1]))

        xs = t.col_to_rx(np.arange(t.w))
        ys = t.row_to_ry(np.arange(t.h))[::-1]
        for cote in sorted({c for _, c, _ in self.zones}):
            zgeom = shapely.union_all([z for z, c, _ in self.zones if c == cote])
            s = np.where(np.isclose(cote_grid, cote), sur_s, np.nan)[::-1]
            if not np.isfinite(s).any():
                continue
            ma = np.ma.masked_invalid(s)
            # terrain sous la cote (surélévation > 0)
            gen = contourpy.contour_generator(xs, ys, ma, fill_type=contourpy.FillType.OuterOffset,
                                              line_type=contourpy.LineType.Separate)
            pts_list, offs_list = gen.filled(0.0, np.inf)
            polys = []
            for pts, offs in zip(pts_list, offs_list):
                rings = [pts[offs[i]:offs[i + 1]] for i in range(len(offs) - 1)]
                rings = [r for r in rings if len(r) >= 4]
                if rings:
                    p = Polygon(rings[0], rings[1:])
                    polys.append(p if p.is_valid else shapely.make_valid(p))
            if polys:
                u = shapely.transform(shapely.union_all(polys), rel2out).intersection(zgeom)
                for p in shapely.get_parts(u):
                    if p.geom_type == "Polygon" and p.area > self.res ** 2:
                        self.under.append((p, cote))
            # ligne TN = cote (niveau 0) + iso-surélévation
            smax = float(np.nanmax(s))
            levels = [0.0] + list(np.arange(o.iso_step_m, smax + 1e-9, o.iso_step_m))
            for lvl in levels:
                for line in gen.lines(float(lvl)):
                    if len(line) >= 2 and np.hypot(*np.diff(line, axis=0).T).sum() > 2 * self.res:
                        self.lines.append((float(lvl), cote, rel2out(line)))

        # statistiques sur l'UF (TN brut)
        if ctx.uf is not None:
            inside = shapely.contains_xy(ctx.uf, X, Y)
            n_in = int(inside.sum())
            if n_in:
                z_in, c_in = t.z[inside], cote_grid[inside]
                ok = np.isfinite(z_in) & np.isfinite(c_in)
                sur = c_in[ok] - z_in[ok]
                self.stats = {
                    "uf_cells": n_in,
                    "frac_in_zone": float(np.isfinite(c_in).sum() / n_in),
                    "cotes": sorted(set(np.round(c_in[np.isfinite(c_in)], 2).tolist())),
                    "tn_min": float(np.nanmin(z_in)), "tn_max": float(np.nanmax(z_in)),
                    "tn_mean": float(np.nanmean(z_in)),
                }
                if sur.size:
                    self.stats.update({
                        "sur_min": float(sur.min()), "sur_max": float(sur.max()),
                        "sur_mean": float(sur[sur > 0].mean()) if (sur > 0).any() else 0.0,
                        "frac_under": float((sur > 0).mean()),
                        "area_under_m2": float((sur > 0).sum() * self.res ** 2),
                        "near": bool((np.abs(sur) < o.near_threshold_m).any()),
                    })
        self._ready = True
        return self

    # -- textes -------------------------------------------------------------- #
    def summary_lines(self) -> list[str]:
        s, o = self.stats, self.o
        if not s:
            return ["Aucun secteur de cote de seuil sur l'unité foncière."]
        out = []
        if s.get("cotes"):
            out.append("Cote(s) de seuil sur l'UF : "
                       + " / ".join(f"{_fmt(c)} m NGF" for c in s["cotes"]))
        out.append(f"TN sur l'UF (MNT LiDAR) : {_fmt(s['tn_min'])} à {_fmt(s['tn_max'])} m NGF "
                   f"(moy. {_fmt(s['tn_mean'])})")
        if "sur_min" in s:
            if s["sur_max"] <= 0:
                out.append("Terrain naturel entièrement au-dessus de la cote de seuil sur l'UF.")
            else:
                start = ("0 (une partie de l'UF est déjà au-dessus)" if s["sur_min"] <= 0
                         else _fmt(s["sur_min"]))
                out.append(f"Surélévation des planchers concernés : {start} à "
                           f"{_fmt(s['sur_max'])} m (moy. {_fmt(s['sur_mean'])} sur la partie "
                           f"sous la cote) – "
                           f"{s['frac_under']:.0%} de l'UF sous la cote "
                           f"(≈ {s['area_under_m2']:.0f} m²)")
                best = max(s["sur_min"], 0.0)
                if best > 0:
                    out.append(f"Rampe PMR indicative ({o.ramp_slope:.0%}) au point le plus favorable : "
                               f"≈ {best / o.ramp_slope:.0f} m")
            if s.get("near"):
                out.append(f"ATTENTION : écart TN / cote < {o.near_threshold_m * 100:.0f} cm sur une "
                           f"partie de l'UF, à confirmer par un levé de géomètre.")
        if s["frac_in_zone"] < 0.999:
            out.append(f"{1 - s['frac_in_zone']:.0%} de l'UF hors secteur de cote de seuil.")
        return out


# --------------------------------------------------------------------------- #
# 2D
# --------------------------------------------------------------------------- #

class CoteSeuil2DContributor(Contributor):
    name = "cote_seuil_2d"

    def __init__(self, analysis: CoteSeuilAnalysis):
        self.a = analysis

    def write(self, doc, ctx: ExportContext) -> dict:
        a = self.a.prepare(ctx)
        o, msp = a.o, doc.modelspace()
        for spec in (
            LayerSpec("RISQ_PPRI_COTE_SEUIL", 5, BLUE, 25, "KER_MIXTE",
                      "Secteurs de cote de seuil PPRi (m NGF)"),
            LayerSpec("RISQ_PPRI_COTE_SEUIL_ETIQ", 5, BLUE, 13, description="Cotes de seuil (étiquettes)"),
            LayerSpec("RISQ_PPRI_SOUS_SEUIL_HACH", 5, BLUE_LIGHT, 0,
                      description="Terrain naturel sous la cote de seuil (hachures)",
                      transparency=o.hatch_transparency),
            LayerSpec("RISQ_PPRI_LIGNE_SEUIL", 5, BLUE_DARK, 50,
                      description="Ligne TN = cote de seuil"),
            LayerSpec("RISQ_PPRI_ISO_SURELEVATION", 5, BLUE_LIGHT, 18, "KER_TIRETS",
                      f"Courbes d'iso-surélévation (pas {o.iso_step_m:g} m)"),
            LayerSpec("RISQ_PPRI_ISO_SURELEVATION_COTES", 5, BLUE_LIGHT, 13,
                      description="Valeurs des courbes d'iso-surélévation"),
            LayerSpec("RISQ_PPRI_PRESCRIPTIONS", 7, None, 13,
                      description="Bloc prescriptions cote de seuil"),
        ):
            ensure_layer(doc, spec)

        for poly, _ in a.under:  # hachures d'abord
            h = msp.add_hatch(color=256, dxfattribs={"layer": "RISQ_PPRI_SOUS_SEUIL_HACH"})
            h.paths.add_polyline_path(list(poly.exterior.coords)[:-1], is_closed=True, flags=1)
            for hole in poly.interiors:
                h.paths.add_polyline_path(list(hole.coords)[:-1], is_closed=True, flags=0)

        th = ctx.mm(1.8)
        n_lines = 0
        for lvl, cote, pts in a.lines:
            layer = "RISQ_PPRI_LIGNE_SEUIL" if lvl == 0 else "RISQ_PPRI_ISO_SURELEVATION"
            msp.add_lwpolyline(pts.tolist(), dxfattribs={"layer": layer})
            text = f"TN = {_fmt(cote)}" if lvl == 0 else f"+{_fmt(lvl)} m"
            label_along_line(msp, pts, text, th, ctx.mm(120),
                             "RISQ_PPRI_LIGNE_SEUIL" if lvl == 0 else "RISQ_PPRI_ISO_SURELEVATION_COTES")
            n_lines += 1

        if o.draw_zone_limits:
            for geom, cote, props in a.zones:
                for p in shapely.get_parts(geom):
                    if p.geom_type != "Polygon":
                        continue
                    e = msp.add_lwpolyline(list(p.exterior.coords)[:-1], close=True,
                                           dxfattribs={"layer": "RISQ_PPRI_COTE_SEUIL"})
                    if props.get("urlfic"):
                        e.set_hyperlink(props["urlfic"], description="Règlement PPRi")
                    text = f"Cote de seuil {_fmt(cote)} m NGF"
                    if p.area > (len(text) * ctx.mm(2.2) * 0.75) * ctx.mm(2.2) * 3:
                        q = polylabel(p, tolerance=ctx.mm(1))
                        add_text(msp, text, (q.x, q.y), ctx.mm(2.2), "RISQ_PPRI_COTE_SEUIL_ETIQ")

        # bloc prescriptions : colonne à droite de la légende
        minx, miny, maxx, maxy = ctx.emprise
        lines = [f"PRESCRIPTIONS COTE DE SEUIL – {o.plan_label}", "(résumé indicatif, seul le "
                 "règlement opposable fait foi)", "", *a.summary_lines(), "", o.prescriptions_text]
        if a.meta.get("urlfic"):
            lines += ["", f"Règlement : {a.meta['urlfic']}"]
        msp.add_mtext("\n".join(lines), dxfattribs={
            "layer": "RISQ_PPRI_PRESCRIPTIONS", "char_height": ctx.mm(2.0),
            "insert": (maxx + ctx.mm(150), maxy, 0), "width": ctx.mm(120), "attachment_point": 1,
        })
        return {"zones": len(a.zones), "under_polygons": len(a.under), "lines": n_lines,
                "uf_stats": a.stats}

    def legend_entries(self, ctx):
        fam, title = "Risques", "Cote de seuil PPRi"
        return [
            LegendEntry(fam, title, "Secteur de cote de seuil", "RISQ_PPRI_COTE_SEUIL", "lin", BLUE),
            LegendEntry(fam, title, "Terrain naturel sous la cote", "RISQ_PPRI_SOUS_SEUIL_HACH",
                        "surf", BLUE_LIGHT),
            LegendEntry(fam, title, "Ligne TN = cote de seuil", "RISQ_PPRI_LIGNE_SEUIL", "lin", BLUE_DARK),
            LegendEntry(fam, title, f"Iso-surélévation (pas {self.a.o.iso_step_m:g} m)",
                        "RISQ_PPRI_ISO_SURELEVATION", "lin", BLUE_LIGHT),
        ]

    def cartouche_lines(self, ctx):
        src = self.a.meta.get("srce_annee")
        return [f"Cote de seuil : {self.a.o.plan_label}" + (f", source {src}" if src else "")
                + " – surélévation = cote - TN (MNT LiDAR), indicative"]


# --------------------------------------------------------------------------- #
# 3D
# --------------------------------------------------------------------------- #

def _earcut_faces(poly, z: float):
    holes = [list(h.coords)[:-1] for h in poly.interiors]
    for tri in mapbox_earcut_2d(list(poly.exterior.coords)[:-1], holes or None):
        yield [(v.x, v.y, z) for v in tri]


class CoteSeuil3DContributor(Contributor):
    name = "cote_seuil_3d"

    def __init__(self, analysis: CoteSeuilAnalysis):
        self.a = analysis

    def write(self, doc, ctx: ExportContext) -> dict:
        a = self.a.prepare(ctx)
        o, msp, t = a.o, doc.modelspace(), ctx.terrain
        ensure_layer(doc, LayerSpec("RISQ_PPRI_PLAN_SEUIL_3D", 5, BLUE, 13,
                                    description="Plan de la cote de seuil (m NGF)",
                                    transparency=o.plane_transparency))
        ensure_layer(doc, LayerSpec("RISQ_PPRI_VOLUME_SOUS_SEUIL_3D", 5, BLUE_LIGHT, 9,
                                    description="Volume entre TN et cote de seuil sur l'UF",
                                    transparency=o.volume_transparency))
        ensure_layer(doc, LayerSpec("RISQ_PPRI_LIGNE_SEUIL_3D", 5, BLUE_DARK, 35,
                                    description="Intersection plan de seuil / terrain"))

        # 1. plan translucide à la cote
        n_plane = 0
        for geom, cote, _ in a.zones:
            mb = MeshVertexMerger()
            for p in shapely.get_parts(geom):
                if p.geom_type == "Polygon":
                    for f in _earcut_faces(p, cote):
                        mb.add_face(f)
            if mb.faces:
                mb.render_polyface(msp, dxfattribs={"layer": "RISQ_PPRI_PLAN_SEUIL_3D"})
                n_plane += 1
                big = max((p for p in shapely.get_parts(geom) if p.geom_type == "Polygon"),
                          key=lambda p: p.area)
                q = polylabel(big, tolerance=ctx.mm(1))
                add_text(msp, f"Cote de seuil {_fmt(cote)} m NGF", (q.x, q.y), ctx.mm(3),
                         "RISQ_PPRI_PLAN_SEUIL_3D", z=cote)

        # 2. volume TN -> cote sur l'UF (dessus + parois)
        n_vol = 0
        if ctx.uf is not None and t is not None:
            for poly, cote in a.under:
                part = poly.intersection(ctx.uf)
                for p in shapely.get_parts(part):
                    if p.geom_type != "Polygon" or p.area < a.res ** 2:
                        continue
                    mb = MeshVertexMerger()
                    for f in _earcut_faces(p, cote):
                        mb.add_face(f)
                    for ring in [p.exterior, *p.interiors]:
                        pts = self._densify(np.asarray(ring.coords), a.res)
                        z = t.sample_out(ctx.georef, pts[:, 0], pts[:, 1])
                        z = np.where(np.isfinite(z), np.minimum(z, cote), cote)
                        for i in range(len(pts) - 1):
                            if cote - z[i] < 0.01 and cote - z[i + 1] < 0.01:
                                continue
                            (x0, y0), (x1, y1) = pts[i], pts[i + 1]
                            mb.add_face([(x0, y0, z[i]), (x1, y1, z[i + 1]),
                                         (x1, y1, cote), (x0, y0, cote)])
                    mb.render_polyface(msp, dxfattribs={"layer": "RISQ_PPRI_VOLUME_SOUS_SEUIL_3D"})
                    n_vol += 1

        # 3. ligne d'intersection plan / terrain
        n_line = 0
        for lvl, cote, pts in a.lines:
            if lvl == 0:
                msp.add_polyline3d(np.c_[pts, np.full(len(pts), cote)].tolist(),
                                   dxfattribs={"layer": "RISQ_PPRI_LIGNE_SEUIL_3D"})
                n_line += 1
        return {"planes": n_plane, "volumes": n_vol, "intersection_lines": n_line}

    @staticmethod
    def _densify(coords: np.ndarray, step: float) -> np.ndarray:
        out = []
        for a, b in zip(coords[:-1], coords[1:]):
            n = max(1, int(np.ceil(np.hypot(*(b - a)) / step)))
            out.append(a + (np.arange(n)[:, None] / n) * (b - a))
        out.append(coords[-1:])
        return np.vstack(out)

    def cartouche_lines(self, ctx):
        return ["Plan bleu translucide = cote de seuil PPRi ; volume bleu = tranche entre TN et "
                "cote sur l'UF (parties résilientes / exemptées uniquement)."]