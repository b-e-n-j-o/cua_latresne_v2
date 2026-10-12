# -*- coding: utf-8 -*-
"""
Contributeurs SIG :
  - SigLayersContributor : couches réglementaires pilotées par le catalogue carto
                           (même payload que la carte MapLibre : prepare_layers_payload)
  - Cadastre2DContributor : UF, parcelles de l'UF + numéros, périmètre d'étude
  - Cadastre3DContributor : UF + couches choisies drapées sur le MNT (vue 3D uniquement)

Convention de calques (2D, tout à Z=0) :
  <PREFIXE>_<LAYER_ID>[_<GROUPE>]          limites / lignes / symboles
  <PREFIXE>_<LAYER_ID>[_<GROUPE>]_HACH     hachures (gel global : filtre *_HACH)
  <PREFIXE>_<LAYER_ID>_ETIQ                étiquettes
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import numpy as np
import shapely
from ezdxf import const
from ezdxf.enums import TextEntityAlignment
from shapely.geometry import shape
from shapely.ops import polylabel

from .core import (Contributor, ExportContext, LayerSpec, LegendEntry, add_text,
                   ensure_layer, hex_to_rgb, layer_name, nearest_aci, readable_angle,
                   set_xdata, slug)

FAMILY_PREFIX = {
    "zonages_plu": "PLU",
    "prescriptions": "PRESC",
    "informations": "INFO",
    "servitudes": "SUP",
    "risques": "RISQ",
    "environnement": "ENV",
    "reseaux": "RES",
    "cadastre": "CAD",
}
LINK_FIELDS = ("urlfic", "url", "lien", "urlreg", "url_reglement", "nomfic_url")
POINT_BLOCK = "KER_PT"


@dataclass
class SigOptions:
    src_epsg: int = 4326                               # GeoJSON du payload carto
    scope: Literal["alentours", "uf"] = "alentours"    # comme le sélecteur de la carte
    only_visible_by_default: bool = False              # True : seulement les couches "on"
    include_layers: set[str] | None = None             # None = toutes
    exclude_layers: set[str] = field(default_factory=set)
    hatch_mode: Literal["solid", "pattern", "none"] = "solid"
    hatch_transparency: float = 0.7
    hatch_pattern: str = "ANSI31"
    max_sublayers: int = 15                            # au-delà : un seul calque + étiquettes
    labels: bool = True
    xdata: bool = True
    hyperlinks: bool = True


def _parts(geom):
    for g in shapely.get_parts(geom):
        if isinstance(g, shapely.GeometryCollection):
            yield from _parts(g)
        elif not g.is_empty:
            yield g


class SigLayersContributor(Contributor):
    name = "sig"

    def __init__(self, layers: list[dict], catalogue: dict | None = None,
                 options: SigOptions | None = None):
        self.layers = layers or []
        self.cat = (catalogue or {}).get("layers", {})
        self.o = options or SigOptions()
        self._legend: dict[tuple[str, str], LegendEntry] = {}
        self._names: dict[str, tuple[str, str]] = {}

    # -- config ------------------------------------------------------------- #
    def _cfg(self, layer_id: str) -> dict:
        return self.cat.get(layer_id, {})

    def _selected(self, layer: dict) -> bool:
        lid = layer.get("layer_id")
        o = self.o
        if lid in o.exclude_layers:
            return False
        if o.include_layers is not None and lid not in o.include_layers:
            return False
        if o.only_visible_by_default and not self._cfg(lid).get("on"):
            return False
        return bool(layer.get("features"))

    def _unique_name(self, name: str, owner: tuple[str, str]) -> str:
        base, i = name, 2
        while name in self._names and self._names[name] != owner:
            name = f"{base}_{i}"
            i += 1
        self._names[name] = owner
        return name

    @staticmethod
    def _kind(layer: dict) -> str:
        gt = layer.get("geom_type")
        return "lin" if gt == "lineaire" else "pct" if gt == "ponctuel" else "surf"

    def _link(self, lid: str, props: dict) -> str | None:
        field_name = self._cfg(lid).get("dxf", {}).get("link")
        for k in ([field_name] if field_name else []) + list(LINK_FIELDS):
            v = props.get(k)
            if isinstance(v, str) and v.startswith("http"):
                return v
        return None

    # -- écriture ----------------------------------------------------------- #
    def write(self, doc, ctx: ExportContext) -> dict:
        o, g, msp = self.o, ctx.georef, doc.modelspace()
        clip = ctx.uf if o.scope == "uf" else ctx.study_area
        self._ensure_point_block(doc)

        prepared = []  # (layer, kind, [(feat_props, geom, sublayer, rgb, label)])
        for layer in self.layers:
            if not self._selected(layer):
                continue
            lid = layer["layer_id"]
            cfg = self._cfg(lid)
            dxf_cfg = cfg.get("dxf", {})
            prefix = dxf_cfg.get("prefix") or FAMILY_PREFIX.get(cfg.get("family"), "DIV")
            short = "_".join(t for t in slug(lid).split("_") if t != slug(prefix)) or slug(lid)
            base = layer_name(prefix, dxf_cfg.get("name") or short)
            kind = self._kind(layer)
            items = {i["key"]: i for i in layer.get("legend_items") or []}
            use_sub = bool(layer.get("filterable")) and 1 < len(items) <= o.max_sublayers
            tip = cfg.get("tip")
            family_title = layer.get("family_title") or "Divers"
            # hachures : désactivables par couche (catalogue dxf.hatch), jamais sur le cadastre
            do_hatch = (kind == "surf" and o.hatch_mode != "none"
                        and dxf_cfg.get("hatch", cfg.get("family") != "cadastre"))
            rows = []
            for f in layer["features"]:
                if not f.get("geometry"):
                    continue
                props = f.get("properties") or {}
                try:
                    geom = g.geom_forward(shape(f["geometry"]), o.src_epsg)
                except Exception:
                    continue
                if not geom.is_valid:
                    geom = shapely.make_valid(geom)
                if clip is not None:
                    if not geom.intersects(clip):
                        continue
                    geom = geom.intersection(clip)
                if geom.is_empty:
                    continue
                key = props.get("_studyKey")
                item = items.get(key, {})
                rgb = hex_to_rgb(props.get("_studyColor") or item.get("color"))
                if use_sub:
                    label_legend = item.get("label") or props.get("_studyLabel") or str(key)
                elif len(items) == 1:
                    label_legend = next(iter(items.values())).get("label") or layer.get("title", lid)
                else:
                    label_legend = layer.get("title", lid)
                sub = layer_name(base, key) if use_sub and key not in (None, "") else base
                sub = self._unique_name(sub, (lid, str(key) if use_sub else ""))
                desc = f"{layer.get('title', lid)}" + (f" – {label_legend}" if use_sub else "")
                lw = {"surf": 25, "lin": 35, "pct": 18}[kind]
                lt = dxf_cfg.get("linetype", "Continuous")
                ensure_layer(doc, LayerSpec(sub, nearest_aci(rgb), rgb, lw, lt, desc))
                if do_hatch:
                    ensure_layer(doc, LayerSpec(f"{sub}_HACH", nearest_aci(rgb), rgb, 0,
                                                description=f"{desc} (hachures)",
                                                transparency=o.hatch_transparency))
                text = props.get(tip) if tip else None
                text = str(text if text not in (None, "") else props.get("_studyLabel") or "")[:40]
                rows.append((props, geom, sub, rgb, text, do_hatch))
                self._legend.setdefault((lid, sub), LegendEntry(
                    family_title, layer.get("title", lid), label_legend, sub,
                    "lin" if (kind == "surf" and not do_hatch) else kind, rgb))
            if rows:
                prepared.append((layer, kind, base, rows))

        # passe 1 : hachures (dessous)
        n_hatch = 0
        if o.hatch_mode != "none":
            for layer, kind, base, rows in prepared:
                if kind != "surf":
                    continue
                for props, geom, sub, rgb, _, do_hatch in rows:
                    if not do_hatch:
                        continue
                    for poly in _parts(geom):
                        if poly.geom_type != "Polygon":
                            continue
                        self._hatch(msp, poly, f"{sub}_HACH", ctx, self._link(layer["layer_id"], props))
                        n_hatch += 1

        # passe 2 : limites, lignes, symboles, étiquettes
        n_ent = n_lab = 0
        for layer, kind, base, rows in prepared:
            lid = layer["layer_id"]
            lab_layer = f"{base}_ETIQ"
            if o.labels:
                ensure_layer(doc, LayerSpec(lab_layer, 7, lineweight=13,
                                            description=f"{layer.get('title', lid)} (étiquettes)"))
            for props, geom, sub, rgb, text, _ in rows:
                link = self._link(lid, props) if o.hyperlinks else None
                xd = {"layer_id": lid, "fid": props.get("_fid"), "libelle": props.get("_studyLabel"),
                      **{k: v for k, v in props.items() if not str(k).startswith("_")
                         and isinstance(v, (str, int, float))}}
                for part in _parts(geom):
                    for e in self._draw(msp, part, sub, ctx):
                        if link:
                            e.set_hyperlink(link, description=str(props.get("_studyLabel") or "")[:120])
                        if o.xdata:
                            set_xdata(e, xd)
                        n_ent += 1
                if o.labels and text:
                    n_lab += self._label(msp, geom, text, lab_layer, ctx)
        return {"layers": len(prepared), "entities": n_ent, "hatches": n_hatch, "labels": n_lab,
                "dxf_layers": len(self._names)}

    # -- primitives --------------------------------------------------------- #
    def _hatch(self, msp, poly, layer, ctx, link):
        hatch = msp.add_hatch(color=256, dxfattribs={"layer": layer})
        if self.o.hatch_mode == "pattern":
            hatch.set_pattern_fill(self.o.hatch_pattern, color=256, scale=ctx.mm(2.5) / 3.175)
        hatch.paths.add_polyline_path(list(poly.exterior.coords)[:-1], is_closed=True,
                                      flags=const.BOUNDARY_PATH_EXTERNAL)
        for hole in poly.interiors:
            hatch.paths.add_polyline_path(list(hole.coords)[:-1], is_closed=True,
                                          flags=const.BOUNDARY_PATH_DEFAULT)
        if link:
            hatch.set_hyperlink(link)

    def _draw(self, msp, part, layer, ctx):
        t = part.geom_type
        if t == "Polygon":
            for ring in [part.exterior, *part.interiors]:
                yield msp.add_lwpolyline(list(ring.coords)[:-1], close=True,
                                         dxfattribs={"layer": layer})
        elif t in ("LineString", "LinearRing"):
            yield msp.add_lwpolyline(list(part.coords), dxfattribs={"layer": layer})
        elif t == "Point":
            s = ctx.mm(1.2)
            yield msp.add_blockref(POINT_BLOCK, (part.x, part.y),
                                   dxfattribs={"layer": layer, "xscale": s, "yscale": s})

    def _label(self, msp, geom, text, layer, ctx) -> int:
        th = ctx.mm(2.0)
        parts = list(_parts(geom))
        polys = [p for p in parts if p.geom_type == "Polygon"]
        lines = [p for p in parts if p.geom_type == "LineString"]
        if polys:
            poly = max(polys, key=lambda p: p.area)
            w = len(text) * th * 0.75
            if poly.area < w * th * 2 or min(poly.bounds[2] - poly.bounds[0],
                                               poly.bounds[3] - poly.bounds[1]) < th * 1.5:
                return 0
            p = polylabel(poly, tolerance=th / 2)
            add_text(msp, text, (p.x, p.y), th, layer)
            return 1
        if lines:
            line = max(lines, key=lambda ln: ln.length)
            if line.length < len(text) * th:
                return 0
            p0 = line.interpolate(0.48, normalized=True)
            p1 = line.interpolate(0.52, normalized=True)
            add_text(msp, text, (p0.x, p0.y), th * 0.9, layer,
                     rotation=readable_angle(p1.x - p0.x, p1.y - p0.y),
                     align=TextEntityAlignment.BOTTOM_CENTER)
            return 1
        pts = [p for p in parts if p.geom_type == "Point"]
        if pts:
            add_text(msp, text, (pts[0].x + ctx.mm(2), pts[0].y), th * 0.9, layer,
                     align=TextEntityAlignment.MIDDLE_LEFT)
            return 1
        return 0

    @staticmethod
    def _ensure_point_block(doc):
        if POINT_BLOCK in doc.blocks:
            return
        blk = doc.blocks.new(POINT_BLOCK)
        # entités sur le calque 0, couleur DUBLOC : héritent du calque de l'insertion
        a = {"layer": "0", "color": 0}
        blk.add_circle((0, 0), 1.0, dxfattribs=a)
        blk.add_line((-1, 0), (1, 0), dxfattribs=a)
        blk.add_line((0, -1), (0, 1), dxfattribs=a)

    def legend_entries(self, ctx):
        return list(self._legend.values())

    def cartouche_lines(self, ctx):
        m = ctx.meta
        s = "Données réglementaires figées"
        if m.get("computed_at"):
            s += f" le {str(m['computed_at'])[:10]}"
        s += (" – périmètre : unité foncière" if self.o.scope == "uf"
              else f" – périmètre : UF + {m.get('buffer_m', 0):.0f} m")
        return [s, "Seuls les documents d'urbanisme et servitudes officiels sont opposables "
                   "(liens hypertextes vers les pièces sources quand disponibles)."]


# --------------------------------------------------------------------------- #
# Cadastre
# --------------------------------------------------------------------------- #

class Cadastre2DContributor(Contributor):
    """UF, parcelles de l'UF (numéros), périmètre d'étude. Dessiné en dernier (dessus)."""
    name = "cadastre_2d"

    def __init__(self, uf_parcels: list[dict] | None = None, src_epsg: int = 4326):
        self.parcels = uf_parcels or []
        self.src_epsg = src_epsg

    def write(self, doc, ctx):
        msp, g = doc.modelspace(), ctx.georef
        ensure_layer(doc, LayerSpec("CAD_UF_LIMITE", 1, (220, 38, 38), 50,
                                    description="Limite de l'unité foncière"))
        ensure_layer(doc, LayerSpec("CAD_UF_PARCELLES", 5, (2, 132, 199), 25,
                                    description="Parcelles composant l'UF"))
        ensure_layer(doc, LayerSpec("CAD_UF_NUMEROS", 5, (2, 132, 199), 18,
                                    description="Numéros des parcelles de l'UF"))
        ensure_layer(doc, LayerSpec("EMPRISE_ETUDE", 2, (250, 204, 21), 25, "KER_TIRETS",
                                    description="Périmètre d'étude (UF + tampon)"))
        n = 0
        if ctx.uf is not None:
            for poly in shapely.get_parts(ctx.uf):
                msp.add_lwpolyline(list(poly.exterior.coords)[:-1], close=True,
                                   dxfattribs={"layer": "CAD_UF_LIMITE"})
                n += 1
        if ctx.study_area is not None and ctx.meta.get("buffer_m"):
            for poly in shapely.get_parts(ctx.study_area):
                msp.add_lwpolyline(list(poly.exterior.coords)[:-1], close=True,
                                   dxfattribs={"layer": "EMPRISE_ETUDE"})
        for f in self.parcels:
            if not f.get("geometry"):
                continue
            geom = g.geom_forward(shape(f["geometry"]), self.src_epsg)
            for poly in _parts(geom):
                if poly.geom_type == "Polygon":
                    msp.add_lwpolyline(list(poly.exterior.coords)[:-1], close=True,
                                       dxfattribs={"layer": "CAD_UF_PARCELLES"})
            label = (f.get("properties") or {}).get("label")
            if label:
                p = polylabel(max(_parts(geom), key=lambda q: q.area), tolerance=ctx.mm(1)) \
                    if geom.geom_type in ("Polygon", "MultiPolygon") else geom.centroid
                add_text(msp, label, (p.x, p.y), ctx.mm(2.5), "CAD_UF_NUMEROS")
        return {"uf_parts": n, "parcels": len(self.parcels)}

    def legend_entries(self, ctx):
        return [LegendEntry("Unité foncière", "Unité foncière", "Limite de l'UF",
                            "CAD_UF_LIMITE", "lin", (220, 38, 38)),
                LegendEntry("Unité foncière", "Unité foncière", "Périmètre d'étude",
                            "EMPRISE_ETUDE", "lin", (250, 204, 21))]


class Cadastre3DContributor(Contributor):
    """Drape l'UF et des couches SIG choisies (par défaut : parcelles) sur le MNT."""
    name = "cadastre_3d"

    def __init__(self, layers: list[dict] | None = None, drape_layer_ids=("parcelles",),
                 src_epsg: int = 4326, step_m: float | None = None,
                 uf_parcels: list[dict] | None = None):
        self.layers = [l for l in (layers or []) if l.get("layer_id") in set(drape_layer_ids)]
        self.src_epsg = src_epsg
        self.step = step_m
        self.parcels = uf_parcels or []

    def _drape(self, msp, coords: np.ndarray, layer: str, ctx, closed: bool) -> bool:
        t = ctx.terrain
        step = self.step or t.res
        pts = []
        for a, b in zip(coords[:-1], coords[1:]):
            n = max(1, int(np.ceil(np.hypot(*(b - a)) / step)))
            pts.append(a + (np.arange(n)[:, None] / n) * (b - a))
        pts.append(coords[-1:])
        pts = np.vstack(pts)
        z = t.sample_out(ctx.georef, pts[:, 0], pts[:, 1])
        fin = np.isfinite(z)
        if not fin.any():
            return False
        idx = np.arange(len(z))
        z = np.interp(idx, idx[fin], z[fin])
        if closed:
            pts, z = pts[:-1], z[:-1]
        msp.add_polyline3d(np.c_[pts, z].tolist(), close=closed, dxfattribs={"layer": layer})
        return True

    def write(self, doc, ctx):
        if ctx.terrain is None:
            return {}
        msp, n = doc.modelspace(), 0
        ensure_layer(doc, LayerSpec("CAD_UF_LIMITE_3D", 1, (220, 38, 38), 35,
                                    description="Limite UF drapée sur le terrain"))
        if ctx.uf is not None:
            for poly in shapely.get_parts(ctx.uf):
                n += self._drape(msp, np.asarray(poly.exterior.coords), "CAD_UF_LIMITE_3D", ctx, True)
        if self.parcels:
            ensure_layer(doc, LayerSpec("CAD_UF_PARCELLES_3D", 5, (2, 132, 199), 18,
                                        description="Parcelles de l'UF drapées sur le terrain"))
            for f in self.parcels:
                if not f.get("geometry"):
                    continue
                geom = ctx.georef.geom_forward(shape(f["geometry"]), self.src_epsg)
                for part in _parts(geom):
                    if part.geom_type == "Polygon":
                        n += self._drape(msp, np.asarray(part.exterior.coords),
                                         "CAD_UF_PARCELLES_3D", ctx, True)
        for layer in self.layers:
            name = layer_name("CAD", layer["layer_id"], "3D")
            ensure_layer(doc, LayerSpec(name, 8, lineweight=13,
                                        description=f"{layer.get('title')} drapé sur le terrain"))
            for f in layer.get("features") or []:
                if not f.get("geometry"):
                    continue
                geom = ctx.georef.geom_forward(shape(f["geometry"]), self.src_epsg)
                if ctx.study_area is not None:
                    geom = geom.intersection(ctx.study_area)
                for part in _parts(geom):
                    if part.geom_type == "Polygon":
                        n += self._drape(msp, np.asarray(part.exterior.coords), name, ctx, True)
                    elif part.geom_type == "LineString":
                        n += self._drape(msp, np.asarray(part.coords), name, ctx, False)
        return {"draped_lines": n}
