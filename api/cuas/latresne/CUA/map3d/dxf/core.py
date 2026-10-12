# -*- coding: utf-8 -*-
"""
dxf_core — noyau commun des exports CAO Kerelia.

Contient : options globales, géoréférencement (CRS de sortie + origine locale),
contexte d'export, interface des contributeurs, registre de calques, couleurs,
types de ligne à l'échelle, textes, cartouche, légende, sauvegarde.

Toutes les tailles « papier » (textes, symboles, tirets, hachures) sont exprimées
en mm imprimés et converties en mètres modèle via l'échelle cible (ctx.mm()).
"""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Literal

import ezdxf
import numpy as np
import shapely
from ezdxf import colors as ezcolors
from ezdxf import zoom
from ezdxf.enums import TextEntityAlignment

APPID = "KERELIA"
TEXT_STYLE = "KERELIA"


# --------------------------------------------------------------------------- #
# Options globales
# --------------------------------------------------------------------------- #

@dataclass
class PackOptions:
    name: str = "kerelia"
    dxf_version: str = "R2013"
    crs: str = "L93"                      # "L93" ou "CC42".."CC50"
    local_origin: bool = False            # soustrait un point de réf. arrondi (Revit / SketchUp)
    local_origin_rounding_m: float = 100.0
    scale: int = 500                      # échelle cible 1/scale : pilote textes, tirets, hachures
    project_label: str = ""


# --------------------------------------------------------------------------- #
# Géoréférencement
# --------------------------------------------------------------------------- #

class Georef:
    """Conversion de n'importe quel EPSG source vers le CRS de sortie (+ origine locale)."""

    def __init__(self, crs: str):
        c = crs.upper().strip()
        if c in ("L93", "2154", "EPSG:2154"):
            self.epsg = 2154
            self.label = "RGF93 / Lambert-93 (EPSG:2154)"
        elif c.startswith("CC"):
            zone = int(c[2:])
            if not 42 <= zone <= 50:
                raise ValueError(f"Zone CC invalide : {crs}")
            self.epsg = 3900 + zone
            self.label = f"RGF93 / CC{zone} (EPSG:{self.epsg})"
        else:
            raise ValueError(f"CRS non supporté : {crs}")
        self.x0 = 0.0
        self.y0 = 0.0

    @staticmethod
    @lru_cache(maxsize=16)
    def _transformer(src: int, dst: int):
        from pyproj import Transformer
        return Transformer.from_crs(src, dst, always_xy=True)

    def forward(self, x, y, src_epsg: int) -> tuple[np.ndarray, np.ndarray]:
        x = np.asarray(x, float)
        y = np.asarray(y, float)
        if src_epsg != self.epsg:
            x, y = self._transformer(src_epsg, self.epsg).transform(x, y)
            x, y = np.asarray(x, float), np.asarray(y, float)
        return x - self.x0, y - self.y0

    def inverse(self, x, y, dst_epsg: int) -> tuple[np.ndarray, np.ndarray]:
        x = np.asarray(x, float) + self.x0
        y = np.asarray(y, float) + self.y0
        if dst_epsg != self.epsg:
            x, y = self._transformer(self.epsg, dst_epsg).transform(x, y)
            x, y = np.asarray(x, float), np.asarray(y, float)
        return x, y

    def geom_forward(self, geom, src_epsg: int):
        """Reprojette une géométrie shapely (2D) vers le CRS de sortie."""
        def f(coords: np.ndarray) -> np.ndarray:
            return np.column_stack(self.forward(coords[:, 0], coords[:, 1], src_epsg))
        return shapely.transform(geom, f)

    def set_origin(self, minx: float, miny: float, rounding: float):
        self.x0 = math.floor(minx / rounding) * rounding
        self.y0 = math.floor(miny / rounding) * rounding

    @property
    def has_origin(self) -> bool:
        return bool(self.x0 or self.y0)


# --------------------------------------------------------------------------- #
# Contexte + interface contributeur
# --------------------------------------------------------------------------- #

@dataclass
class ExportContext:
    options: PackOptions
    georef: Georef
    emprise: tuple[float, float, float, float]      # coordonnées de sortie
    uf: Any = None                                  # shapely, coordonnées de sortie
    study_area: Any = None                          # UF + tampon, coordonnées de sortie
    terrain: Any = None                             # TerrainModel (optionnel)
    meta: dict = field(default_factory=dict)        # commune, computed_at, numero_cu...

    def mm(self, mm: float) -> float:
        """mm imprimés -> mètres modèle à l'échelle cible."""
        return mm * self.options.scale / 1000.0


@dataclass
class LegendEntry:
    family: str
    layer_title: str
    label: str
    dxf_layer: str
    kind: Literal["surf", "lin", "pct"]
    rgb: tuple[int, int, int]


class Contributor:
    """Un contributeur écrit une thématique dans un document DXF."""

    name = "contributor"

    def write(self, doc, ctx: ExportContext) -> dict:
        raise NotImplementedError

    def legend_entries(self, ctx: ExportContext) -> list[LegendEntry]:
        return []

    def cartouche_lines(self, ctx: ExportContext) -> list[str]:
        return []


# --------------------------------------------------------------------------- #
# Couleurs & noms de calques
# --------------------------------------------------------------------------- #

def hex_to_rgb(value: str | None, default=(136, 136, 136)) -> tuple[int, int, int]:
    if not value:
        return default
    m = re.fullmatch(r"#?([0-9a-fA-F]{6})", value.strip())
    if not m:
        return default
    h = m.group(1)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


@lru_cache(maxsize=1)
def _aci_table() -> np.ndarray:
    return np.array([tuple(ezcolors.aci2rgb(i)) for i in range(1, 256)], float)


@lru_cache(maxsize=4096)
def nearest_aci(rgb: tuple[int, int, int]) -> int:
    """Couleur ACI la plus proche (impression via tables CTB)."""
    d = ((_aci_table() - np.array(rgb, float)) ** 2).sum(1)
    return int(np.argmin(d)) + 1


def slug(text: Any, maxlen: int = 40) -> str:
    s = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").upper()
    return (s or "X")[:maxlen]


def layer_name(*parts: Any, maxlen: int = 120) -> str:
    return "_".join(slug(p) for p in parts if p not in (None, ""))[:maxlen]


@dataclass(frozen=True)
class LayerSpec:
    name: str
    aci: int = 7
    rgb: tuple[int, int, int] | None = None
    lineweight: int = -3                 # 1/100 mm, -3 = défaut
    linetype: str = "Continuous"
    description: str = ""
    transparency: float | None = None    # 0 = opaque, 1 = invisible


def ensure_layer(doc, spec: LayerSpec):
    if spec.name in doc.layers:
        return doc.layers.get(spec.name)
    layer = doc.layers.add(spec.name, color=spec.aci, lineweight=spec.lineweight,
                           linetype=spec.linetype)
    if spec.rgb is not None:
        layer.rgb = spec.rgb
    if spec.description:
        layer.description = spec.description[:250]
    if spec.transparency is not None:
        layer.transparency = spec.transparency
    return layer


# --------------------------------------------------------------------------- #
# Document
# --------------------------------------------------------------------------- #

def new_document(ctx: ExportContext):
    doc = ezdxf.new(ctx.options.dxf_version, setup=True)
    doc.units = ezdxf.units.M
    h = doc.header
    h["$INSUNITS"] = 6
    h["$MEASUREMENT"] = 1
    h["$LUNITS"] = 2
    h["$LUPREC"] = 3
    h["$LWDISPLAY"] = 1
    h["$LTSCALE"] = ctx.options.scale / 1000.0   # types de ligne définis en mm papier
    h["$PDMODE"] = 3
    h["$PDSIZE"] = ctx.mm(1.0)
    doc.appids.add(APPID)
    if "PE_URL" not in doc.appids:
        doc.appids.add("PE_URL")
    if TEXT_STYLE not in doc.styles:
        doc.styles.add(TEXT_STYLE, font="arial.ttf")
    # types de ligne en mm papier (× $LTSCALE)
    for name, pattern, desc in (
        ("KER_TIRETS", [4.5, 3.0, -1.5], "Tirets __ __ __"),
        ("KER_MIXTE", [6.0, 3.5, -1.0, 0.0, -1.5], "Mixte __ . __ ."),
        ("KER_POINTILLES", [1.5, 0.3, -1.2], "Pointillés . . ."),
    ):
        if name not in doc.linetypes:
            doc.linetypes.add(name, pattern=pattern, description=desc)
    ensure_layer(doc, LayerSpec("CARTOUCHE", aci=7, lineweight=18,
                                description="Cartouche, nord, avertissements"))
    ensure_layer(doc, LayerSpec("LEGENDE", aci=7, lineweight=13,
                                description="Légende des calques"))
    return doc


def add_text(msp, text: str, pos, height: float, layer: str, *, rotation: float = 0.0,
             align: TextEntityAlignment = TextEntityAlignment.MIDDLE_CENTER, z: float = 0.0,
             dxfattribs: dict | None = None):
    attrs = {"layer": layer, "style": TEXT_STYLE}
    attrs.update(dxfattribs or {})
    t = msp.add_text(str(text), height=height, rotation=rotation, dxfattribs=attrs)
    t.set_placement((float(pos[0]), float(pos[1]), z), align=align)
    return t


def readable_angle(dx: float, dy: float) -> float:
    a = math.degrees(math.atan2(dy, dx))
    if a > 90:
        a -= 180
    elif a < -90:
        a += 180
    return a


def label_along_line(msp, pts: np.ndarray, text: str, height: float, spacing: float,
                     layer: str, z: float = 0.0) -> int:
    """Pose `text` le long d'une polyligne (pts (n,2)), tous les `spacing` m."""
    if len(pts) < 2:
        return 0
    seg = np.hypot(*np.diff(pts, axis=0).T)
    cum = np.concatenate([[0], np.cumsum(seg)])
    total = cum[-1]
    if total < 6 * height:
        return 0
    n = max(1, int(total // spacing))
    for k in range(n):
        s = total * (k + 0.5) / n
        i = int(np.clip(np.searchsorted(cum, s) - 1, 0, len(pts) - 2))
        u = (s - cum[i]) / max(seg[i], 1e-9)
        p = pts[i] + u * (pts[i + 1] - pts[i])
        add_text(msp, text, p, height, layer, rotation=readable_angle(*(pts[i + 1] - pts[i])), z=z)
    return n


def set_xdata(entity, props: dict, max_items: int = 20):
    items = []
    for k, v in props.items():
        if v is None or v == "":
            continue
        s = f"{k}={v}".replace("\n", " ").replace("\r", " ")
        items.append((1000, s[:250]))
        if len(items) >= max_items:
            break
    if items:
        entity.set_xdata(APPID, items)


# --------------------------------------------------------------------------- #
# Cartouche & légende
# --------------------------------------------------------------------------- #

def add_cartouche(msp, lines: Iterable[str], ctx: ExportContext, z: float = 0.0):
    minx, miny, maxx, maxy = ctx.emprise
    th = ctx.mm(2.2)
    msp.add_mtext("\n".join(lines), dxfattribs={
        "layer": "CARTOUCHE", "style": TEXT_STYLE, "char_height": th,
        "insert": (minx, miny - ctx.mm(6), z), "width": maxx - minx, "attachment_point": 1,
    })
    s = ctx.mm(8)
    ox, oy = minx - ctx.mm(12), maxy - s
    msp.add_lwpolyline([(ox - s * 0.4, oy - s), (ox, oy + s), (ox + s * 0.4, oy - s),
                        (ox, oy - s * 0.5)], close=True,
                       dxfattribs={"layer": "CARTOUCHE", "elevation": z})
    add_text(msp, "N", (ox, oy + s * 1.7), s * 0.7, "CARTOUCHE", z=z)


def draw_legend(doc, entries: list[LegendEntry], ctx: ExportContext, title: str = "LÉGENDE"):
    if not entries:
        return
    msp = doc.modelspace()
    minx, miny, maxx, maxy = ctx.emprise
    x0 = maxx + ctx.mm(20)
    y = maxy
    sw, sh = ctx.mm(8), ctx.mm(4)
    row = ctx.mm(5.5)
    grey = {"color": 8}

    add_text(msp, title, (x0, y), ctx.mm(4), "LEGENDE", align=TextEntityAlignment.TOP_LEFT)
    y -= ctx.mm(9)

    by_family: dict[str, dict[str, list[LegendEntry]]] = {}
    for e in entries:
        by_family.setdefault(e.family, {}).setdefault(e.layer_title, []).append(e)

    for family, layers in by_family.items():
        add_text(msp, family.upper(), (x0, y), ctx.mm(3), "LEGENDE",
                 align=TextEntityAlignment.TOP_LEFT)
        y -= ctx.mm(6)
        for layer_title, items in layers.items():
            add_text(msp, layer_title, (x0 + ctx.mm(2), y), ctx.mm(2.4), "LEGENDE",
                     align=TextEntityAlignment.TOP_LEFT)
            y -= ctx.mm(5)
            for e in items:
                sx, sy = x0 + ctx.mm(4), y - sh
                color = {"color": nearest_aci(e.rgb), "true_color": ezcolors.rgb2int(e.rgb)}
                if e.kind == "surf":
                    rect = [(sx, sy), (sx + sw, sy), (sx + sw, sy + sh), (sx, sy + sh)]
                    hatch = msp.add_hatch(dxfattribs={"layer": "LEGENDE", **color})
                    hatch.paths.add_polyline_path(rect, is_closed=True)
                    hatch.transparency = 0.5
                    msp.add_lwpolyline(rect, close=True, dxfattribs={"layer": "LEGENDE", **color})
                elif e.kind == "lin":
                    msp.add_lwpolyline([(sx, sy + sh / 2), (sx + sw, sy + sh / 2)],
                                       dxfattribs={"layer": "LEGENDE", "lineweight": 50, **color})
                else:
                    msp.add_circle((sx + sw / 2, sy + sh / 2), ctx.mm(1.2),
                                   dxfattribs={"layer": "LEGENDE", **color})
                add_text(msp, e.label[:70], (sx + sw + ctx.mm(2), y - sh / 2), ctx.mm(2.2),
                         "LEGENDE", align=TextEntityAlignment.MIDDLE_LEFT)
                add_text(msp, f"[{e.dxf_layer}]", (sx + sw + ctx.mm(2), y - sh - ctx.mm(0.6)),
                         ctx.mm(1.5), "LEGENDE", align=TextEntityAlignment.TOP_LEFT,
                         dxfattribs=grey)
                y -= row + ctx.mm(1.5)
            y -= ctx.mm(1.5)
        y -= ctx.mm(3)


def save_document(doc, path: Path):
    doc.audit()
    zoom.extents(doc.modelspace())
    doc.saveas(str(path))
