# -*- coding: utf-8 -*-
"""
Assembleur du pack CAO :
  <name>_plan_2D.dxf    : SIG réglementaire + terrain 2D + cote de seuil + cadastre UF + légende + cartouche (Z=0)
  <name>_terrain_3D.dxf : maillage MNT + UF / parcelles drapées + plan/volume cote de seuil (+ bâti 3D à venir)
  LISEZMOI.txt          : CRS, origine, sources, liste des calques
"""

from __future__ import annotations

import zipfile
from datetime import date
from pathlib import Path

import numpy as np
import shapely
from shapely import affinity
from shapely.geometry import Polygon, mapping, shape
from shapely.ops import transform as shapely_transform

from .core import (Contributor, ExportContext, Georef, PackOptions, add_cartouche,
                   draw_legend, new_document, save_document)
from .cote_de_seuil import (CoteSeuil2DContributor, CoteSeuil3DContributor,
                            CoteSeuilAnalysis, CoteSeuilOptions)
from .sig import (Cadastre2DContributor, Cadastre3DContributor, SigLayersContributor,
                  SigOptions)
from .terrain import (TERRAIN_EPSG, Terrain2DContributor, Terrain3DContributor,
                      TerrainModel, TerrainOptions)


def _uf_geometry(carto_context, terrain, georef, src_epsg):
    """UF en coordonnées de sortie : contexte carto en priorité, sinon payload terrain."""
    if carto_context and carto_context.get("parcelle"):
        gj = carto_context["parcelle"]
        gj = gj.get("geometry", gj) if gj.get("type") == "Feature" else gj
        return georef.geom_forward(shape(gj), src_epsg)
    if terrain is not None:
        polys = []
        for ring in terrain.uf_rings_rel():
            x, y = georef.forward(terrain.cx + ring[:, 0], terrain.cy + ring[:, 1], TERRAIN_EPSG)
            polys.append(Polygon(np.c_[x, y]))
        if polys:
            return shapely.union_all(polys)
    return None


def _run(doc, contributors: list[Contributor], ctx) -> dict:
    return {c.name: c.write(doc, ctx) for c in contributors}


def _parcels_rel_to_4326(payload: dict) -> list[dict]:
    """Parcelles du payload MNT (relatif Lambert-93) → features GeoJSON 4326."""
    from pyproj import Transformer

    to_4326 = Transformer.from_crs(2154, 4326, always_xy=True)
    cx = float(payload.get("center_x") or 0.0)
    cy = float(payload.get("center_y") or 0.0)
    features = []
    for parcel in payload.get("parcels") or []:
        geom = parcel.get("geometry")
        if not geom:
            continue
        g = affinity.translate(shape(geom), xoff=cx, yoff=cy)
        g = shapely_transform(to_4326.transform, g)
        c = g.representative_point()
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(g),
                "properties": {
                    "label": parcel.get("label"),
                    "section": parcel.get("section"),
                    "numero": parcel.get("numero"),
                    "lon": float(c.x),
                    "lat": float(c.y),
                },
            }
        )
    return features


def _merge_carto_context(terrain_payload: dict | None, carto_context: dict | None) -> dict | None:
    ctx = dict(carto_context or {})
    if not ctx.get("parcels") and terrain_payload:
        parcels = _parcels_rel_to_4326(terrain_payload)
        if parcels:
            ctx["parcels"] = parcels
    return ctx or None


def build_cad_package(
    output_dir: str | Path,
    *,
    terrain_payload: dict | None = None,
    carto_context: dict | None = None,
    carto_catalogue: dict | None = None,
    layers: list[dict] | None = None,
    options: PackOptions | None = None,
    terrain_options: TerrainOptions | None = None,
    sig_options: SigOptions | None = None,
    meta: dict | None = None,
    extra_2d: list[Contributor] | None = None,
    extra_3d: list[Contributor] | None = None,
    cote_seuil_features: list[dict] | None = None,
    cote_seuil_options: CoteSeuilOptions | None = None,
) -> dict:
    opts = options or PackOptions()
    topts = terrain_options or TerrainOptions()
    sopts = sig_options or SigOptions()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if meta and not opts.project_label:
        parts = [meta.get("commune"), meta.get("numero_cu")]
        opts.project_label = " — ".join(str(p) for p in parts if p)

    # -- données ------------------------------------------------------------ #
    carto_context = _merge_carto_context(terrain_payload, carto_context)
    terrain = TerrainModel(terrain_payload, topts.pixel_is_center) if terrain_payload else None
    if layers is None and carto_context is not None:
        # même préparation que la carte MapLibre -> ce qu'on voit = ce qu'on exporte
        from api.cuas.argeles.carto_context.carto_context_enrich import prepare_layers_payload
        layers = prepare_layers_payload(carto_context, carto_catalogue)
    layers = layers or []
    buffer_m = float((carto_context or {}).get("context_buffer_m") or 0.0)

    # -- géoréférencement + emprise ---------------------------------------- #
    georef = Georef(opts.crs)
    uf = _uf_geometry(carto_context, terrain, georef, sopts.src_epsg)
    study = uf.buffer(buffer_m) if (uf is not None and buffer_m > 0) else uf
    bounds = []
    if terrain is not None:
        bounds.append(terrain.bbox_out(georef))
    if study is not None:
        bounds.append(study.bounds)
    if not bounds:
        raise ValueError("Rien à exporter : ni terrain, ni unité foncière.")
    b = np.array(bounds)
    emprise = (b[:, 0].min(), b[:, 1].min(), b[:, 2].max(), b[:, 3].max())
    if opts.local_origin:
        georef.set_origin(emprise[0], emprise[1], opts.local_origin_rounding_m)
        dx, dy = -georef.x0, -georef.y0
        uf = affinity.translate(uf, dx, dy) if uf is not None else None
        study = affinity.translate(study, dx, dy) if study is not None else None
        emprise = (emprise[0] + dx, emprise[1] + dy, emprise[2] + dx, emprise[3] + dy)

    cc = carto_context or {}
    ctx = ExportContext(
        options=opts, georef=georef, emprise=emprise, uf=uf, study_area=study, terrain=terrain,
        meta={"computed_at": cc.get("computed_at"), "buffer_m": buffer_m, **(meta or {})},
    )

    # -- contributeurs (l'ordre = ordre de dessin) -------------------------- #
    features = cote_seuil_features if cote_seuil_features is not None else cc.get("cote_seuil")
    extra2 = list(extra_2d or [])
    extra3 = list(extra_3d or [])
    if features:
        analysis = CoteSeuilAnalysis(features, cote_seuil_options)
        extra2.append(CoteSeuil2DContributor(analysis))
        extra3.append(CoteSeuil3DContributor(analysis))

    sig = SigLayersContributor(layers, carto_catalogue, sopts)
    # extra_2d (cote de seuil) au-dessus du terrain, sous le cadastre UF
    c2d: list[Contributor] = [sig, Terrain2DContributor(topts), *extra2,
                              Cadastre2DContributor(cc.get("parcels"), sopts.src_epsg)]
    c3d: list[Contributor] = [Terrain3DContributor(topts),
                              Cadastre3DContributor(
                                  layers, src_epsg=sopts.src_epsg,
                                  uf_parcels=cc.get("parcels"),
                              ),
                              *extra3]

    doc2d, doc3d = new_document(ctx), new_document(ctx)
    stats = {"2d": _run(doc2d, c2d, ctx), "3d": _run(doc3d, c3d, ctx) if terrain else {}}

    # -- légende + cartouche ------------------------------------------------ #
    legend = [e for c in c2d for e in c.legend_entries(ctx)]
    draw_legend(doc2d, legend, ctx)

    head = [opts.project_label] if opts.project_label else []
    if ctx.meta.get("commune"):
        head.append(f"Commune : {ctx.meta['commune']}")
    head.append(f"Système : {georef.label}")
    if georef.has_origin:
        head.append(f"ORIGINE LOCALE : X réel = X + {georef.x0:.0f} / Y réel = Y + {georef.y0:.0f}")
    head.append(f"Échelle de mise en forme : 1/{opts.scale}")
    tail = [f"Export Kerelia du {date.today():%d/%m/%Y} – données IGN / GPU sous Licence Ouverte Etalab 2.0"]
    cart2d = head + [l for c in c2d for l in c.cartouche_lines(ctx)] + tail
    cart3d = head + [l for c in (c2d[1], *c3d) for l in c.cartouche_lines(ctx)] + tail
    add_cartouche(doc2d.modelspace(), cart2d, ctx)

    # -- écriture ----------------------------------------------------------- #
    files = []
    p2d = out_dir / f"{opts.name}_plan_2D.dxf"
    save_document(doc2d, p2d)
    files.append(p2d)
    if terrain is not None:
        zmin = float(np.nanmin(terrain.z))
        add_cartouche(doc3d.modelspace(), cart3d, ctx, z=zmin)
        p3d = out_dir / f"{opts.name}_terrain_3D.dxf"
        save_document(doc3d, p3d)
        files.append(p3d)

    readme = out_dir / "LISEZMOI.txt"
    layer_lines = [f"  {l.dxf.name:<45} {l.description}" for l in doc2d.layers
                   if l.dxf.name not in ("0", "Defpoints")]
    readme.write_text(
        "\n".join(cart2d) + "\n\nFichiers :\n"
        + "\n".join(f"  {p.name}" for p in files)
        + "\n\nAstuce : geler tous les calques *_HACH pour masquer les hachures, "
          "*_ETIQ pour les étiquettes.\n\nCalques du plan 2D :\n" + "\n".join(layer_lines) + "\n",
        encoding="utf-8",
    )
    files.append(readme)

    pzip = out_dir / f"{opts.name}_pack_cao.zip"
    with zipfile.ZipFile(pzip, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in files:
            zf.write(p, arcname=p.name)

    stats["sizes_bytes"] = {p.name: p.stat().st_size for p in (*files, pzip)}
    return {"zip": str(pzip), "files": [str(p) for p in files], "stats": stats}
