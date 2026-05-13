"""
Page de garde du rapport PDF d'identité foncière : mise en forme, métadonnées,
superficie UF, zonage urbain, lien vers la carte web.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.sax.saxutils import escape as xml_escape

from pyproj import Transformer
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import HRFlowable, Image, Paragraph, Spacer, Table, TableStyle
from shapely.geometry import shape
from shapely.ops import transform

from .plu_zonage_rapport import ZONAGE_PAGE_LAYER_KEYS, zone_key_from_intersection_element


def _first_xy_pair(coords: Any) -> Optional[Tuple[float, float]]:
    """Premier couple (x,y) dans l’arbre coordinates GeoJSON (même logique que identite_fonciere)."""
    if isinstance(coords, list):
        if (
            len(coords) >= 2
            and isinstance(coords[0], (int, float))
            and isinstance(coords[1], (int, float))
        ):
            return (float(coords[0]), float(coords[1]))
        for item in coords:
            got = _first_xy_pair(item)
            if got:
                return got
    return None


def _detect_input_srid(parcelle_geometry: Dict[str, Any], explicit_srid: Optional[int] = None) -> int:
    """Détection SRID — alignée sur identite_fonciere._detect_input_srid (évite import lourd ici)."""
    if explicit_srid in (4326, 2154, 3857):
        return explicit_srid

    pair = _first_xy_pair(parcelle_geometry.get("coordinates"))
    if not pair:
        return 4326

    x, y = pair
    if -180 <= x <= 180 and -90 <= y <= 90:
        return 4326
    if abs(x) <= 20037508 and abs(y) <= 20037508:
        return 3857
    if 0 <= x <= 1300000 and 5800000 <= y <= 7300000:
        return 2154
    return 4326


def extract_zonage_urbain_summary(intersections: List[Dict[str, Any]]) -> str:
    """
    Libellé(s) de zone(s) PLU pour la ligne « Zonage urbain ».
    (Aligné sur le filtre rapport ≥ 1 % de surface d'étude pour la couche zonage active.)
    """
    for layer in intersections:
        t = (layer.get("table") or "").strip()
        if t not in ZONAGE_PAGE_LAYER_KEYS:
            continue
        if layer.get("_plu_all_zonages_below_min_pct"):
            return "Aucun zonage ≥ 1 % (surface d'étude)"
        elems = layer.get("elements") or []
        names: List[str] = []
        for el in elems:
            if not isinstance(el, dict):
                continue
            z = zone_key_from_intersection_element(el, t)
            if z:
                names.append(z)
        uniq: List[str] = []
        seen: set[str] = set()
        for n in names:
            if n not in seen:
                seen.add(n)
                uniq.append(n)
        if uniq:
            text = ", ".join(uniq)
            return text if len(text) <= 420 else text[:417] + "…"
        return layer.get("display_name") or "Zonage PLU (intersection détectée)"
    return "—"


def compute_uf_surface_m2(
    geometry: Optional[Dict[str, Any]],
    srid: Optional[int] = None,
) -> Optional[float]:
    """
    Superficie de l'UF en m² (Lambert-93), à partir du GeoJSON.
    Retourne None si géométrie absente ou invalide.
    """
    if not geometry or not isinstance(geometry, dict) or "type" not in geometry:
        return None
    try:
        g = shape(geometry)
        if g.is_empty:
            return None
        detected = _detect_input_srid(geometry, srid)
        if detected == 2154:
            return round(float(g.area), 2)
        tf = Transformer.from_crs(f"EPSG:{detected}", "EPSG:2154", always_xy=True)
        g2154 = transform(lambda x, y, z=None: tf.transform(x, y), g)
        return round(float(g2154.area), 2)
    except Exception:
        return None


def _format_surface_fr(m2: float) -> str:
    """Affichage lisible : m² et ha entre parenthèses si pertinent."""
    m2i = int(round(m2))
    sep = "\u202f"  # espace fin insécable
    s = f"{m2i:,}".replace(",", sep)
    ha = m2 / 10000.0
    if ha >= 0.01:
        return f"{s} m² ({ha:.2f} ha)".replace(".", ",")
    return f"{s} m²"


def _map_url_from_result(result: Dict[str, Any]) -> Optional[str]:
    for key in ("carte_web_url", "map_url", "carteUrl", "mapUrl"):
        u = result.get(key)
        if isinstance(u, str) and u.strip().startswith(("http://", "https://")):
            return u.strip()
    return None


def _href_escape(url: str) -> str:
    return (
        url.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
    )


def _laius_text_to_paragraph_html(text: str) -> str:
    """Repli si le rendu Markdown échoue : échappement XML + retours ligne → <br/>."""
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    return xml_escape(raw).replace("\n", "<br/>")


def _laius_markdown_flowables(md: str, inner_w: float, fallback_style: ParagraphStyle) -> List[Any]:
    """`laius_reglement` en Markdown → flowables (module zonage_markdown_pdf)."""
    try:
        from .zonage_markdown_pdf import laius_reglement_to_flowables

        flows = laius_reglement_to_flowables(md, inner_w)
        if flows:
            return flows
    except Exception:
        pass
    return [Paragraph(_laius_text_to_paragraph_html(md), fallback_style)]


def _plu_cover_image_size_pt(
    png_path: Path,
    target_width_pt: float,
) -> Tuple[float, float]:
    """
    Largeur = zone utile (table) ; hauteur dérivée des pixels du PNG pour ne pas déformer.
    Secours : ratio largeur/hauteur aligné sur `plu_visuels` (carte carrée + légende).
    """
    try:
        from PIL import Image as PILImage

        with PILImage.open(png_path) as im:
            pw, ph = im.size
        if pw > 0 and ph > 0:
            w = max(float(target_width_pt), 1.0)
            return w, w * (float(ph) / float(pw))
    except Exception:
        pass
    try:
        from .plu_visuels import PLU_MAP_COVER_ASPECT_WH

        ratio_wh = float(PLU_MAP_COVER_ASPECT_WH)
    except Exception:
        ratio_wh = 1.0 + 0.34
    w = max(float(target_width_pt), 1.0)
    return w, w / ratio_wh


def build_cover_styles() -> Dict[str, ParagraphStyle]:
    """Styles dédiés à la page de garde (complètent ceux du rapport)."""
    base = getSampleStyleSheet()
    return {
        "cover_kicker": ParagraphStyle(
            "CoverKicker",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#5c7268"),
            fontName="Helvetica",
            spaceAfter=4,
            leading=11,
        ),
        "cover_title": ParagraphStyle(
            "CoverTitle",
            parent=base["Normal"],
            fontSize=22,
            textColor=colors.HexColor("#2D6A4F"),
            fontName="Helvetica-Bold",
            spaceAfter=6,
            leading=26,
        ),
        "cover_sub": ParagraphStyle(
            "CoverSub",
            parent=base["Normal"],
            fontSize=10.5,
            textColor=colors.HexColor("#555555"),
            fontName="Helvetica",
            spaceAfter=10,
            leading=14,
        ),
        "cover_label": ParagraphStyle(
            "CoverLabel",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#5a5a5a"),
            fontName="Helvetica-Bold",
            leading=12,
        ),
        "cover_value": ParagraphStyle(
            "CoverValue",
            parent=base["Normal"],
            fontSize=9.5,
            textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica",
            leading=12,
        ),
        "cover_zonage_value": ParagraphStyle(
            "CoverZonageVal",
            parent=base["Normal"],
            fontSize=10,
            textColor=colors.HexColor("#1a4d36"),
            fontName="Helvetica-Bold",
            leading=13,
        ),
        "cover_link": ParagraphStyle(
            "CoverLink",
            parent=base["Normal"],
            fontSize=9.5,
            textColor=colors.HexColor("#1d4ed8"),
            fontName="Helvetica",
            leading=12,
        ),
        "cover_muted": ParagraphStyle(
            "CoverMuted",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#888888"),
            fontName="Helvetica-Oblique",
            leading=12,
        ),
    }


def build_plu_zonage_page_styles() -> Dict[str, ParagraphStyle]:
    """Styles pour la page dédiée « Zonage PLU » (distincte de la page de garde)."""
    base = getSampleStyleSheet()
    return {
        "plu_page_kicker": ParagraphStyle(
            "PluPageKicker",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#6b7f72"),
            fontName="Helvetica-Bold",
            spaceAfter=6,
            leading=10,
        ),
        "plu_page_title": ParagraphStyle(
            "PluPageTitle",
            parent=base["Normal"],
            fontSize=17,
            textColor=colors.HexColor("#1e4d2f"),
            fontName="Helvetica-Bold",
            spaceAfter=8,
            leading=22,
        ),
        "plu_page_intro": ParagraphStyle(
            "PluPageIntro",
            parent=base["Normal"],
            fontSize=9.5,
            textColor=colors.HexColor("#4a5568"),
            fontName="Helvetica",
            spaceAfter=14,
            leading=14,
        ),
        "plu_page_caption": ParagraphStyle(
            "PluPageCaption",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#718096"),
            fontName="Helvetica-Oblique",
            leading=11,
            spaceBefore=8,
        ),
        "plu_laius_section": ParagraphStyle(
            "PluLaiusSection",
            parent=base["Normal"],
            fontSize=11.5,
            textColor=colors.HexColor("#1e4d2f"),
            fontName="Helvetica-Bold",
            spaceAfter=10,
            spaceBefore=4,
            leading=15,
        ),
        "plu_laius_zone_title": ParagraphStyle(
            "PluLaiusZoneTitle",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica-Bold",
            leading=12,
        ),
        "plu_laius_body": ParagraphStyle(
            "PluLaiusBody",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#2d3748"),
            fontName="Helvetica",
            leading=11,
        ),
        "plu_zonage_table_hdr": ParagraphStyle(
            "PluZonageTblHdr",
            parent=base["Normal"],
            fontSize=8.5,
            textColor=colors.HexColor("#1e4d2f"),
            fontName="Helvetica-Bold",
            leading=11,
        ),
        "plu_zonage_table_cell": ParagraphStyle(
            "PluZonageTblCell",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#2d3748"),
            fontName="Helvetica",
            leading=10,
        ),
    }


def _plu_zonage_cell_text(val: str, max_len: int = 3200) -> str:
    s = (val or "").strip()
    if not s:
        return "—"
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s


def build_plu_zonage_page_flowables(
    plu_map_png_path: str,
    *,
    table_width: float,
    c_kerelia_green: Any,
    c_kerelia_light: Any,
    zonage_laius: Optional[dict[str, str]] = None,
    plu_zonage_table_rows: Optional[List[Dict[str, str]]] = None,
    c_border: Optional[Any] = None,
    c_laius_header_bg: Optional[Any] = None,
) -> List[Any]:
    """
    Page autonome : titre, encadré visuel, image carte + légende (plu_visuels), tableau
    libellés / libellés détaillés / descriptions (données issues des intersections filtrées),
    puis blocs « laius » par zone (plu_latresne.laius_reglement). Les clés fournies dans
    `zonage_laius` doivent déjà respecter le seuil surface UF (ex. ≥ 1 %), comme le corps du rapport.
    """
    pp = Path(plu_map_png_path)
    if not pp.is_file():
        return []

    bc = c_border if c_border is not None else colors.HexColor("#B7D9C8")
    bh = c_laius_header_bg if c_laius_header_bg is not None else colors.HexColor("#E8F5EE")

    ps = build_plu_zonage_page_styles()
    tw = max(float(table_width), 120.0)
    content_w = max(tw * 0.98, 1.0)
    img_w, img_h = _plu_cover_image_size_pt(pp, content_w)

    flow: List[Any] = []
    flow.append(Spacer(1, 0.4 * cm))
    flow.append(Paragraph("VUE D’ENSEMBLE — ZONAGE PLU", ps["plu_page_kicker"]))
    flow.append(
        Paragraph(
            "Zonage PLU — carte et répartition sur la surface d’étude",
            ps["plu_page_title"],
        )
    )

    title_band = Table(
        [
            [
                Paragraph(
                    "<font color='white'><b>Zonage PLU</b></font>",
                    ParagraphStyle(
                        "PluBandTxt",
                        parent=getSampleStyleSheet()["Normal"],
                        fontSize=10,
                        fontName="Helvetica-Bold",
                        leading=12,
                    ),
                ),
            ]
        ],
        colWidths=[tw],
        rowHeights=[22],
    )
    title_band.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), c_kerelia_green),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    flow.append(title_band)
    flow.append(Spacer(1, 10))
    flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
    flow.append(Spacer(1, 12))
    flow.append(Image(str(pp), width=img_w, height=img_h))

    if plu_zonage_table_rows:
        flow.append(Spacer(1, 14))
        flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
        flow.append(Spacer(1, 10))
        flow.append(Spacer(1, 8))
        ph = ps["plu_zonage_table_hdr"]
        pc = ps["plu_zonage_table_cell"]
        hdr = [
            Paragraph(xml_escape("Libellé"), ph),
            Paragraph(xml_escape("Libellé détaillé"), ph),
            Paragraph(xml_escape("Description"), ph),
        ]
        tbl_rows: List[List[Any]] = [hdr]
        w1, w2, w3 = tw * 0.20, tw * 0.30, tw * 0.50
        for row in plu_zonage_table_rows:
            z = _plu_zonage_cell_text(row.get("zonage_reglement") or "")
            lb = _plu_zonage_cell_text(row.get("libelle") or "")
            ds = _plu_zonage_cell_text(row.get("libelle_description") or "")
            tbl_rows.append(
                [
                    Paragraph(xml_escape(z), pc),
                    Paragraph(xml_escape(lb), pc),
                    Paragraph(xml_escape(ds), pc),
                ]
            )
        ztbl = Table(tbl_rows, colWidths=[w1, w2, w3])
        ztbl.setStyle(
            TableStyle(
                [
                    ("GRID", (0, 0), (-1, -1), 0.5, bc),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ("LEFTPADDING", (0, 0), (-1, -1), 7),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5EE")),
                ]
            )
        )
        flow.append(ztbl)

    if zonage_laius:
        non_vides = {
            str(k).strip(): str(v).strip()
            for k, v in zonage_laius.items()
            if k is not None and str(k).strip() and v is not None and str(v).strip()
        }
        if non_vides:
            flow.append(Spacer(1, 16))
            flow.append(HRFlowable(width="100%", thickness=1, color=c_kerelia_light))
            flow.append(Spacer(1, 12))
            flow.append(
                Paragraph(
                    "Réglementation par zone (extraits PLU) — mêmes zonages que pour le détail "
                    "réglementaire (seuil de surface d'étude)",
                    ps["plu_laius_section"],
                )
            )
            flow.append(Spacer(1, 6))
            for z_key in sorted(non_vides.keys(), key=lambda x: str(x).lower()):
                inner_w_md = max(float(tw) - 24, 120.0)
                md_flows = _laius_markdown_flowables(
                    non_vides[z_key],
                    inner_w_md,
                    ps["plu_laius_body"],
                )
                inner_rows = [[f] for f in md_flows]
                inner_tbl = Table(inner_rows, colWidths=[inner_w_md])
                inner_tbl.setStyle(
                    TableStyle(
                        [
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("LEFTPADDING", (0, 0), (-1, -1), 0),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                            ("TOPPADDING", (0, 0), (-1, -1), 0),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                        ]
                    )
                )
                zone_title = xml_escape(str(z_key))
                tbl = Table(
                    [
                        [
                            Paragraph(
                                f'<font color="#1e4d2f"><b>Zone {zone_title}</b> — zonage réglementaire</font>',
                                ps["plu_laius_zone_title"],
                            )
                        ],
                        [inner_tbl],
                    ],
                    colWidths=[tw],
                )
                tbl.setStyle(
                    TableStyle(
                        [
                            ("BACKGROUND", (0, 0), (-1, 0), bh),
                            ("BACKGROUND", (0, 1), (-1, 1), colors.white),
                            ("BOX", (0, 0), (-1, -1), 0.7, bc),
                            ("TOPPADDING", (0, 0), (-1, -1), 8),
                            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                            ("LEFTPADDING", (0, 0), (-1, -1), 10),
                            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                            ("VALIGN", (0, 0), (-1, -1), "TOP"),
                            ("ROUNDEDCORNERS", [3, 3, 3, 3]),
                        ]
                    )
                )
                flow.append(tbl)
                flow.append(Spacer(1, 10))

    return flow


def build_cover_page_flowables(
    result: Dict[str, Any],
    *,
    meta_parcelle_label: str,
    meta_parcelle_html: str,
    commune: str,
    insee: str,
    table_width: float,
    c_border: Any,
    c_kerelia_light: Any,
) -> List[Any]:
    """
    Construit les flowables de la première page (titre + tableau métadonnées enrichi).

    `result` peut contenir :
    - geometry (+ srid optionnel dans result) : calcul superficie UF
    - carte_web_url | map_url : lien cliquable « Visualiser la carte web »
    - intersections : extraction du zonage plu_latresne pour la ligne « Zonage urbain (PLU) »
    (L’image carte PLU, la légende % et le tableau libellés / descriptions sont sur une page dédiée,
    voir `build_plu_zonage_page_flowables`.)
    - parcelles_uf_detail : liste optionnelle de dicts (ref, contenance_m2, pct_uf, idu)
      pour un tableau « Détail des parcelles cadastrales » sous le bloc métadonnées.
    """
    cs = build_cover_styles()
    flow: List[Any] = []

    flow.append(Spacer(1, 0.6 * cm))
    flow.append(Paragraph("IDENTITÉ FONCIÈRE", cs["cover_kicker"]))
    flow.append(Paragraph("CARTE D'IDENTITÉ FONCIÈRE", cs["cover_title"]))
    flow.append(
        Paragraph(
            "Synthèse des intersections réglementaires et du zonage pour votre unité foncière.",
            cs["cover_sub"],
        )
    )
    flow.append(HRFlowable(width="100%", thickness=2, color=c_kerelia_light))
    flow.append(Spacer(1, 10))

    geom = result.get("geometry")
    srid = result.get("srid")
    if isinstance(srid, str) and srid.isdigit():
        srid = int(srid)
    elif not isinstance(srid, int):
        srid = None

    surface_m2 = result.get("surface_uf_m2")
    if surface_m2 is not None:
        try:
            surface_m2 = float(surface_m2)
        except (TypeError, ValueError):
            surface_m2 = None
    if surface_m2 is None:
        surface_m2 = compute_uf_surface_m2(geom if isinstance(geom, dict) else None, srid)

    surface_str = _format_surface_fr(surface_m2) if surface_m2 is not None else "—"
    zonage_str = extract_zonage_urbain_summary(result.get("intersections") or [])
    map_url = _map_url_from_result(result)

    if map_url:
        map_cell = Paragraph(
            f'<a href="{_href_escape(map_url)}" color="#1d4ed8"><u>Visualiser la carte web</u></a>',
            cs["cover_link"],
        )
    else:
        map_cell = Paragraph(
            "<i>Non renseigné — indiquez l’URL fournie par l’application (carte 2D).</i>",
            cs["cover_muted"],
        )

    rows_data: List[Tuple[str, Any]] = [
        ("Commune", Paragraph(xml_escape(commune), cs["cover_value"])),
        ("Code INSEE", Paragraph(xml_escape(insee or "—"), cs["cover_value"])),
        (meta_parcelle_label, Paragraph(meta_parcelle_html, cs["cover_value"])),
        ("Zonage urbain (PLU)", Paragraph(xml_escape(zonage_str), cs["cover_zonage_value"])),
        ("Superficie estimée", Paragraph(xml_escape(surface_str), cs["cover_value"])),
        ("Carte interactive", map_cell),
    ]

    tw = max(float(table_width), 120.0)
    label_w = tw * 0.34
    val_w = tw * 0.66

    table_rows = []
    zonage_row_index = 3
    for label, value_cell in rows_data:
        lbl = Paragraph(xml_escape(label), cs["cover_label"])
        table_rows.append([lbl, value_cell])

    t = Table(table_rows, colWidths=[label_w, val_w])
    style_cmds = [
        ("GRID", (0, 0), (-1, -1), 0.5, c_border),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        # ReportLab : (col_début, ligne_début), (col_fin, ligne_fin)
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F7F4")),
        ("BACKGROUND", (0, zonage_row_index), (-1, zonage_row_index), colors.HexColor("#e8f5ee")),
    ]
    t.setStyle(TableStyle(style_cmds))
    flow.append(t)

    raw_pd = result.get("parcelles_uf_detail")
    if isinstance(raw_pd, list) and raw_pd:
        flow.append(Spacer(1, 10))
        flow.append(
            Paragraph(
                xml_escape("Détail des parcelles cadastrales (répartition de l’UF)"),
                cs["cover_label"],
            )
        )
        flow.append(Spacer(1, 4))
        hdr_cells = [
            Paragraph(xml_escape("Référence"), cs["cover_label"]),
            Paragraph(xml_escape("Superficie cadastrale"), cs["cover_label"]),
            Paragraph(xml_escape("% de l’UF"), cs["cover_label"]),
            Paragraph(xml_escape("IDU"), cs["cover_label"]),
        ]
        pr_rows: List[List[Any]] = [hdr_cells]
        for it in raw_pd:
            if not isinstance(it, dict):
                continue
            ref = str(it.get("ref") or "—").strip() or "—"
            cm2 = it.get("contenance_m2")
            pct = it.get("pct_uf")
            idu = str(it.get("idu") or "").strip() or "—"
            try:
                srf = (
                    _format_surface_fr(float(cm2))
                    if cm2 is not None
                    else "—"
                )
            except (TypeError, ValueError):
                srf = "—"
            try:
                pct_s = (
                    f"{float(pct):.2f} %".replace(".", ",")
                    if pct is not None
                    else "—"
                )
            except (TypeError, ValueError):
                pct_s = "—"
            pr_rows.append(
                [
                    Paragraph(xml_escape(ref), cs["cover_value"]),
                    Paragraph(xml_escape(srf), cs["cover_value"]),
                    Paragraph(xml_escape(pct_s), cs["cover_value"]),
                    Paragraph(xml_escape(idu), cs["cover_value"]),
                ]
            )
        if len(pr_rows) > 1:
            twp = tw
            pt = Table(
                pr_rows,
                colWidths=[twp * 0.22, twp * 0.28, twp * 0.14, twp * 0.36],
            )
            pt.setStyle(
                TableStyle(
                    [
                        ("GRID", (0, 0), (-1, -1), 0.5, c_border),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("TOPPADDING", (0, 0), (-1, -1), 5),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                        ("LEFTPADDING", (0, 0), (-1, -1), 8),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F7F4")),
                    ]
                )
            )
            flow.append(pt)

    flow.append(Spacer(1, 12))

    return flow
