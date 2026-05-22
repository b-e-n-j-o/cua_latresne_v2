"""
Rendu PDF de la réglementation PLU (zonage) à partir de Markdown structuré.

Le parseur est défensif : toute ligne commençant par 1–4 « # » est traitée comme titre,
ce qui évite une boucle infinie si le corps du texte commence par « # » sans matcher
l’ancien motif « # » + espace obligatoire.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, Spacer, Table, TableStyle

from xml.sax.saxutils import escape as xml_escape

PLU_ZONAGE_TABLES = frozenset({"zonage_plu"})

C_KERELIA_GREEN = colors.HexColor("#2D6A4F")
C_ZONE_BAND = colors.HexColor("#E8F5EE")
C_ZONE_BORDER = colors.HexColor("#B7D9C8")

# Titre Markdown : 1 à 4 # en début de ligne ; espaces optionnels entre # et le libellé
_HEADING_LINE = re.compile(r"^(#{1,4})\s*(.*)$")


def is_plu_zonage_layer(layer: Dict[str, Any]) -> bool:
    return (layer.get("table") or "").strip() in PLU_ZONAGE_TABLES


def regulation_text_looks_like_markdown(text: str) -> bool:
    if not text or not isinstance(text, str):
        return False
    # Aligné sur _HEADING_LINE : #…# puis espaces optionnels puis le libellé (ou fin de ligne).
    return bool(re.search(r"(?m)^#{1,4}(?:\s|$|\S)", text.strip()))


def _parse_line_heading(stripped: str) -> Optional[Tuple[int, str]]:
    m = _HEADING_LINE.match(stripped.strip())
    if not m:
        return None
    level = len(m.group(1))
    title = (m.group(2) or "").strip()
    return (min(level, 4), title)


def _inline_format(text: str) -> str:
    parts = text.split("**")
    out: List[str] = []
    for i, seg in enumerate(parts):
        esc = xml_escape(seg).replace("\n", "<br/>")
        if i % 2 == 1:
            out.append(f"<b>{esc}</b>")
        else:
            out.append(esc)
    return "".join(out)


def _build_md_styles() -> Dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "h1": ParagraphStyle(
            "ZonageMdH1",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=13,
            textColor=C_KERELIA_GREEN,
            spaceAfter=6,
            spaceBefore=10,
            leading=16,
            alignment=TA_LEFT,
        ),
        "h2": ParagraphStyle(
            "ZonageMdH2",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=11,
            textColor=colors.HexColor("#1a4d36"),
            spaceAfter=4,
            spaceBefore=12,
            leading=14,
            alignment=TA_LEFT,
        ),
        "h3": ParagraphStyle(
            "ZonageMdH3",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=9.5,
            textColor=colors.HexColor("#333333"),
            spaceAfter=3,
            spaceBefore=8,
            leading=12,
            alignment=TA_LEFT,
        ),
        "h4": ParagraphStyle(
            "ZonageMdH4",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=8.75,
            textColor=colors.HexColor("#374151"),
            spaceAfter=2,
            spaceBefore=6,
            leading=11,
            alignment=TA_LEFT,
        ),
        "body": ParagraphStyle(
            "ZonageMdBody",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            textColor=colors.HexColor("#1f2937"),
            leading=11.5,
            spaceAfter=5,
            alignment=TA_LEFT,
        ),
        "li_num": ParagraphStyle(
            "ZonageMdLi",
            parent=base["Normal"],
            fontName="Helvetica",
            fontSize=8.5,
            textColor=colors.HexColor("#1f2937"),
            leading=11.5,
            leftIndent=10,
            spaceAfter=4,
            alignment=TA_LEFT,
        ),
        "zone_label": ParagraphStyle(
            "ZonageZoneLbl",
            parent=base["Normal"],
            fontName="Helvetica-Bold",
            fontSize=10,
            textColor=C_KERELIA_GREEN,
            leading=12,
            alignment=TA_LEFT,
        ),
    }


def _parse_markdown_to_flowables(md: str, inner_w: float, styles: Dict[str, ParagraphStyle]) -> List[Any]:
    flow: List[Any] = []
    lines = md.replace("\r\n", "\n").split("\n")
    i = 0
    n = len(lines)
    # Garde-fou : le nombre d’itérations ne peut pas dépasser ~4n (filet)
    max_outer = max(n * 4 + 50, 100)

    def flush_para(buf: List[str]) -> None:
        if not buf:
            return
        raw = "\n".join(buf).strip()
        if raw:
            flow.append(Paragraph(_inline_format(raw), styles["body"]))

    outer_steps = 0
    while i < n and outer_steps < max_outer:
        outer_steps += 1
        s = lines[i].strip()
        if not s:
            i += 1
            continue

        hl = _parse_line_heading(s)
        if hl is not None:
            lev, title = hl
            if lev <= 1:
                sk = "h1"
            elif lev == 2:
                sk = "h2"
            elif lev == 3:
                sk = "h3"
            else:
                sk = "h4"
            flow.append(Paragraph(_inline_format(title if title else "—"), styles[sk]))
            i += 1
            continue

        m = re.match(r"^(\d+)\.\s+(.*)$", s)
        if m:
            num, rest = m.group(1), m.group(2)
            item_lines = [rest]
            i += 1
            inner_steps = 0
            while i < n and inner_steps < max_outer:
                inner_steps += 1
                nxt = lines[i]
                ns = nxt.strip()
                if not ns:
                    break
                if _parse_line_heading(ns) is not None:
                    break
                if re.match(r"^\d+\.\s+", ns):
                    break
                item_lines.append(ns.strip())
                i += 1
            body = " ".join(item_lines)
            col2 = max(float(inner_w) - 14 * mm, 40 * mm)
            w1, w2 = max(12 * mm, 1), max(col2, 1)
            tbl = Table(
                [
                    [
                        Paragraph(f"<b>{xml_escape(num)}.</b>", styles["li_num"]),
                        Paragraph(_inline_format(body), styles["li_num"]),
                    ]
                ],
                colWidths=[w1, w2],
            )
            tbl.setStyle(
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
            flow.append(tbl)
            continue

        m = re.match(r"^[-*]\s+(.*)$", s)
        if m:
            rest = m.group(1)
            item_lines = [rest]
            i += 1
            inner_steps = 0
            while i < n and inner_steps < max_outer:
                inner_steps += 1
                nxt = lines[i]
                ns = nxt.strip()
                if not ns:
                    break
                if _parse_line_heading(ns) is not None:
                    break
                if re.match(r"^\d+\.\s+", ns):
                    break
                if re.match(r"^[-*]\s+", ns):
                    break
                item_lines.append(ns.strip())
                i += 1
            body = " ".join(item_lines)
            col2 = max(float(inner_w) - 14 * mm, 40 * mm)
            w1, w2 = max(8 * mm, 1), max(col2, 1)
            tbl = Table(
                [
                    [
                        Paragraph("•", styles["li_num"]),
                        Paragraph(_inline_format(body), styles["li_num"]),
                    ]
                ],
                colWidths=[w1, w2],
            )
            tbl.setStyle(
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
            flow.append(tbl)
            continue

        buf: List[str] = []
        inner_steps = 0
        while i < n and inner_steps < max_outer:
            inner_steps += 1
            ln = lines[i]
            st = ln.strip()
            if not st:
                break
            if _parse_line_heading(st) is not None:
                break
            if re.match(r"^\d+\.\s+", st):
                break
            if re.match(r"^[-*]\s+", st):
                break
            buf.append(ln)
            i += 1
        flush_para(buf)
        # Si rien n’a été consommé (ne devrait pas arriver), on avance d’une ligne
        if not buf and i < n:
            i += 1

    return flow


def _zone_banner_table(zone_title: str, inner_w: float, styles: Dict[str, ParagraphStyle]) -> Table:
    w = max(float(inner_w), 1.0)
    banner = Table(
        [[Paragraph(xml_escape(zone_title.strip() or "Zone"), styles["zone_label"])]],
        colWidths=[w],
    )
    banner.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), C_ZONE_BAND),
                ("BOX", (0, 0), (-1, -1), 0.6, C_ZONE_BORDER),
                ("ROUNDEDCORNERS", [3, 3, 3, 3]),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    return banner


def _append_zone_reglementation(
    out: List[Any],
    zone_title: Optional[str],
    combined: str,
    inner_w: float,
    styles: Dict[str, ParagraphStyle],
    *,
    show_zone_banner: bool,
) -> None:
    if not combined.strip():
        return
    if not regulation_text_looks_like_markdown(combined):
        if show_zone_banner and zone_title:
            out.append(Spacer(1, 6))
            out.append(_zone_banner_table(zone_title, inner_w, styles))
            out.append(Spacer(1, 8))
        out.append(Paragraph(_inline_format(combined.strip()), styles["body"]))
        return
    if show_zone_banner and zone_title:
        out.append(Spacer(1, 6))
        out.append(_zone_banner_table(zone_title, inner_w, styles))
        out.append(Spacer(1, 8))
    out.extend(_parse_markdown_to_flowables(combined, inner_w, styles))


def build_zonage_regulation_flowables(
    reg_by_group: Dict[str, List[str]],
    ordered_groups: List[str],
    grp_key: Optional[str],
    inner_w: float,
) -> List[Any]:
    styles = _build_md_styles()
    out: List[Any] = []

    if grp_key:
        keys_order: List[str] = []
        seen = set()
        for gv in ordered_groups:
            if gv in seen or gv not in reg_by_group:
                continue
            keys_order.append(gv)
            seen.add(gv)
        for gv in reg_by_group.keys():
            if gv not in seen:
                keys_order.append(gv)
                seen.add(gv)

        for gv in keys_order:
            texts = reg_by_group.get(gv) or []
            combined = "\n\n".join(t for t in texts if t and str(t).strip())
            _append_zone_reglementation(
                out,
                str(gv).strip() or "Zone",
                combined,
                inner_w,
                styles,
                show_zone_banner=True,
            )
    else:
        texts = reg_by_group.get("__ALL__") or []
        combined = "\n\n".join(t for t in texts if t and str(t).strip())
        _append_zone_reglementation(
            out,
            None,
            combined,
            inner_w,
            styles,
            show_zone_banner=False,
        )

    return out


def laius_reglement_to_flowables(md: str, inner_w: float) -> List[Any]:
    """
    Texte `laius_reglement` (Markdown) → flowables ReportLab : titres #/##/###/####, **gras**,
    listes numérotées et à puces (- ou *).
    """
    if not isinstance(md, str) or not str(md).strip():
        return []
    styles = _build_md_styles()
    return _parse_markdown_to_flowables(str(md).strip(), inner_w, styles)
