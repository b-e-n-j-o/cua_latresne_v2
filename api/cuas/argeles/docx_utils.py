# -*- coding: utf-8 -*-
"""docx_utils.py — Rendu Markdown → python-docx (gras, italique, listes, titres)."""

from __future__ import annotations

import re

from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt

FONT = "Arial"
_HYPERLINK_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink"

_INLINE_MD_RE = re.compile(r"(\*\*\*.*?\*\*\*|\*\*.*?\*\*|\*.*?\*)")


def parse_markdown_inline(paragraph, text, size=9, italic_base=False):
    """Parse le gras (**) et l'italique (*) en runs Word."""
    parts = _INLINE_MD_RE.split(text)

    for part in parts:
        if not part:
            continue
        run = paragraph.add_run()
        run.font.name = FONT
        run.font.size = Pt(size)
        run.italic = italic_base

        if part.startswith("***") and part.endswith("***"):
            run.text = part[3:-3]
            run.bold = True
            run.italic = True
        elif part.startswith("**") and part.endswith("**"):
            run.text = part[2:-2]
            run.bold = True
        elif part.startswith("*") and part.endswith("*"):
            run.text = part[1:-1]
            run.italic = True
        else:
            run.text = part


def add_markdown_block(doc, text, size=9, is_bullet=False):
    """Ajoute un bloc multi-lignes Markdown dans le document.

    is_bullet=True force une puce Word sur chaque ligne — à éviter si le texte
    contient déjà des puces (•, –, etc.) ou des préfixes markdown (- / *).
    """
    if not text:
        return

    _NATIVE_BULLET_PREFIXES = ("•", "–", "—", "·")

    for line in text.split("\n"):
        cleaned_line = line.strip()
        if not cleaned_line:
            continue

        if cleaned_line.startswith("#"):
            level = len(cleaned_line) - len(cleaned_line.lstrip("#"))
            title_text = cleaned_line.lstrip("#").strip()
            doc.add_heading(title_text, level=min(level, 5))
            continue

        has_native_bullet = cleaned_line.startswith(_NATIVE_BULLET_PREFIXES)

        is_line_bullet = is_bullet and not has_native_bullet
        if cleaned_line.startswith("- ") or cleaned_line.startswith("* "):
            is_line_bullet = True
            cleaned_line = cleaned_line[2:].strip()

        if is_line_bullet:
            p = doc.add_paragraph(style="List Bullet")
        else:
            p = doc.add_paragraph()

        p.paragraph_format.space_after = Pt(3)
        parse_markdown_inline(p, cleaned_line, size=size)


def add_hyperlink(paragraph, text: str, url: str, *, size: int = 10, italic: bool = False):
    """Insère un hyperlien cliquable dans un paragraphe existant."""
    part = paragraph.part
    r_id = part.relate_to(url, _HYPERLINK_REL, is_external=True)

    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("r:id"), r_id)

    run = OxmlElement("w:r")
    r_pr = OxmlElement("w:rPr")

    color = OxmlElement("w:color")
    color.set(qn("w:val"), "0563C1")
    r_pr.append(color)

    underline = OxmlElement("w:u")
    underline.set(qn("w:val"), "single")
    r_pr.append(underline)

    if italic:
        italic_el = OxmlElement("w:i")
        r_pr.append(italic_el)

    font = OxmlElement("w:rFonts")
    font.set(qn("w:ascii"), FONT)
    font.set(qn("w:hAnsi"), FONT)
    r_pr.append(font)

    sz = OxmlElement("w:sz")
    sz.set(qn("w:val"), str(int(size * 2)))
    r_pr.append(sz)

    run.append(r_pr)
    text_el = OxmlElement("w:t")
    text_el.text = text
    run.append(text_el)
    hyperlink.append(run)
    paragraph._p.append(hyperlink)


def add_para_with_link(
    doc,
    prefix: str,
    link_text: str,
    url: str,
    *,
    size: int = 10,
    space_after: int = 4,
):
    """Paragraphe avec libellé + hyperlien cliquable."""
    p = doc.add_paragraph()
    if space_after is not None:
        p.paragraph_format.space_after = Pt(space_after)
    if prefix:
        run = p.add_run(prefix)
        run.font.name = FONT
        run.font.size = Pt(size)
    add_hyperlink(p, link_text, url, size=size)
    return p
