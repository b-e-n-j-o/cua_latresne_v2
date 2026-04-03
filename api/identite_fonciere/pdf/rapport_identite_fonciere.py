"""
rapport_identite_fonciere.py
Génération du rapport PDF d'identité foncière à partir des intersections.

Usage standalone :
    python rapport_identite_fonciere.py <intersections.json>

Intégration programmatique :
    from rapport_identite_fonciere import generate_rapport_pdf
    path = generate_rapport_pdf(result_dict, output_dir=".")
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from xml.sax.saxutils import escape as xml_escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    HRFlowable,
    Image,
    KeepTogether,
    Flowable,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.flowables import HRFlowable
from reportlab.pdfbase.pdfmetrics import stringWidth

from .header import build_cover_page_flowables, build_plu_zonage_page_flowables

try:
    from .zonage_markdown_pdf import (
        build_zonage_regulation_flowables,
        is_plu_zonage_layer,
        regulation_text_looks_like_markdown,
    )
except ImportError:
    build_zonage_regulation_flowables = None  # type: ignore[assignment,misc]

    def is_plu_zonage_layer(layer):  # type: ignore[misc]
        return False

    def regulation_text_looks_like_markdown(t):  # type: ignore[misc]
        return False


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Palette couleurs Kerelia
# ---------------------------------------------------------------------------
C_KERELIA_GREEN = colors.HexColor("#2D6A4F")   # vert forêt
C_KERELIA_LIGHT = colors.HexColor("#52B788")   # vert clair
C_BG_ARTICLE = colors.HexColor("#F0F7F4")      # fond section article
C_BORDER = colors.HexColor("#B7D9C8")

# Couleurs par type
TYPE_COLORS: Dict[str, Any] = {
    "servitude": colors.HexColor("#1B4F72"),
    "prescription": colors.HexColor("#784212"),
    "information": colors.HexColor("#145A32"),
    "information_ou_prescription": colors.HexColor("#4A235A"),
    "installations_classees": colors.HexColor("#922B21"),
    "reseaux": colors.HexColor("#154360"),
    "Informations": colors.HexColor("#145A32"),
}

TYPE_LABELS: Dict[str, str] = {
    "servitude": "Servitude",
    "prescription": "Prescription",
    "information": "Information",
    "information_ou_prescription": "Information / Prescription",
    "installations_classees": "Installation classée",
    "reseaux": "Réseau",
    "Informations": "Information",
}

# Noms des articles
ARTICLE_LABELS: Dict[str, str] = {
    "3": "Zonage PLU",
    "4": "Servitudes d'utilité publique",
    "5": "Risques et nuisances",
    "6": "Réseaux et équipements",
    "7": "Informations diverses",
    "8": "Autres",
    "9": "Droits de préemption",
}

# Chemin logo par défaut (relatif à ce fichier puis fallback)
_DEFAULT_LOGO_CANDIDATES = [
    Path(__file__).parents[2] / "CUA" / "logos" / "logo_kerelia.png",
    Path(__file__).parent / "logos" / "logo_kerelia.png",
    Path("/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CUA/logos/logo_kerelia.png"),
]


def _find_logo() -> Optional[Path]:
    for p in _DEFAULT_LOGO_CANDIDATES:
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

def _build_styles():
    base = getSampleStyleSheet()

    styles = {
        "title": ParagraphStyle(
            "KTitle",
            parent=base["Normal"],
            fontSize=20,
            textColor=C_KERELIA_GREEN,
            fontName="Helvetica-Bold",
            spaceAfter=4,
            leading=24,
        ),
        "subtitle": ParagraphStyle(
            "KSubtitle",
            parent=base["Normal"],
            fontSize=11,
            textColor=colors.HexColor("#555555"),
            fontName="Helvetica",
            spaceAfter=2,
            leading=14,
        ),
        "article_header": ParagraphStyle(
            "KArticleHeader",
            parent=base["Normal"],
            fontSize=13,
            textColor=colors.white,
            fontName="Helvetica-Bold",
            spaceAfter=0,
            spaceBefore=0,
            leading=16,
        ),
        "layer_name": ParagraphStyle(
            "KLayerName",
            parent=base["Normal"],
            fontSize=10,
            textColor=colors.HexColor("#1a1a1a"),
            fontName="Helvetica-Bold",
            spaceAfter=2,
            leading=13,
        ),
        "type_badge": ParagraphStyle(
            "KTypeBadge",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.white,
            fontName="Helvetica-Bold",
            leading=10,
        ),
        "attr_key": ParagraphStyle(
            "KAttrKey",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#666666"),
            fontName="Helvetica-Bold",
            leading=11,
        ),
        "attr_val": ParagraphStyle(
            "KAttrVal",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#222222"),
            fontName="Helvetica",
            leading=12,
        ),
        "reglementation_block": ParagraphStyle(
            "KRegBlock",
            parent=base["Normal"],
            fontSize=8.5,
            textColor=colors.HexColor("#1f2937"),
            fontName="Helvetica",
            leading=11,
        ),
        "footer": ParagraphStyle(
            "KFooter",
            parent=base["Normal"],
            fontSize=8,
            textColor=colors.HexColor("#999999"),
            fontName="Helvetica",
            alignment=TA_CENTER,
        ),
        "meta_label": ParagraphStyle(
            "KMetaLabel",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#888888"),
            fontName="Helvetica-Bold",
            leading=12,
        ),
        "meta_value": ParagraphStyle(
            "KMetaValue",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#222222"),
            fontName="Helvetica",
            leading=12,
        ),
        "no_intersection": ParagraphStyle(
            "KNoIntersect",
            parent=base["Normal"],
            fontSize=9,
            textColor=colors.HexColor("#888888"),
            fontName="Helvetica-Oblique",
            leading=12,
        ),
        "summary_title": ParagraphStyle(
            "KSummaryTitle",
            parent=base["Normal"],
            fontSize=11,
            textColor=C_KERELIA_GREEN,
            fontName="Helvetica-Bold",
            spaceAfter=4,
            leading=14,
        ),
    }
    return styles


class _LinkToBookmark(Flowable):
    """Petit lien interne vers une destination PDF (bookmark)."""

    def __init__(
        self,
        text: str,
        dest: str,
        fontName: str = "Helvetica",
        fontSize: float = 8,
        color: Any = colors.HexColor("#1d4ed8"),
        padding_x: float = 4,
        padding_y: float = 1,
    ):
        super().__init__()
        self.text = text
        self.dest = dest
        self.fontName = fontName
        self.fontSize = fontSize
        self.color = color
        self.padding_x = padding_x
        self.padding_y = padding_y

    def wrap(self, availWidth: float, availHeight: float):
        w_text = stringWidth(self.text, self.fontName, self.fontSize)
        w = min(availWidth, w_text + 2 * self.padding_x)
        h = self.fontSize * 1.25 + 2 * self.padding_y
        self.width = w
        self.height = h
        return w, h

    def draw(self):
        w, h = self.width, self.height
        self.canv.saveState()
        self.canv.setFont(self.fontName, self.fontSize)
        self.canv.setFillColor(self.color)
        # Baseline un peu au-dessus du bas pour éviter d'écraser le texte.
        y = self.padding_y
        self.canv.drawString(self.padding_x, y, self.text)

        # Rectangle de lien en coordonnées absolues (sinon certains viewers
        # créent une annotation à largeur nulle).
        abs_x, abs_y = self.canv.absolutePosition(0, 0)
        self.canv.linkRect(
            "",
            self.dest,
            (abs_x, abs_y, abs_x + w, abs_y + h),
            relative=0,
            thickness=0,
            color=self.color,
        )
        self.canv.restoreState()


class _AnchorBookmarkPage(Flowable):
    """Crée une destination PDF interne via bookmarkPage (plus fiable que horizontal seul)."""

    _ZEROSIZE = 0

    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def wrap(self, availWidth: float, availHeight: float):
        return 0, 0

    def draw(self):
        # On ancre au point courant pour que le lecteur aille "au bon endroit"
        # (pas seulement au début de la page).
        x, y = self.canv.absolutePosition(0, 0)
        self.canv.bookmarkPage(self.name, fit="FitH", top=y)


# ---------------------------------------------------------------------------
# Helpers de rendu
# ---------------------------------------------------------------------------

def _type_color(layer_type: str) -> Any:
    return TYPE_COLORS.get(layer_type, colors.HexColor("#555555"))


def _type_label(layer_type: str) -> str:
    return TYPE_LABELS.get(layer_type, layer_type.capitalize() if layer_type else "—")


def _article_key(article_raw: Optional[str]) -> str:
    """Normalise l'article en clé simple (premier token)."""
    if not article_raw:
        return "8"
    first = str(article_raw).split(",")[0].strip()
    return first if first in ARTICLE_LABELS else "8"


def _layer_row_result_label(row: Dict[str, Any]) -> str:
    """Même logique que le tableau dynamique du frontend (ParcelleIdentity)."""
    st = (row.get("status") or "").strip()
    ec = row.get("elements_count")
    if ec is None:
        ec = row.get("elementsCount")
    try:
        ec_int = int(ec) if ec is not None else 0
    except (TypeError, ValueError):
        ec_int = 0
    if st == "intersected":
        return f"{ec_int} éléments"
    if st == "not_intersected":
        return "-"
    if st == "skipped":
        sr = (row.get("skip_reason") or row.get("skipReason") or "").strip()
        return f"Ignoré ({sr})" if sr else "Ignoré"
    if st == "error":
        err = str(row.get("error") or "Erreur")
        return err[:220] + ("…" if len(err) > 220 else "")
    if st == "pending":
        return "…"
    return "—"


def _normalize_couche_display_name(row: Dict[str, Any]) -> str:
    return str(
        row.get("display_name")
        or row.get("displayName")
        or row.get("table")
        or "—"
    )

def _format_attr_value(val: Any) -> str:
    if isinstance(val, list):
        return ", ".join(str(v) for v in val if v is not None)
    return str(val) if val is not None else "—"


def _build_attr_label_map(layer: Dict[str, Any]) -> Dict[str, str]:
    """
    Construit la table de correspondance attribut brut -> libellé propre.

    Sources acceptées (dans la config couche du catalogue):
    - clean_attributes: { "attr_brut": "Nom propre", ... }
    - clean_attributes: ["Nom propre 1", "Nom propre 2", ...] (aligné sur keep)
    """
    mapping: Dict[str, str] = {}
    clean_cfg = layer.get("clean_attributes")
    keep_cfg = layer.get("keep") or []

    if isinstance(clean_cfg, dict):
        for raw, clean in clean_cfg.items():
            if isinstance(raw, str) and raw.strip() and isinstance(clean, str) and clean.strip():
                mapping[raw] = clean
        return mapping

    if isinstance(clean_cfg, list) and isinstance(keep_cfg, list):
        keep_fields = [k for k in keep_cfg if isinstance(k, str)]
        clean_fields = [c for c in clean_cfg if isinstance(c, str)]
        for raw, clean in zip(keep_fields, clean_fields):
            if raw.strip() and clean.strip():
                mapping[raw] = clean
    return mapping


def _pdf_column_keys(layer: Dict[str, Any]) -> Optional[List[str]]:
    """
    Si `clean_attributes` est renseigné dans le catalogue, colonnes du tableau PDF
    uniquement dans cet ordre (dict: clés ; liste: alignement sur keep).
    Sinon None : comportement historique (toutes les clés présentes sur les lignes).
    """
    clean_cfg = layer.get("clean_attributes")
    keep_cfg = layer.get("keep") or []

    if isinstance(clean_cfg, dict) and clean_cfg:
        keys = [
            k
            for k in clean_cfg.keys()
            if isinstance(k, str) and k.strip() and k.lower() != "reglementation"
        ]
        # Si seules des clés « réglementation » existent, ne pas retourner [] (tableau 0 colonne → crash ReportLab)
        return keys if keys else None

    if isinstance(clean_cfg, list) and clean_cfg and isinstance(keep_cfg, list):
        keep_fields = [k for k in keep_cfg if isinstance(k, str)]
        clean_fields = [c for c in clean_cfg if isinstance(c, str)]
        keys: List[str] = []
        for raw, clean in zip(keep_fields, clean_fields):
            if raw.strip() and clean.strip():
                if raw.lower() != "reglementation":
                    keys.append(raw)
        return keys if keys else None

    return None


def _pdf_keep_effectif_vide(layer: Dict[str, Any]) -> bool:
    """True si keep est absent ou [] (ou uniquement des chaînes vides) : rapport sans tableau d'attributs."""
    keep = layer.get("keep")
    if keep is None:
        return False
    if not isinstance(keep, list):
        return False
    return not any(isinstance(k, str) and k.strip() for k in keep)


def _group_by_key_from_layer(layer: Dict[str, Any], elements: List[Dict[str, Any]]) -> Optional[str]:
    """
    Détermine la clé de groupement effective à utiliser dans le PDF.
    Accepte `group_by` en str ou list.
    """
    group_by = layer.get("group_by")
    candidates: List[str] = []
    if isinstance(group_by, str) and group_by.strip():
        candidates = [group_by]
    elif isinstance(group_by, list):
        candidates = [str(x) for x in group_by if isinstance(x, str) and x.strip()]

    if not candidates:
        return None

    for cand in candidates:
        if any(cand in el and el.get(cand) not in (None, "", "null", "None") for el in elements):
            return cand
    return candidates[0]


def _normalize_layer_elements(layer: Dict[str, Any]) -> List[Dict[str, Any]]:
    """`elements` peut être absent, null (JSON) ou mal typé — toujours une liste de dicts."""
    raw = layer.get("elements")
    if raw is None:
        return []
    if not isinstance(raw, list):
        return []
    return [e for e in raw if isinstance(e, dict)]


def _aggregate_elements_for_pdf(layer: Dict[str, Any], elements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Agrège les éléments pour éviter les doublons:
    - si `group_by` existe: 1 ligne par valeur de groupement
    - sinon: comportement existant (sans réglementation)
    Ajoute, quand disponible, un champ de pourcentage d'intersection agrégé.
    """
    # Nettoyage base: on n'affiche jamais la réglementation brute dans le tableau
    cleaned = []
    for el in elements:
        row = {k: v for k, v in el.items() if k.lower() != "reglementation"}
        if row:
            cleaned.append(row)

    if not cleaned:
        return []

    grp_key = _group_by_key_from_layer(layer, cleaned)
    if not grp_key:
        return cleaned

    # Champs potentiels de pourcentage d'intersection
    pct_key_candidates = []
    head0 = cleaned[0]
    if not isinstance(head0, dict):
        return cleaned
    for k in head0.keys():
        lk = k.lower()
        if re.search(r"(pourcent|percentage|pct|taux)", lk):
            pct_key_candidates.append(k)
    for row in cleaned[1:]:
        for k in row.keys():
            lk = k.lower()
            if re.search(r"(pourcent|percentage|pct|taux)", lk) and k not in pct_key_candidates:
                pct_key_candidates.append(k)

    grouped: Dict[str, Dict[str, Any]] = {}
    pct_values_by_group: Dict[str, List[str]] = {}

    for row in cleaned:
        grp_val = _format_attr_value(row.get(grp_key))
        if not grp_val or grp_val in ("—", "None", "null"):
            grp_val = "Non renseigné"
        if grp_val not in grouped:
            grouped[grp_val] = {grp_key: grp_val}
            pct_values_by_group[grp_val] = []

        for pk in pct_key_candidates:
            if pk in row and row.get(pk) not in (None, "", "None", "null"):
                pct_values_by_group[grp_val].append(_format_attr_value(row.get(pk)))

    aggregated = list(grouped.values())
    if pct_key_candidates:
        pct_label = "pourcentage_intersection"
        for g in aggregated:
            gv = g.get(grp_key, "Non renseigné")
            vals = sorted(set(v for v in pct_values_by_group.get(str(gv), []) if v))
            if vals:
                g[pct_label] = " / ".join(vals)

    return aggregated


def _build_layer_block(
    layer: Dict[str, Any],
    styles: Dict,
    page_width_pts: float,
    *,
    reg_annex_blocks: List[Any],
    reg_counter: Dict[str, int],
) -> List:
    """Construit le bloc ReportLab pour une couche intersectée."""
    flowables = []

    display_name = layer.get("display_name") or layer.get("table") or "Couche inconnue"
    layer_type = layer.get("type") or ""
    article_raw = layer.get("article")
    elements: List[Dict] = _normalize_layer_elements(layer)
    attr_disc = layer.get("attribut_discriminant")

    type_color = _type_color(layer_type)
    type_lbl = _type_label(layer_type)

    # --- En-tête couche (nom + badge type) ---
    inner_w = page_width_pts - 2 * 2 * cm  # marges doc

    badge_text = Paragraph(
        f'<font color="white"><b>{type_lbl}</b></font>',
        styles["type_badge"],
    )
    name_para = Paragraph(f"<b>{display_name}</b>", styles["layer_name"])

    badge_table = Table(
        [[badge_text]],
        colWidths=[len(type_lbl) * 5.5 + 12],
        rowHeights=[14],
    )
    badge_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), type_color),
        ("ROUNDEDCORNERS", [3, 3, 3, 3]),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))

    header_row = Table(
        [[name_para, badge_table]],
        colWidths=[inner_w * 0.72, inner_w * 0.28],
        rowHeights=[18],
    )
    header_row.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (0, 0), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
    ]))

    # --- Filtrer/Agréger les éléments ---
    # (group_by du catalogue si présent: une ligne par valeur)
    filtered_elements = _aggregate_elements_for_pdf(layer, elements)

    # --- Tableau des attributs ---
    if _pdf_keep_effectif_vide(layer):
        if layer.get("_plu_all_zonages_below_min_pct"):
            attr_block = Paragraph(
                "Les zonages PLU visibles sur la carte (contexte 50 m, page de garde) "
                "représentent chacun moins de 1 % de la surface d'étude : le détail "
                "réglementaire n'est pas reproduit ici.",
                styles["no_intersection"],
            )
        else:
            attr_block = Paragraph(
                "Intersection détectée (données attributaires non disponibles)",
                styles["no_intersection"],
            )
    elif not filtered_elements:
        if layer.get("_plu_all_zonages_below_min_pct"):
            attr_block = Paragraph(
                "Les zonages PLU visibles sur la carte (contexte 50 m, page de garde) "
                "représentent chacun moins de 1 % de la surface d'étude : le détail "
                "réglementaire n'est pas reproduit ici.",
                styles["no_intersection"],
            )
        else:
            attr_block = Paragraph(
                "Intersection détectée (données attributaires non disponibles)",
                styles["no_intersection"],
            )
    else:
        label_map = _build_attr_label_map(layer)

        # Construire les lignes du tableau attributs
        attr_rows = []
        catalog_keys = _pdf_column_keys(layer)
        if catalog_keys:
            all_keys = catalog_keys
        else:
            all_keys = []
            for el in filtered_elements:
                for k in el.keys():
                    if k not in all_keys:
                        all_keys.append(k)

        if not all_keys:
            attr_block = Paragraph(
                "Intersection détectée (données attributaires non disponibles pour le tableau)",
                styles["no_intersection"],
            )
        else:
            # Ligne header colonnes
            header_cells = [
                Paragraph(f"<b>{label_map.get(k, k)}</b>", styles["attr_key"]) for k in all_keys
            ]
            attr_rows.append(header_cells)

            for el in filtered_elements:
                row_cells = []
                for k in all_keys:
                    val = el.get(k)
                    row_cells.append(Paragraph(_format_attr_value(val), styles["attr_val"]))
                attr_rows.append(row_cells)

            col_w = inner_w / max(len(all_keys), 1)
            col_widths = [col_w] * len(all_keys)
            # repeatRows=1 avec 1 seule ligne + colWidths[] peut provoquer list index out of range (ReportLab)
            repeat_hdr = 1 if len(attr_rows) > 1 else 0

            attr_block = Table(attr_rows, colWidths=col_widths, repeatRows=repeat_hdr)
            attr_block.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5EE")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCDDCC")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FCF9")]),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
            ]))

    # --- Réglementation : texte mis en annexe + lien depuis la couche ---
    reg_dest: Optional[str] = None
    reg_key = None
    for el in elements:
        if not isinstance(el, dict):
            continue
        for k in el.keys():
            if isinstance(k, str) and k.lower() == "reglementation":
                reg_key = k
                break
        if reg_key:
            break

    if reg_key:
        grp_key = _group_by_key_from_layer(layer, elements)
        # Ordre d'affichage : suit l'ordre agrégé (si group_by existe)
        group_order: List[str] = []
        if grp_key:
            for row in filtered_elements:
                gv = row.get(grp_key)
                if gv is None:
                    continue
                group_order.append(_format_attr_value(gv))
        if not group_order and grp_key:
            group_order = ["Non renseigné"]

        MAX_REG_CHARS = 20000
        reg_by_group: Dict[str, List[str]] = {}
        seen_texts: Dict[str, set] = {}

        for el in elements:
            if not isinstance(el, dict) or reg_key not in el:
                continue
            raw_val = el.get(reg_key)
            if raw_val is None:
                continue
            if isinstance(raw_val, list):
                text_val = ", ".join(str(v) for v in raw_val if v is not None).strip()
            else:
                text_val = str(raw_val).strip()
            if not text_val:
                continue
            if len(text_val) > MAX_REG_CHARS:
                text_val = text_val[:MAX_REG_CHARS] + "…"

            if grp_key:
                gv_raw = el.get(grp_key)
                gv = _format_attr_value(gv_raw)
                if not gv or gv in ("—", "None", "null"):
                    gv = "Non renseigné"
            else:
                gv = "__ALL__"

            reg_by_group.setdefault(gv, [])
            seen_texts.setdefault(gv, set())
            if text_val not in seen_texts[gv]:
                seen_texts[gv].add(text_val)
                reg_by_group[gv].append(text_val)

        if reg_by_group:
            ordered_groups_full = group_order + [g for g in reg_by_group.keys() if g not in group_order]
            use_zonage_md = (
                build_zonage_regulation_flowables is not None
                and is_plu_zonage_layer(layer)
                and any(
                    regulation_text_looks_like_markdown(str(t))
                    for texts in reg_by_group.values()
                    for t in texts
                    if t
                )
            )
            md_flows: List[Any] = []
            if use_zonage_md:
                try:
                    md_flows = build_zonage_regulation_flowables(  # type: ignore[misc]
                        reg_by_group,
                        ordered_groups_full,
                        grp_key,
                        inner_w,
                    )
                except Exception as exc:
                    logger.warning(
                        "Rendu Markdown zonage PLU indisponible, repli HTML : %s",
                        exc,
                        exc_info=True,
                    )
                    md_flows = []

            if md_flows:
                reg_counter["i"] += 1
                reg_dest = f"reg_{reg_counter['i']}"
                reg_heading = Paragraph(
                    f"<b>Réglementation — {xml_escape(display_name)}</b>",
                    styles["layer_name"],
                )
                reg_annex_blocks.extend(
                    [
                        _AnchorBookmarkPage(reg_dest),
                        reg_heading,
                        Spacer(1, 2),
                        *md_flows,
                        Spacer(1, 10),
                    ]
                )
            else:
                parts_html: List[str] = []
                if grp_key:
                    for gv in ordered_groups_full:
                        texts = reg_by_group.get(gv) or []
                        if not texts:
                            continue
                        label_html = xml_escape(str(gv))
                        text_html = "<br/><br/>".join(
                            xml_escape(t).replace("\n", "<br/>") for t in texts
                        )
                        parts_html.append(f"<b>{label_html}</b><br/>{text_html}")
                else:
                    texts = reg_by_group.get("__ALL__") or []
                    if texts:
                        parts_html.append(
                            "<br/><br/>".join(
                                xml_escape(t).replace("\n", "<br/>") for t in texts
                            )
                        )

                if parts_html:
                    reg_counter["i"] += 1
                    reg_dest = f"reg_{reg_counter['i']}"

                    reg_heading = Paragraph(
                        f"<b>Réglementation — {xml_escape(display_name)}</b>",
                        styles["layer_name"],
                    )
                    reg_body = Paragraph(
                        "<br/><br/>".join(parts_html),
                        styles["reglementation_block"],
                    )
                    reg_annex_blocks.extend(
                        [
                            _AnchorBookmarkPage(reg_dest),
                            reg_heading,
                            Spacer(1, 2),
                            reg_body,
                            Spacer(1, 10),
                        ]
                    )

    # Encadrement de la couche entière
    content_table = Table(
        [[header_row], [Spacer(1, 4)], [attr_block]],
        colWidths=[inner_w],
    )
    content_table.setStyle(TableStyle([
        ("BOX", (0, 0), (-1, -1), 0.8, C_BORDER),
        ("ROUNDEDCORNERS", [4, 4, 4, 4]),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
    ]))

    flowables.append(content_table)
    if reg_dest:
        flowables.append(Spacer(1, 2))
        flowables.append(_LinkToBookmark("Voir réglementation", reg_dest))
    flowables.append(Spacer(1, 6))
    return flowables


def _build_article_section(
    article_key: str,
    layers: List[Dict],
    styles: Dict,
    page_width_pts: float,
    *,
    reg_annex_blocks: List[Any],
    reg_counter: Dict[str, int],
) -> List:
    """Construit la section complète d'un article."""
    flowables = []

    label = ARTICLE_LABELS.get(article_key, f"Article {article_key}")

    # Bandeau article
    header_para = Paragraph(label, styles["article_header"])
    header_table = Table(
        [[header_para]],
        colWidths=[page_width_pts - 4 * cm],
        rowHeights=[22],
    )
    header_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), C_KERELIA_GREEN),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
    ]))

    # Compteur de couches
    n = len(layers)
    badge_count = Paragraph(
        f'<font color="#2D6A4F"><b>{n} couche{"s" if n > 1 else ""} intersectée{"s" if n > 1 else ""}</b></font>',
        ParagraphStyle("cnt", fontSize=8, fontName="Helvetica-Bold", leading=10),
    )

    flowables.append(Spacer(1, 10))
    flowables.append(KeepTogether([header_table, Spacer(1, 4), badge_count, Spacer(1, 6)]))

    for layer in layers:
        flowables.extend(
            _build_layer_block(
                layer,
                styles,
                page_width_pts,
                reg_annex_blocks=reg_annex_blocks,
                reg_counter=reg_counter,
            )
        )

    return flowables


# ---------------------------------------------------------------------------
# Page callbacks (header/footer)
# ---------------------------------------------------------------------------

class _PageDecorator:
    def __init__(self, logo_path: Optional[Path], commune: str, date_str: str):
        self.logo_path = logo_path
        self.commune = commune
        self.date_str = date_str

    def __call__(self, canvas, doc):
        canvas.saveState()
        w, h = A4

        # Bande verte top
        canvas.setFillColor(C_KERELIA_GREEN)
        canvas.rect(0, h - 12 * mm, w, 12 * mm, fill=1, stroke=0)

        # Logo en haut à gauche
        if self.logo_path and self.logo_path.exists():
            try:
                canvas.drawImage(
                    str(self.logo_path),
                    8 * mm, h - 10 * mm,
                    height=8 * mm,
                    preserveAspectRatio=True,
                    mask="auto",
                )
            except Exception:
                pass

        # Commune + date en haut à droite (texte blanc)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica", 7)
        canvas.drawRightString(w - 8 * mm, h - 5 * mm, f"{self.commune}  |  {self.date_str}")

        # Footer
        canvas.setFillColor(colors.HexColor("#AAAAAA"))
        canvas.setFont("Helvetica", 7)
        canvas.drawCentredString(w / 2, 8 * mm, f"Carte d'identité foncière – {self.commune} – Page {doc.page}")
        canvas.setStrokeColor(colors.HexColor("#DDDDDD"))
        canvas.line(15 * mm, 13 * mm, w - 15 * mm, 13 * mm)

        canvas.restoreState()


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def _meta_parcelle_label_and_html(result: Dict[str, Any]) -> Tuple[str, str]:
    """
    Libellé + fragment HTML pour la ligne « parcelle » sur la page de garde.
    Si `parcelles_cadastrales` est fourni (UF), une ligne par parcelle (section + numéro).
    """
    raw_list = result.get("parcelles_cadastrales")
    parcelle = result.get("parcelle") or ""
    if isinstance(raw_list, list) and raw_list:
        lines: List[str] = []
        for p in raw_list:
            if not isinstance(p, dict):
                continue
            s = str(p.get("section", "")).strip()
            n = str(p.get("numero", "")).strip()
            if s or n:
                lines.append(f"{s} {n}".strip())
        if lines:
            label = (
                "Références cadastrales"
                if len(lines) > 1
                else "Référence cadastrale"
            )
            html = "<br/>".join(xml_escape(x) for x in lines)
            return label, html
    pv = parcelle.strip() if isinstance(parcelle, str) else ""
    # Liste UF passée uniquement dans `parcelle` (ex. "AC 12, AC 34") — plusieurs lignes
    if pv and pv != "UNITE_FONCIERE" and "," in pv:
        parts = [p.strip() for p in pv.split(",") if p.strip()]
        if len(parts) > 1:
            label = "Références cadastrales"
            html = "<br/>".join(xml_escape(x) for x in parts)
            return label, html
    if not pv or pv == "UNITE_FONCIERE":
        return "Référence parcelle / UF", "—"
    return "Référence parcelle / UF", xml_escape(pv)


def generate_rapport_pdf(
    result: Dict[str, Any],
    output_dir: str = ".",
    logo_path: Optional[str] = None,
    filename: Optional[str] = None,
    catalogue: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Génère le PDF du rapport d'identité foncière.

    Args:
        result: Dict retourné par analyser_identite_fonciere / analyser_identite_parcelle
                Doit contenir 'intersections', 'commune', 'insee', et optionnellement
                'parcelle' et/ou 'parcelles_cadastrales' (liste {section, numero}) pour l’UF.
                Optionnel pour la page de garde : 'geometry' (GeoJSON UF), 'srid',
                'carte_web_url' ou 'map_url' (lien https vers la carte 2D),
                'surface_uf_m2' (nombre, sinon calcul depuis geometry).
                Avec 'geometry', génération optionnelle du PNG PLU combiné (carte + légende)
                sur une page dédiée après la page de garde, et filtrage des zonages sous 1 %
                pour la couche plu_latresne (la carto conserve le buffer 50 m).
        output_dir: Dossier de sortie.
        logo_path: Chemin explicite vers logo_kerelia.png (optionnel).
        filename: Nom du fichier de sortie (optionnel, auto-généré sinon).
        catalogue: Dict du catalogue JSON (pour récupérer 'type', 'article', etc.)
                   Si None, on suppose que les intersections portent déjà ces champs.

    Returns:
        str: Chemin absolu du PDF généré.
    """
    intersections: List[Dict] = result.get("intersections", [])
    for _ly in intersections:
        if not isinstance(_ly, dict):
            continue
        el = _ly.get("elements")
        if el is None or not isinstance(el, list):
            _ly["elements"] = []
        else:
            _ly["elements"] = [e for e in el if isinstance(e, dict)]

    geom = result.get("geometry")
    srid = result.get("srid")
    if isinstance(srid, str) and srid.isdigit():
        srid = int(srid)
    elif not isinstance(srid, int):
        srid = None

    plu_map_png_path: Optional[str] = None
    pct_stats: Dict[str, float] = {}
    if isinstance(geom, dict) and geom.get("type"):
        try:
            from .plu_visuels import (
                PLU_LATRESNE_TABLE,
                filter_plu_latresne_layer_for_report,
                generate_plu_visuals_from_uf_geometry,
            )

            out_base = Path(output_dir).resolve()
            out_base.mkdir(parents=True, exist_ok=True)
            pcs = result.get("parcelles_cadastrales")
            if not isinstance(pcs, list):
                pcs = None
            map_path, map_png_compat, pct_stats, parcelles_detail = (
                generate_plu_visuals_from_uf_geometry(
                    geom,
                    str(out_base),
                    srid=srid,
                    insee=str(result.get("insee") or "").strip(),
                    parcelles_cadastrales=pcs,
                )
            )
            plu_map_png_path = map_path
            result["plu_map_png"] = map_path
            # Clé historique : même fichier que plu_map_png (ancien second visuel supprimé).
            result["plu_pie_png"] = map_png_compat
            result["plu_pct_stats"] = pct_stats
            if parcelles_detail:
                result["parcelles_uf_detail"] = parcelles_detail

            if pct_stats:
                new_ix: List[Dict] = []
                for ly in intersections:
                    if not isinstance(ly, dict):
                        new_ix.append(ly)
                        continue
                    if (ly.get("table") or "").strip() == PLU_LATRESNE_TABLE:
                        new_ix.append(filter_plu_latresne_layer_for_report(ly, pct_stats))
                    else:
                        new_ix.append(ly)
                intersections = new_ix
                result["intersections"] = intersections
        except Exception as exc:
            logger.warning("Visuels PLU ou filtre zonage indisponible : %s", exc, exc_info=True)

    commune: str = result.get("commune", "Commune inconnue")
    insee: str = result.get("insee", "")
    nb_intersections: int = result.get("nb_intersections", len(intersections))

    date_str = datetime.now().strftime("%d/%m/%Y %H:%M")
    date_file = datetime.now().strftime("%Y%m%d_%H%M")

    # Logo
    logo: Optional[Path] = None
    if logo_path:
        logo = Path(logo_path)
        if not logo.exists():
            logger.warning(f"Logo introuvable: {logo_path}")
            logo = _find_logo()
    else:
        logo = _find_logo()

    # Nom de fichier
    if not filename:
        safe_commune = commune.replace(" ", "_").lower()
        filename = f"rapport_identite_fonciere_{safe_commune}_{date_file}.pdf"

    output_path = Path(output_dir) / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)

    styles = _build_styles()
    page_w, page_h = A4

    # Annexe : toutes les réglementations (liées depuis chaque couche)
    reg_annex_blocks: List[Any] = []
    reg_counter: Dict[str, int] = {"i": 0}

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2.2 * cm,
        bottomMargin=1.8 * cm,
        title=f"Rapport identité foncière – {commune}",
        author="Kerelia",
        subject="Analyse foncière et urbanistique",
    )

    decorator = _PageDecorator(logo, commune, date_str)
    story = []

    # -----------------------------------------------------------------------
    # Page de garde (module header : zonage, surface UF, lien carte web)
    # -----------------------------------------------------------------------
    if catalogue:
        for layer in intersections:
            if not isinstance(layer, dict):
                continue
            t = layer.get("table") or ""
            cat_entry = catalogue.get(t, {})
            if not layer.get("type") and cat_entry.get("type"):
                layer["type"] = cat_entry["type"]
            if not layer.get("article") and cat_entry.get("article"):
                layer["article"] = cat_entry["article"]
            if not layer.get("group_by") and cat_entry.get("group_by"):
                layer["group_by"] = cat_entry["group_by"]
            if not layer.get("clean_attributes") and cat_entry.get("clean_attributes"):
                layer["clean_attributes"] = cat_entry["clean_attributes"]
            if "keep" not in layer and "keep" in cat_entry:
                layer["keep"] = cat_entry["keep"]

    pl_lbl, pl_html = _meta_parcelle_label_and_html(result)
    cover_w = page_w - 4 * cm
    story.extend(
        build_cover_page_flowables(
            result,
            meta_parcelle_label=pl_lbl,
            meta_parcelle_html=pl_html,
            commune=commune,
            insee=insee,
            table_width=cover_w,
            c_border=C_BORDER,
            c_kerelia_light=C_KERELIA_LIGHT,
        )
    )
    if plu_map_png_path:
        story.append(PageBreak())
        plu_laius_map: Dict[str, str] = {}
        if pct_stats:
            try:
                from .plu_visuels import (
                    MIN_PCT_ZONAGE_URBAIN,
                    fetch_laius_reglement_par_zonages,
                )

                # Même seuil que le filtre plu_latresne / réglementation : ≥ 1 % (surface UF)
                zonages_pour_laius = [
                    z
                    for z, p in pct_stats.items()
                    if isinstance(p, (int, float)) and float(p) >= MIN_PCT_ZONAGE_URBAIN
                ]
                plu_laius_map = fetch_laius_reglement_par_zonages(zonages_pour_laius)
                if plu_laius_map:
                    result["plu_zonage_laius"] = plu_laius_map
            except Exception as exc:
                logger.warning("Textes laius PLU indisponibles : %s", exc)
        story.extend(
            build_plu_zonage_page_flowables(
                plu_map_png_path,
                table_width=cover_w,
                c_kerelia_green=C_KERELIA_GREEN,
                c_kerelia_light=C_KERELIA_LIGHT,
                zonage_laius=plu_laius_map or None,
                c_border=C_BORDER,
                c_laius_header_bg=C_BG_ARTICLE,
            )
        )
    else:
        story.append(Spacer(1, 14))

    story.append(PageBreak())

    # -----------------------------------------------------------------------
    # Corps : regroupement par article
    # -----------------------------------------------------------------------
    # Grouper par article
    articles: Dict[str, List[Dict]] = {}
    for layer in intersections:
        art = _article_key(layer.get("article"))
        articles.setdefault(art, []).append(layer)

    # Trier les articles numériquement, puis les couches par display_name
    for art_key in articles:
        articles[art_key].sort(key=lambda x: (x.get("display_name") or "").lower())

    for art_key in sorted(articles.keys(), key=lambda k: int(k) if k.isdigit() else 99):
        layers_in_art = articles[art_key]
        story.extend(
            _build_article_section(
                art_key,
                layers_in_art,
                styles,
                page_w,
                reg_annex_blocks=reg_annex_blocks,
                reg_counter=reg_counter,
            )
        )

    # Annexe : réglementations
    if reg_annex_blocks:
        story.append(PageBreak())
        story.append(Paragraph("Annexe — Réglementations", styles["summary_title"]))
        story.append(Spacer(1, 6))
        story.extend(reg_annex_blocks)

    # -----------------------------------------------------------------------
    # Build
    # -----------------------------------------------------------------------
    doc.build(
        story,
        onFirstPage=decorator,
        onLaterPages=decorator,
    )

    logger.info(f"✅ Rapport PDF généré : {output_path}")
    return str(output_path.resolve())


# ---------------------------------------------------------------------------
# CLI standalone
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 2:
        print("Usage: python rapport_identite_fonciere.py <intersections.json> [output_dir]")
        sys.exit(1)

    json_path = Path(sys.argv[1])
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "."

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    pdf_path = generate_rapport_pdf(data, output_dir=out_dir)
    print(f"PDF généré : {pdf_path}")