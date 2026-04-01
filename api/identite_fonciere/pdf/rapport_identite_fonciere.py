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
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)
from reportlab.platypus.flowables import HRFlowable

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
    "9": "Article 9 – Droits de préemption",
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
        return [k for k in clean_cfg.keys() if isinstance(k, str) and k.strip()]

    if isinstance(clean_cfg, list) and clean_cfg and isinstance(keep_cfg, list):
        keep_fields = [k for k in keep_cfg if isinstance(k, str)]
        clean_fields = [c for c in clean_cfg if isinstance(c, str)]
        keys: List[str] = []
        for raw, clean in zip(keep_fields, clean_fields):
            if raw.strip() and clean.strip():
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
    for k in cleaned[0].keys():
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


def _build_layer_block(layer: Dict[str, Any], styles: Dict, page_width_pts: float) -> List:
    """Construit le bloc ReportLab pour une couche intersectée."""
    flowables = []

    display_name = layer.get("display_name") or layer.get("table") or "Couche inconnue"
    layer_type = layer.get("type") or ""
    article_raw = layer.get("article")
    elements: List[Dict] = layer.get("elements", [])
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
        # keep [] dans le catalogue : uniquement le texte (pas de tableau)
        attr_block = Paragraph(
            "Intersection détectée (données attributaires non disponibles)",
            styles["no_intersection"],
        )
    elif not filtered_elements:
        # Intersection géométrique seule (pas d'attributs visibles)
        attr_block = Paragraph("Intersection détectée (données attributaires non disponibles)", styles["no_intersection"])
    else:
        label_map = _build_attr_label_map(layer)

        # Construire les lignes du tableau attributs
        attr_rows = []
        catalog_keys = _pdf_column_keys(layer)
        if catalog_keys is not None:
            all_keys = catalog_keys
        else:
            all_keys = []
            for el in filtered_elements:
                for k in el.keys():
                    if k not in all_keys:
                        all_keys.append(k)

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

        attr_block = Table(attr_rows, colWidths=col_widths, repeatRows=1)
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
        ("BOTTOMPADDING", (0, 2), (-1, -1), 6),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
    ]))

    flowables.append(KeepTogether([content_table]))
    flowables.append(Spacer(1, 6))
    return flowables


def _build_article_section(
    article_key: str,
    layers: List[Dict],
    styles: Dict,
    page_width_pts: float,
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
        flowables.extend(_build_layer_block(layer, styles, page_width_pts))

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
        canvas.drawCentredString(w / 2, 8 * mm, f"Rapport d'identité foncière – {self.commune} – Page {doc.page}")
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
                "Références cadastrales (UF)"
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
            label = "Références cadastrales (UF)"
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
        output_dir: Dossier de sortie.
        logo_path: Chemin explicite vers logo_kerelia.png (optionnel).
        filename: Nom du fichier de sortie (optionnel, auto-généré sinon).
        catalogue: Dict du catalogue JSON (pour récupérer 'type', 'article', etc.)
                   Si None, on suppose que les intersections portent déjà ces champs.

    Returns:
        str: Chemin absolu du PDF généré.
    """
    intersections: List[Dict] = result.get("intersections", [])
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
    # Page de garde
    # -----------------------------------------------------------------------
    story.append(Spacer(1, 1 * cm))

    # Bloc titre
    story.append(Paragraph("RAPPORT D'IDENTITÉ FONCIÈRE", styles["title"]))
    story.append(Spacer(1, 4))
    story.append(HRFlowable(width="100%", thickness=2, color=C_KERELIA_LIGHT))
    story.append(Spacer(1, 8))

    # Métadonnées
    pl_lbl, pl_html = _meta_parcelle_label_and_html(result)
    meta_rows = [
        ("Commune", commune),
        ("Code INSEE", insee or "—"),
        (pl_lbl, pl_html),
        ("Date d'analyse", date_str),
        ("Couches intersectées", str(nb_intersections)),
    ]
    meta_table_data = [
        [
            Paragraph(k, styles["meta_label"]),
            Paragraph(v, styles["meta_value"]),
        ]
        for k, v in meta_rows
    ]
    meta_table = Table(meta_table_data, colWidths=[5 * cm, 11 * cm])
    meta_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#F0F7F4")),
        ("GRID", (0, 0), (-1, -1), 0.5, C_BORDER),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 14))

    # -----------------------------------------------------------------------
    # Sommaire des types
    # -----------------------------------------------------------------------
    story.append(Paragraph("Résumé par type de contrainte", styles["summary_title"]))
    story.append(Spacer(1, 4))

    type_counts: Dict[str, int] = {}
    for layer in intersections:
        t = layer.get("type") or "—"
        type_counts[t] = type_counts.get(t, 0) + 1

    if type_counts:
        summary_rows = [
            [
                Paragraph("<b>Type</b>", styles["attr_key"]),
                Paragraph("<b>Nb couches</b>", styles["attr_key"]),
            ]
        ]
        for t, cnt in sorted(type_counts.items()):
            c = _type_color(t)
            badge = Table(
                [[Paragraph(f'<font color="white"><b>{_type_label(t)}</b></font>', styles["type_badge"])]],
                colWidths=[max(len(_type_label(t)) * 5.5 + 12, 60)],
                rowHeights=[14],
            )
            badge.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), c),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]))
            summary_rows.append([badge, Paragraph(str(cnt), styles["attr_val"])])

        summary_table = Table(summary_rows, colWidths=[9 * cm, 3 * cm])
        summary_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#E8F5EE")),
            ("GRID", (0, 0), (-1, -1), 0.5, C_BORDER),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F7FCF9")]),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ]))
        story.append(summary_table)

    story.append(PageBreak())

    # -----------------------------------------------------------------------
    # Corps : regroupement par article
    # -----------------------------------------------------------------------
    # Enrichir les intersections avec les infos du catalogue si fourni
    if catalogue:
        for layer in intersections:
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
            _build_article_section(art_key, layers_in_art, styles, page_w)
        )

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