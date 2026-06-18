# -*- coding: utf-8 -*-
"""
builder.py — Génération du Certificat d'Urbanisme (DOCX) pour Argelès-sur-Mer.

Consomme le rapport d'intersections (sortie de intersections.py) : pour chaque
couche concernée, lit la réglementation déjà taguée en base (obj["reglementation"])
et l'écrit dans la bonne section du document. Aucune logique réglementaire ici —
le texte vit en base, le builder ne fait que router et mettre en forme.

Architecture : une fonction par section, le builder itère sur SECTIONS.
- Changer de commune  → nouveau CommuneConfig.
- Logo commune en haut à droite de la 1ʳᵉ page (n° dossier / pagination : à brancher).
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from docx import Document
from docx.shared import Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement

try:
    from api.cuas.argeles.docx_utils import (
        add_markdown_block,
        add_para_with_link,
        parse_markdown_inline,
    )
except ImportError:
    from docx_utils import add_markdown_block, add_para_with_link, parse_markdown_inline

_ARGELES_DIR = Path(__file__).resolve().parent
LOGO_COMMUNE_PATH = _ARGELES_DIR / "logos" / "argeles.png"


# ============================================================
# CONFIG COMMUNE
# ============================================================
@dataclass
class CommuneConfig:
    nom: str = "ARGELÈS-SUR-MER"
    code_insee: str = "66008"
    departement: str = "Pyrénées-Orientales"

    plu_mention: str = ("approuvé le 20/04/2017, révisé le 10/03/2022, "
                        "modifié le 14/12/2023 et le 30/10/2025")
    pprn_mention: str = ("approuvé par arrêté préfectoral du 25/11/2008, "
                         "modifié par arrêté préfectoral du 29/05/2017")
    pgri_mention: str = ("porter à connaissance relatif aux règles de gestion du risque "
                         "d'inondation (PGRI) en date du 11/07/2019")
    geoportail_url: str = "https://data.geopf.fr/annexes/gpu/documents/DU_66008/dd4c8deab39aa04c8938e88dd2337dba/66008_reglement_20251030.pdf"
    hauteurs_url: str = "https://data.geopf.fr/annexes/gpu/documents/DU_66008/dd4c8deab39aa04c8938e88dd2337dba/66008_reglement_graphique_4_20251030.pdf"
    taxe_communale: str = "5 %"
    taxe_departementale: str = "2 %"
    rap: str = "0,40 %"

    maire: str = "Julie SANZ"
    delegation: str = "Didier WINZER, Responsable Service Urbanisme"

    mentions_communales: list = field(default_factory=lambda: [
        "La commune d'Argelès-sur-Mer est située dans une zone contaminée ou susceptible "
        "de l'être par les termites.",
        "La commune d'Argelès-sur-Mer est concernée par un risque d'exposition au plomb.",
        "La commune d'Argelès-sur-Mer est située en zone 3 de sismicité modérée.",
        "La commune d'Argelès-sur-Mer est située en zone 3 : zone à potentiel radon significatif.",
    ])


# Routage couche → section. Tout ce qui n'est pas listé tombe en "prescriptions".
# Couverture catalogue Argelès (catalogue_cua_argeles.json) :
#   dispositions → zonage_plu, hauteurs (art. 3)
#   sup          → sup_assiette_* (art. 4, via module servitudes_reglementees)
#   risques      → ppr, pprif, retrait_gonflement_argiles_2026, old (art. 5)
#   prescriptions→ aoc, prescriptions_*, infos_surf (hors DPU), haies_bocages,
#                  znieffs, zaer, batiments (art. 5/7)
#   métier       → reseaux_enedis_lineaires, prairies_et_natura_2000 (+ natura_2000,
#                  prairies_sensibles via module dédié), servitudes_reglementees
LAYER_TO_SECTION = {
    # DPU : c'est une info de infos_surf filtrée, géré séparément (voir section_dpu)
    # SUP : réglementation via module servitudes_reglementees (voir section_sup)
    "ppr":                   "risques",
    "pprif":                 "risques",
    "retrait_gonflement_argiles_2026": "risques",
    "old":                   "risques",
    "zonage_plu":            "dispositions",
    "hauteurs":              "dispositions",
}
# Couches gérées par un rendu spécifique (pas dans le flux générique "objets")
LAYERS_METIER = {
    "reseaux_enedis_lineaires",
    "prairies_et_natura_2000",
    "natura_2000",
    "prairies_sensibles",
    "servitudes_reglementees",
    "sup_assiette_s",
    "sup_assiette_l",
    "sup_assiette_p",
}
# Statuts à ignorer silencieusement (ne rien montrer au pétitionnaire)
STATUTS_IGNORES = {"erreur", "table_absente"}
MIN_ZONAGE_PCT = 1.0

FONT = "Arial"
TITLE_BAR_FILL = "D9D9D9"


@dataclass
class Context:
    dossier: dict
    rapport: dict
    config: CommuneConfig


# ============================================================
# HELPERS DOCX
# ============================================================
def _set_cell_bg(cell, fill=TITLE_BAR_FILL):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear"); shd.set(qn("w:color"), "auto"); shd.set(qn("w:fill"), fill)
    tcPr.append(shd)


def _set_cell_borders(cell, color="808080", sz=4):
    tcPr = cell._tc.get_or_add_tcPr()
    borders = OxmlElement("w:tcBorders")
    for edge in ("top", "left", "bottom", "right"):
        el = OxmlElement(f"w:{edge}")
        el.set(qn("w:val"), "single"); el.set(qn("w:sz"), str(sz)); el.set(qn("w:color"), color)
        borders.append(el)
    tcPr.append(borders)


def add_title_bar(doc, text):
    table = doc.add_table(rows=1, cols=1)
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = table.rows[0].cells[0]
    _set_cell_bg(cell); _set_cell_borders(cell)
    p = cell.paragraphs[0]; p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(text.upper()); r.bold = True; r.font.size = Pt(10); r.font.name = FONT
    doc.add_paragraph()


def add_para(doc, text="", bold=False, italic=False, size=10, align=None, space_after=4):
    p = doc.add_paragraph()
    if align: p.alignment = align
    if space_after is not None: p.paragraph_format.space_after = Pt(space_after)
    r = p.add_run(text); r.bold = bold; r.italic = italic
    r.font.size = Pt(size); r.font.name = FONT
    return p


def _add_logo_first_page(doc, logo_path: Path = LOGO_COMMUNE_PATH) -> None:
    """Logo commune en haut à droite de la première page."""
    if not logo_path.is_file():
        return
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    try:
        p.add_run().add_picture(str(logo_path), width=Cm(4.0))
    except Exception:
        pass


def add_kv_table(doc, rows):
    table = doc.add_table(rows=0, cols=2)
    for label, value in rows:
        cells = table.add_row().cells
        for c in cells: _set_cell_borders(c)
        cells[0].width = Cm(5.5); cells[1].width = Cm(11.5)
        r0 = cells[0].paragraphs[0].add_run(label); r0.bold = True
        r0.font.size = Pt(10); r0.font.name = FONT
        r1 = cells[1].paragraphs[0].add_run(str(value) if value not in (None, "") else "—")
        r1.font.size = Pt(10); r1.font.name = FONT
    doc.add_paragraph()


# ============================================================
# EXTRACTION DU TEXTE D'UN OBJET
# ============================================================
def _format_cadastre(dossier: dict, rapport: dict) -> Optional[str]:
    """Cadastre du dossier, ou liste des parcelles de l'UF depuis le rapport."""
    cadastre = dossier.get("cadastre")
    if cadastre and str(cadastre).strip():
        return str(cadastre).strip()
    parcelles = rapport.get("parcelles") or []
    if not parcelles:
        return None
    parts = []
    for p in parcelles:
        section = (p.get("section") or "").strip()
        numero = (p.get("numero") or "").strip()
        if section and numero:
            parts.append(f"{section} n°{numero}")
    return ", ".join(parts) if parts else None


def _reglementation_text(obj: dict) -> Optional[str]:
    regl = obj.get("reglementation")
    if regl and str(regl).strip() and str(regl).strip() != "\\N":
        return str(regl).strip()
    return None


def texte_objet(obj: dict, *, skip_reglementation: bool = False) -> Optional[str]:
    """Réglementation taguée en priorité ; fallback selon les attributs métier de la couche."""
    if not skip_reglementation:
        regl = _reglementation_text(obj)
        if regl:
            return regl

    # Hauteurs PLU (libellé long / valeur numérique)
    if any(k in obj for k in ("hauteur", "libelong")):
        parts = []
        libelong = (obj.get("libelong") or "").strip()
        hauteur = obj.get("hauteur")
        if libelong:
            parts.append(libelong)
        if hauteur not in (None, ""):
            parts.append(f"hauteur maximale : {hauteur} m")
        if parts:
            return " — ".join(parts)

    fallback = (
        obj.get("libelle")
        or obj.get("libelong")
        or obj.get("legende")
        or obj.get("nomsuplitt")
        or obj.get("nom_site")
        or obj.get("denom")
        or obj.get("nom")
    )
    return str(fallback).strip() if fallback else None


def _is_dpu_objet(obj: dict) -> bool:
    lib = (obj.get("libelle") or "").lower()
    return "préemption" in lib or "preemption" in lib


def _objets_affichables(key: str, layer: dict) -> list:
    """Filtre les objets d'une couche (ex. DPU déjà traité dans section_dpu)."""
    objets = layer.get("objets") or []
    if key == "infos_surf":
        return [o for o in objets if not _is_dpu_objet(o)]
    return objets


def couches_par_section(rapport: dict):
    """Regroupe les couches concernées par section cible. Retourne {section: [(key, layer)]}."""
    groupes = {}
    for key, layer in rapport.get("intersections", {}).items():
        if key in LAYERS_METIER:
            continue
        if layer.get("status") in STATUTS_IGNORES:
            continue
        if not layer.get("objets"):
            continue
        section = LAYER_TO_SECTION.get(key, "prescriptions")
        groupes.setdefault(section, []).append((key, layer))
    return groupes


def _bloc_couche(doc, layer, prefix_nom=True, objets=None):
    """Écrit le nom de couche en gras + une puce par objet (texte réglementaire)."""
    rows = objets if objets is not None else layer.get("objets") or []
    if not rows:
        return
    if prefix_nom:
        add_para(doc, layer.get("nom") or "", bold=True, space_after=2)
    for obj in rows:
        regl = _reglementation_text(obj)
        if regl:
            add_markdown_block(doc, regl, size=9)
            continue
        txt = texte_objet(obj, skip_reglementation=True)
        if not txt:
            continue
        add_para(doc, "• " + txt, size=9, space_after=3)


def _pct_sig(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def _zones_plu_avec_pct(objets: list) -> list[tuple[str, float]]:
    """Zones PLU avec part de surface UF (> MIN_ZONAGE_PCT), dédoublonnées."""
    seen: dict[str, float] = {}
    for obj in objets:
        pct = _pct_sig(obj)
        if pct <= MIN_ZONAGE_PCT:
            continue
        zone = (obj.get("libelle") or obj.get("zonage_reglement") or "").strip()
        if not zone:
            continue
        seen[zone] = max(seen.get(zone, 0.0), pct)
    return sorted(seen.items(), key=lambda item: -item[1])


def _format_zonage_plu_intro(objets: list) -> tuple[list[str], Optional[str]]:
    """Résumé zonage + parts de surface significatives."""
    items = _zones_plu_avec_pct(objets)
    if items:
        zones = [zone for zone, _ in items]
        if len(items) == 1:
            zone, pct = items[0]
            texte = (
                f"La parcelle est située dans la zone {zone} du PLU "
                f"({pct:.2f} % de la surface)."
            )
        else:
            parts = [f"{zone} ({pct:.2f} %)" for zone, pct in items]
            texte = f"La parcelle est située dans les zones {', '.join(parts)} du PLU."
        return zones, texte

    zones, seen = [], set()
    for obj in objets:
        zone = (obj.get("libelle") or obj.get("zonage_reglement") or "").strip()
        if zone and zone not in seen:
            seen.add(zone)
            zones.append(zone)
    if not zones:
        return [], None
    return zones, f"La parcelle est située dans la zone {', '.join(zones)} du PLU."


def _objets_zonage_significatifs(objets: list) -> list:
    significatifs = [obj for obj in objets if _pct_sig(obj) > MIN_ZONAGE_PCT]
    return significatifs or list(objets)


def _titre_hauteur_obj(obj: dict) -> Optional[str]:
    libelong = (obj.get("libelong") or "").strip()
    return libelong or None


def _write_zonage_plu_details(doc, objets: list, zones: list):
    """Réglementation PLU par zone (markdown) + libellés complémentaires."""
    seen_regl: set[str] = set()
    multi_zones = len(zones) > 1

    for obj in _objets_zonage_significatifs(objets):
        zone_code = (obj.get("libelle") or obj.get("zonage_reglement") or "").strip()
        libelong = (obj.get("libelong") or "").strip()
        regl = _reglementation_text(obj)
        pct = _pct_sig(obj)

        if regl:
            if regl in seen_regl:
                continue
            seen_regl.add(regl)
            if multi_zones and zone_code:
                suffix = f" ({pct:.2f} %)" if pct > MIN_ZONAGE_PCT else ""
                add_para(doc, f"Zone {zone_code}{suffix}", bold=True, space_after=2)
            add_markdown_block(doc, regl, size=9)
            continue

        if libelong and libelong not in zones:
            add_para(doc, f"• {libelong}", size=9, space_after=3)
            continue

        txt = texte_objet(obj, skip_reglementation=True)
        if txt and txt not in zones and txt != libelong and txt != zone_code:
            add_para(doc, f"• {txt}", size=9, space_after=3)


def _write_hauteurs_details(doc, layer: dict, *, show_layer_title: bool = True):
    """Hauteurs PLU : libelong en titre + réglementation markdown."""
    objets = layer.get("objets") or []
    if not objets:
        return False

    if show_layer_title:
        add_para(doc, layer.get("nom") or "Hauteurs maximales (PLU)", bold=True, space_after=2)
    seen_regl: set[str] = set()
    wrote = False

    for obj in objets:
        regl = _reglementation_text(obj)
        titre = _titre_hauteur_obj(obj)

        if regl:
            if regl in seen_regl:
                continue
            seen_regl.add(regl)
            if titre:
                add_para(doc, titre, bold=True, size=9, space_after=2)
            add_markdown_block(doc, regl, size=9)
            wrote = True
            continue

        if titre:
            add_para(doc, f"• {titre}", size=9, space_after=3)
            wrote = True
            continue

        txt = texte_objet(obj, skip_reglementation=True)
        if txt:
            add_para(doc, f"• {txt}", size=9, space_after=3)
            wrote = True

    return wrote


# ============================================================
# SECTIONS
# ============================================================
def section_identite(doc, ctx):
    d = ctx.dossier
    superficie = d.get("superficie") or ctx.rapport.get("surface_indicative")
    add_kv_table(doc, [
        ("Par", d.get("demandeur")),
        ("Demeurant à", d.get("demandeur_adresse")),
        ("Sur un terrain sis", d.get("terrain")),
        ("Cadastre", _format_cadastre(d, ctx.rapport)),
        ("Demande déposée le", d.get("date_depot")),
        ("Superficie", f"{superficie} m²" if superficie else None),
        ("N° de dossier", d.get("numero_cu")),
    ])


def section_vu(doc, ctx):
    c = ctx.config
    add_title_bar(doc, "Demande en vue de connaître les dispositions d'urbanisme applicables au terrain")
    for ligne in [
        "Vu la demande de certificat d'urbanisme ;",
        "Vu le code de l'urbanisme ;",
        f"Vu le Plan local d'urbanisme {c.plu_mention} ;",
        f"Vu le Plan de Prévention des Risques Naturels Prévisibles {c.pprn_mention} ;",
        f"Vu le {c.pgri_mention}.",
    ]:
        add_para(doc, ligne)
    doc.add_paragraph()


def section_dpu(doc, ctx):
    add_title_bar(doc, "Droit de préemption")
    # Le DPU vient de infos_surf (ligne libelle='Droit de Préemption Urbain', taguée)
    dpu_txt = None
    for key in ("infos_surf",):
        layer = ctx.rapport.get("intersections", {}).get(key, {})
        for obj in layer.get("objets", []):
            lib = (obj.get("libelle") or "").lower()
            if "préemption" in lib or "preemption" in lib:
                dpu_txt = texte_objet(obj)
                break
    if dpu_txt:
        add_para(doc, dpu_txt)
    else:
        add_para(doc, "La parcelle n'est pas soumise au Droit de Préemption Urbain (DPU).")
    doc.add_paragraph()


def section_sup(doc, ctx):
    serv = ctx.rapport.get("intersections", {}).get("servitudes_reglementees", {})
    servitudes = serv.get("servitudes") or []
    if not servitudes:
        return
    add_title_bar(doc, "Servitudes d'utilité publique")
    for i, s in enumerate(servitudes):
        if i > 0:
            doc.add_paragraph()
        titre = s.get("libelle") or s.get("nomsuplitt") or s.get("suptype") or "Servitude"
        add_para(doc, titre, bold=True, space_after=4)
        regl = (s.get("reglementation") or "").strip()
        if regl:
            add_markdown_block(doc, regl, size=9)
        url_gpu = (s.get("url_fiche_gpu") or "").strip()
        if url_gpu:
            add_para_with_link(
                doc,
                "Fiche GPU : ",
                "consulter la fiche réglementaire",
                url_gpu,
                size=9,
                space_after=4,
            )
        base = (s.get("base_legale") or "").strip()
        if base:
            p = doc.add_paragraph()
            p.paragraph_format.space_before = Pt(4)
            p.paragraph_format.space_after = Pt(8)
            parse_markdown_inline(p, f"Base légale : {base}", size=9, italic_base=True)
    doc.add_paragraph()


def section_risques(doc, ctx):
    groupes = couches_par_section(ctx.rapport)
    layers = groupes.get("risques", [])
    if not layers:
        return
    add_title_bar(doc, "Risques naturels et technologiques")
    for key, layer in layers:
        _bloc_couche(doc, layer)
    doc.add_paragraph()


def section_prescriptions(doc, ctx):
    groupes = couches_par_section(ctx.rapport)
    layers = groupes.get("prescriptions", [])
    if not layers:
        return
    add_title_bar(doc, "Prescriptions et informations applicables au terrain")
    for key, layer in layers:
        _bloc_couche(doc, layer, objets=_objets_affichables(key, layer))
    doc.add_paragraph()


def _write_prairies_natura_details(doc, pn: dict):
    """Natura 2000 / Prairies : blocs réglementaires depuis la table dédiée."""
    if not (pn.get("has_natura") or pn.get("has_prairie")):
        return

    add_title_bar(doc, "Natura 2000 / Prairies sensibles")
    diag = (pn.get("diagnostic_metier") or "").strip()
    if diag:
        add_para(doc, diag, bold=True, space_after=4)

    blocs = pn.get("blocs") or []
    if not blocs:
        for legacy in (pn.get("natura"), pn.get("prairie")):
            if not legacy:
                continue
            regl = (legacy.get("reglementation") or legacy.get("laius") or "").strip()
            if regl:
                blocs.append(legacy)

    for bloc in blocs:
        titre = (bloc.get("nom_regime") or bloc.get("code_regime") or "").strip()
        if titre:
            add_para(doc, titre, bold=True, space_after=2)
        statut = (bloc.get("statut_juridique") or bloc.get("statut") or "").strip()
        if statut:
            add_para(doc, f"Statut juridique : {statut}", size=9, italic=True, space_after=2)
        regl = (bloc.get("reglementation") or bloc.get("laius") or "").strip()
        if regl:
            add_markdown_block(doc, regl, size=9)
        base = (bloc.get("base_legale") or "").strip()
        if base:
            p = doc.add_paragraph()
            p.paragraph_format.space_after = Pt(3)
            parse_markdown_inline(p, f"Base légale : {base}", size=9, italic_base=True)

    doc.add_paragraph()


def section_metier(doc, ctx):
    """Modules métier (ENEDIS, Natura/Prairies) : rendu via leurs champs dédiés."""
    inter = ctx.rapport.get("intersections", {})

    enedis = inter.get("reseaux_enedis_lineaires", {})
    analyses = enedis.get("analyses") or []
    if analyses:
        add_title_bar(doc, "Desserte par les réseaux (ENEDIS)")
        for a in analyses:
            diag = a.get("diagnostic_expert_raccordement")
            if diag:
                add_para(doc, f"• {a.get('type_reseau', 'Réseau')} : {diag}", size=9, space_after=3)
        doc.add_paragraph()

    pn = inter.get("prairies_et_natura_2000", {})
    _write_prairies_natura_details(doc, pn)


def section_dispositions(doc, ctx):
    c = ctx.config
    add_title_bar(doc, "Nature des dispositions d'urbanisme applicables au terrain")
    for ligne in [
        "Articles d'ordre public du Règlement National d'Urbanisme : R.111-2, R.111-4, "
        "R.111-25 à R.111-27 et R.111-31 à R.111-51 du Code de l'urbanisme.",
        "Articles L.111-6 à L.111-10 du Code de l'urbanisme.",
        "Loi Littoral n° 86-2 du 3 janvier 1986.",
        "Loi Montagne n° 85-30 du 9 janvier 1985.",
        f"Plan local d'urbanisme (PLU) {c.plu_mention}.",
    ]:
        add_para(doc, ligne)

    doc.add_paragraph()

    # ── Zonage PLU ──
    zonage = ctx.rapport.get("intersections", {}).get("zonage_plu", {})
    objets_zonage = zonage.get("objets") or []
    zones, intro_zonage = _format_zonage_plu_intro(objets_zonage)

    add_title_bar(doc, "Zonage du PLU")
    add_para_with_link(
        doc,
        "Règlement PLU consultable sur : ",
        "règlement PLU (PDF)",
        c.geoportail_url,
        space_after=6,
    )
    if intro_zonage:
        add_para(doc, intro_zonage, bold=True, space_after=6)
    else:
        add_para(doc, "Zonage PLU non déterminé pour ce terrain.", italic=True, space_after=6)
    _write_zonage_plu_details(doc, objets_zonage, zones)

    # ── Hauteurs PLU ──
    groupes = couches_par_section(ctx.rapport)
    layer_hauteurs = next(
        (layer for key, layer in groupes.get("dispositions", []) if key == "hauteurs"),
        None,
    )
    if layer_hauteurs and (layer_hauteurs.get("objets") or []):
        doc.add_paragraph()
        add_title_bar(doc, "Réglementation liée aux hauteurs")
        add_para_with_link(
            doc,
            "Carte des hauteurs consultable sur : ",
            "plan des hauteurs (PDF)",
            c.hauteurs_url,
            space_after=6,
        )
        _write_hauteurs_details(doc, layer_hauteurs, show_layer_title=False)

    # Autres couches art. 3 éventuelles
    for key, layer in groupes.get("dispositions", []):
        if key in ("zonage_plu", "hauteurs"):
            continue
        _bloc_couche(doc, layer, objets=_objets_affichables(key, layer))

    doc.add_paragraph()


def section_sursis(doc, ctx):
    add_title_bar(doc, "Sursis à statuer")
    add_para(doc, "La commune peut décider de surseoir à statuer, dans les conditions et délais "
                  "prévus à l'article L.424-1 du code de l'urbanisme, sur les demandes d'autorisation "
                  "qui seraient de nature à compromettre ou rendre plus onéreuse l'exécution du futur PLU.")
    doc.add_paragraph()


def section_taxes(doc, ctx):
    c = ctx.config
    add_title_bar(doc, "Taxes et contributions")
    add_para(doc, "Les taxes et contributions ne peuvent être déterminées précisément qu'à l'examen "
                  "de la demande d'autorisation.")
    add_para(doc, "Fiscalité applicable au terrain :", bold=True)
    add_para(doc, f"– Taxe d'aménagement – part communale (taux : {c.taxe_communale})")
    add_para(doc, f"– Taxe d'aménagement – part départementale (taux : {c.taxe_departementale})")
    add_para(doc, f"– Redevance d'archéologie préventive (taux : {c.rap})")
    add_para(doc, "Participations applicables au terrain :", bold=True)
    add_para(doc, "– Projet Urbain Partenarial")
    add_para(doc, "– Participation pour équipements publics exceptionnels")
    doc.add_paragraph()


def section_formalites(doc, ctx):
    add_title_bar(doc, "Formalités administratives préalables à l'opération")
    add_para(doc, "Préalablement à l'édification de construction ou à la réalisation de l'opération "
                  "projetée, les formalités ci-après devront être accomplies : Permis de Construire, "
                  "Permis d'Aménager, Déclaration Préalable, Permis de Démolir.")
    add_para(doc, "Attention : le non-respect de ces formalités ou l'utilisation du sol en "
                  "méconnaissance des règles indiquées est passible de l'amende prévue à "
                  "l'article L.480-4 du Code de l'urbanisme.", italic=True)
    doc.add_paragraph()


def section_signature(doc, ctx):
    c = ctx.config
    add_para(doc, f"{c.nom.title()}, le {datetime.now().strftime('%d/%m/%Y')}", align=WD_ALIGN_PARAGRAPH.RIGHT)
    add_para(doc, f"Le Maire, {c.maire}", align=WD_ALIGN_PARAGRAPH.RIGHT)
    add_para(doc, "Pour le Maire, par délégation", align=WD_ALIGN_PARAGRAPH.RIGHT)
    add_para(doc, c.delegation, align=WD_ALIGN_PARAGRAPH.RIGHT)
    doc.add_paragraph()
    add_para(doc, "Le présent certificat est transmis au représentant de l'État (article L.2131-1 du CGCT).",
             size=9, italic=True)


def section_mentions(doc, ctx):
    for m in ctx.config.mentions_communales:
        add_para(doc, f"– {m}", size=9)
    doc.add_paragraph()


def section_informations(doc, ctx):
    add_title_bar(doc, "Informations")
    add_para(doc, "Durée de validité :", bold=True, size=9)
    add_para(doc, "Le certificat est valable dix-huit mois. Une demande déposée dans ce délai bénéficie "
                  "de la cristallisation des règles d'urbanisme, taxes et participations en vigueur à la "
                  "date du certificat, sauf dispositions de sécurité ou de salubrité publique.", size=9)
    add_para(doc, "Prolongation (article R.410-17 du code de l'urbanisme) :", bold=True, size=9)
    add_para(doc, "Prorogeable par période d'un an, sur demande deux mois avant l'expiration, si les règles "
                  "applicables n'ont pas évolué.", size=9)
    add_para(doc, "Délais et voies de recours :", bold=True, size=9)
    add_para(doc, "Recours contentieux possible devant le tribunal administratif dans les deux mois suivant "
                  "la notification (www.telerecours.fr).", size=9)


# Ordre du document
SECTIONS = [
    section_identite,
    section_vu,
    section_dpu,
    section_dispositions,
    section_sup,
    section_risques,
    section_prescriptions,
    section_metier,
    section_sursis,
    section_taxes,
    section_formalites,
    section_signature,
    section_mentions,
    section_informations,
]


# ============================================================
# POINT D'ENTRÉE
# ============================================================
def _setup_document() -> Document:
    doc = Document()
    st = doc.styles["Normal"]; st.font.name = FONT; st.font.size = Pt(10)
    s = doc.sections[0]
    s.page_height = Cm(29.7); s.page_width = Cm(21.0)
    for m in ("top_margin", "bottom_margin", "left_margin", "right_margin"):
        setattr(s, m, Cm(2.0))
    return doc


def build_cua(dossier: dict, rapport: dict, output_path: str,
              config: Optional[CommuneConfig] = None) -> str:
    config = config or CommuneConfig()
    ctx = Context(dossier=dossier, rapport=rapport, config=config)
    doc = _setup_document()
    _add_logo_first_page(doc)
    add_para(doc, "CERTIFICAT D'URBANISME", bold=True, size=14, align=WD_ALIGN_PARAGRAPH.CENTER)
    add_para(doc, "délivré par le Maire au nom de la commune", italic=True, align=WD_ALIGN_PARAGRAPH.CENTER)
    doc.add_paragraph()
    for section in SECTIONS:
        section(doc, ctx)
    doc.save(output_path)
    return output_path


if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser(description="Builder CUA Argelès (v2 — consomme reglementation)")
    ap.add_argument("--rapport", required=True)
    ap.add_argument("--dossier", default=None)
    ap.add_argument("--output", default="CUA_argeles.docx")
    args = ap.parse_args()
    rapport = json.loads(open(args.rapport, encoding="utf-8").read())
    dossier = json.loads(open(args.dossier, encoding="utf-8").read()) if args.dossier else {}
    print("✅ CUA généré :", build_cua(dossier, rapport, args.output))