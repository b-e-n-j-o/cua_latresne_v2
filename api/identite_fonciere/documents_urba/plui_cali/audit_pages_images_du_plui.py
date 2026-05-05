"""
Audit du PLUi CALi – Libournais
Cible : identifier les pages texte-pur vs pages nécessitant vision LLM
        (tableaux désalignés, images raster, pages mixtes)
Usage : python audit_plui.py <chemin_pdf>
"""

import sys
import re
import json
from pathlib import Path
from collections import defaultdict

import fitz          # PyMuPDF
import pdfplumber

PDF_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/api/identite_fonciere/documents_urba/plui_cali/PLUI_CALI.pdf"

# ── Seuils ────────────────────────────────────────────────────────────────────
# Une ligne est "tableau-like" si elle contient ≥ N espaces consécutifs
TABLE_SPACES_THRESHOLD = 4
# Une page est "table-heavy" si ≥ X% de ses lignes non-vides semblent tabulaires
TABLE_LINE_RATIO = 0.25
# Nb min de caractères de texte pour qu'une page soit considérée "texte"
MIN_TEXT_CHARS = 80

# ── Regex pour détecter les grandes sections ──────────────────────────────────
SECTION_PATTERNS = [
    (re.compile(r"^\s*1\.\s+DISPOSITIONS\s+G[EÉ]N[EÉ]RALES", re.I), "S1_DISPOSITIONS_GENERALES"),
    (re.compile(r"^\s*2\.\s+ZONES?\s+URBAINES?", re.I),               "S2_ZONES_URBAINES"),
    (re.compile(r"^\s*3\.\s+ZONES?\s+[AÀ]\s+URBANISER", re.I),        "S3_ZONES_AU"),
    (re.compile(r"^\s*4\.\s+ZONE\s+AGRICOLE", re.I),                  "S4_ZONE_A"),
    (re.compile(r"^\s*5\.\s+ZONE\s+NATURELLE", re.I),                 "S5_ZONE_N"),
    (re.compile(r"^\s*6\.\s+ANNEXES?", re.I),                         "S6_ANNEXES"),
]

ZONE_PATTERN = re.compile(
    r"^\s*(?:R[EÈ]GLEMENT\s+(?:DE\s+LA\s+)?)?ZONE\s+([\w\d]+)\s*$", re.I
)
SUBSECTION_PATTERN = re.compile(r"^\s*(\d+\.\d+(?:\.\d+)*)\.\s+(.+)$")


def classify_page(page_text: str, has_raster_images: bool) -> dict:
    """Retourne un dict de classification pour une page."""
    lines = page_text.splitlines()
    non_empty = [l for l in lines if l.strip()]

    total_chars = len(page_text.strip())

    # Détection tableau : lignes avec gros blocs d'espaces consécutifs
    table_lines = [
        l for l in non_empty
        if re.search(r" {%d,}" % TABLE_SPACES_THRESHOLD, l)
    ]
    table_ratio = len(table_lines) / max(len(non_empty), 1)

    is_table_heavy = table_ratio >= TABLE_LINE_RATIO
    is_text_poor   = total_chars < MIN_TEXT_CHARS
    needs_vision   = is_table_heavy or is_text_poor or has_raster_images

    return {
        "total_chars":    total_chars,
        "non_empty_lines": len(non_empty),
        "table_lines":    len(table_lines),
        "table_ratio":    round(table_ratio, 3),
        "has_raster":     has_raster_images,
        "is_table_heavy": is_table_heavy,
        "is_text_poor":   is_text_poor,
        "needs_vision":   needs_vision,
    }


def detect_section(text: str, current_section: str) -> str:
    for line in text.splitlines():
        for pattern, name in SECTION_PATTERNS:
            if pattern.match(line):
                return name
    return current_section


def detect_zone(text: str) -> str | None:
    for line in text.splitlines():
        m = ZONE_PATTERN.match(line)
        if m:
            return m.group(1).upper()
    return None


def run_audit(pdf_path: str):
    path = Path(pdf_path)
    print(f"\n{'═'*60}")
    print(f"  AUDIT PLUi – {path.name}")
    print(f"{'═'*60}\n")

    doc_fitz = fitz.open(pdf_path)
    total_pages = len(doc_fitz)

    pages_info = []
    current_section = "PREAMBULE"
    current_zone    = None

    needs_vision_pages   = []
    text_only_pages      = []
    table_pages          = []
    image_pages          = []
    section_page_map     = defaultdict(list)  # section → liste de numéros de page
    zone_page_map        = defaultdict(list)  # zone → liste de numéros de page

    with pdfplumber.open(pdf_path) as plumber_doc:
        for page_num in range(total_pages):
            # ── Texte via pdfplumber ──────────────────────────────────────────
            plumber_page = plumber_doc.pages[page_num]
            text = plumber_page.extract_text() or ""

            # ── Images raster via PyMuPDF ─────────────────────────────────────
            fitz_page   = doc_fitz[page_num]
            raster_imgs = fitz_page.get_images(full=False)
            has_raster  = len(raster_imgs) > 0

            # ── Classification ────────────────────────────────────────────────
            info = classify_page(text, has_raster)
            info["page"]    = page_num + 1
            info["section"] = current_section

            # Mise à jour section
            new_section = detect_section(text, current_section)
            if new_section != current_section:
                current_section = new_section
                current_zone    = None  # reset zone à chaque nouvelle grande section
            info["section"] = current_section

            # Mise à jour zone
            detected_zone = detect_zone(text)
            if detected_zone:
                current_zone = detected_zone
            info["zone"] = current_zone

            pages_info.append(info)
            section_page_map[current_section].append(page_num + 1)
            if current_zone:
                zone_page_map[current_zone].append(page_num + 1)

            if info["needs_vision"]:
                needs_vision_pages.append(page_num + 1)
            else:
                text_only_pages.append(page_num + 1)
            if info["is_table_heavy"]:
                table_pages.append(page_num + 1)
            if has_raster:
                image_pages.append(page_num + 1)

            # Progress dot
            if (page_num + 1) % 50 == 0:
                print(f"  … {page_num+1}/{total_pages} pages analysées")

    doc_fitz.close()

    # ═══════════════════════════════════════════════════════════════════════════
    # RAPPORT
    # ═══════════════════════════════════════════════════════════════════════════

    print(f"\n{'─'*60}")
    print(f"  RÉSUMÉ GLOBAL ({total_pages} pages)")
    print(f"{'─'*60}")
    print(f"  Pages texte pur (pdftotext suffit) : {len(text_only_pages):>4}  ({100*len(text_only_pages)/total_pages:.1f}%)")
    print(f"  Pages nécessitant vision LLM       : {len(needs_vision_pages):>4}  ({100*len(needs_vision_pages)/total_pages:.1f}%)")
    print(f"    dont tableaux désalignés          : {len(table_pages):>4}")
    print(f"    dont images raster                : {len(image_pages):>4}")

    print(f"\n{'─'*60}")
    print(f"  RÉPARTITION PAR SECTION")
    print(f"{'─'*60}")
    for section, pnums in section_page_map.items():
        vision_count = sum(1 for p in pnums if p in needs_vision_pages)
        print(f"  {section:<35} {len(pnums):>4} pages  (vision: {vision_count})")

    print(f"\n{'─'*60}")
    print(f"  RÉPARTITION PAR ZONE (section 2 & 3)")
    print(f"{'─'*60}")
    for zone, pnums in sorted(zone_page_map.items()):
        vision_count = sum(1 for p in pnums if p in needs_vision_pages)
        print(f"  Zone {zone:<10} {len(pnums):>4} pages  (vision: {vision_count})")

    print(f"\n{'─'*60}")
    print(f"  PAGES NÉCESSITANT VISION LLM")
    print(f"{'─'*60}")
    # Regrouper en plages consécutives
    def to_ranges(nums):
        if not nums:
            return []
        nums = sorted(nums)
        ranges, start, end = [], nums[0], nums[0]
        for n in nums[1:]:
            if n == end + 1:
                end = n
            else:
                ranges.append((start, end))
                start = end = n
        ranges.append((start, end))
        return ranges

    for s, e in to_ranges(needs_vision_pages):
        label = f"p.{s}" if s == e else f"p.{s}–{e}"
        # Retrouver section+zone
        sample = pages_info[s-1]
        ctx = sample["section"]
        if sample["zone"]:
            ctx += f" / Zone {sample['zone']}"
        reasons = []
        if pages_info[s-1]["is_table_heavy"]: reasons.append("tableau")
        if pages_info[s-1]["has_raster"]:     reasons.append("image")
        if pages_info[s-1]["is_text_poor"]:   reasons.append("peu de texte")
        print(f"  {label:<14} [{', '.join(reasons) or '?'}]  {ctx}")

    print(f"\n{'─'*60}")
    print(f"  PAGES TEXTE VIDE / SÉPARATRICES")
    print(f"{'─'*60}")
    empty_pages = [p["page"] for p in pages_info if p["total_chars"] < 20]
    print(f"  {len(empty_pages)} pages quasi-vides (séparateurs de section) : {empty_pages}")

    # Export JSON pour usage programmatique
    out_json = Path("./plui_audit.json")
    out_json.parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({
            "total_pages": total_pages,
            "text_only":   text_only_pages,
            "needs_vision": needs_vision_pages,
            "table_pages": table_pages,
            "image_pages": image_pages,
            "sections":    {k: v for k, v in section_page_map.items()},
            "zones":       {k: v for k, v in zone_page_map.items()},
            "pages":       pages_info,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n  ✓ Données détaillées exportées → {out_json}\n")


if __name__ == "__main__":
    run_audit(PDF_PATH)