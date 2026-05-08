"""
Audit détection des zones – PLUi CALi
Cible : vérifier la qualité du découpage par zone dans les sections 2 & 3
Usage : python audit_zones.py <chemin_pdf>
"""

import sys
import re
from pathlib import Path
from dataclasses import dataclass, field

import pdfplumber

PDF_PATH = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/api/identite_fonciere/documents_urba/plui_cali/PLUI_CALI.pdf"

# ── Détection grande section ──────────────────────────────────────────────────
GRANDE_SECTION_PATTERNS = [
    (re.compile(r"^\s*1\.\s+DISPOSITIONS\s+G[EÉ]N[EÉ]RALES\s*$", re.I), "S1"),
    (re.compile(r"^\s*2\.\s+ZONES?\s+URBAINES?\s*$",               re.I), "S2"),
    (re.compile(r"^\s*3\.\s+ZONES?\s+[AÀ]\s+URBANISER\s*$",        re.I), "S3"),
    (re.compile(r"^\s*4\.\s+ZONE\s+AGRICOLE\s*$",                  re.I), "S4"),
    (re.compile(r"^\s*5\.\s+ZONE\s+NATURELLE\s*$",                 re.I), "S5"),
    (re.compile(r"^\s*6\.\s+ANNEXES?\s*$",                         re.I), "S6"),
]

# Patterns de détection d'entrée de zone — du plus spécifique au plus large
# Chaque pattern doit matcher UNE ligne isolée
ZONE_ENTRY_PATTERNS = [
    # "ZONE UA" ou "ZONE 1AUH" seul sur une ligne
    re.compile(r"^\s*ZONE\s+(\d?[A-Z]+\d*[+]?[a-z]*)\s*$"),
    # "3.1. R" suivi de "EGLEMENT DE LA ZONE 1AUH" sur la ligne suivante
    # → géré séparément via detect_zone_entry_multiline
    # "Règlement de la zone 1AUH" seul
    re.compile(r"^\s*R[EÈ]GLEMENT\s+(?:DE\s+LA\s+)?ZONE\s+(\d?[\w]+\d*[+]?)\s*$", re.I),
    # "3.1. REGLEMENT DE LA ZONE 1AUH" sur une seule ligne
    re.compile(r"^\s*\d+\.\d+\.\s+R[EÈ]GLEMENT\s+(?:DE\s+LA\s+)?ZONE\s+(\d?[\w]+\d*[+]?)\s*$", re.I),
    # Header de zone dans sommaire/titre de section : "ZONE UA ..." avec trailing dots ou tirets
    re.compile(r"^\s*ZONE\s+(\d?[A-Z]+\d*[+]?[a-z]*)\s*[.\-─]{3,}", re.I),
]

# Sous-sections attendues dans chaque zone
SUBSECTION_PATTERN = re.compile(
    r"^\s*(\d+\.\d+\.\d+(?:\.\d+)*)\s*[.\-]?\s*(.{10,80})\s*$"
)

# Sous-sections connues du PLUi CALi (pour validation)
KNOWN_SUBSECTIONS = {
    "DESTINATION":   re.compile(r"DESTINATION\s+DES\s+CONSTRUCTIONS", re.I),
    "VOLUMETRIE":    re.compile(r"VOLUM[EÉ]TRIE|IMPLANTATION", re.I),
    "QUALITE":       re.compile(r"QUALIT[EÉ]\s+URBAINE|ARCHITECTURALE", re.I),
    "PAYSAGER":      re.compile(r"PAYSAGER|ESPACES\s+NON\s+B[AÂ]TIS", re.I),
    "RESEAUX":       re.compile(r"[EÉ]QUIPEMENTS?\s*[&ET]+\s*R[EÉ]SEAUX|DESSERTE", re.I),
}

# Zones attendues dans S2 et S3
EXPECTED_ZONES_S2 = ["UA", "UB", "UC", "UD", "UE", "UH", "UL", "UT", "UX", "UY"]
EXPECTED_ZONES_S3 = ["1AUH", "1AUE", "1AUX", "2AU"]
EXPECTED_ZONES_S4 = ["A"]
EXPECTED_ZONES_S5 = ["N"]


@dataclass
class ZoneBlock:
    zone_code:   str
    section:     str           # S2, S3, S4, S5
    page_start:  int
    page_end:    int = 0
    trigger_line: str = ""     # la ligne qui a déclenché la détection
    trigger_pattern: str = ""  # quel pattern a matché
    subsections_found: list = field(default_factory=list)
    subsections_missing: list = field(default_factory=list)
    page_texts: dict = field(default_factory=dict)   # page → extrait


def detect_grande_section(lines: list[str], current: str) -> str:
    for line in lines:
        for pattern, name in GRANDE_SECTION_PATTERNS:
            if pattern.match(line):
                return name
    return current


def detect_zone_entry(lines: list[str]) -> tuple[str | None, str, str]:
    """Retourne (zone_code, ligne_trigger, nom_pattern) ou (None, '', '')"""
    # Cas 1 : une seule ligne matche
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        for i, pat in enumerate(ZONE_ENTRY_PATTERNS):
            m = pat.match(stripped)
            if m:
                code = m.group(1).upper()
                if len(code) >= 1 and not code.isdigit():
                    return code, stripped, f"pattern_{i+1}"

    # Cas 2 : "3.x. R" sur une ligne + "EGLEMENT DE LA ZONE 1AUH" sur la suivante
    # (artefact de césure de mot en fin de ligne dans le PDF)
    split_prefix = re.compile(r"^\s*(?:\d+\.\d+\.\s+)?R\s*$")
    split_suffix = re.compile(r"^\s*[EÈ]GLEMENT\s+(?:DE\s+LA\s+)?ZONE\s+(\d?[\w]+\d*[+]?)\s*", re.I)
    for i in range(len(lines) - 1):
        if split_prefix.match(lines[i].strip()):
            m = split_suffix.match(lines[i+1].strip())
            if m:
                code = m.group(1).upper()
                if not code.isdigit():
                    return code, f"{lines[i].strip()} {lines[i+1].strip()}", "pattern_split"

    return None, "", ""


def check_subsections(full_text: str) -> tuple[list, list]:
    found, missing = [], []
    for name, pat in KNOWN_SUBSECTIONS.items():
        if pat.search(full_text):
            found.append(name)
        else:
            missing.append(name)
    return found, missing


def run_audit(pdf_path: str):
    print(f"\n{'═'*68}")
    print(f"  AUDIT DÉTECTION ZONES – {Path(pdf_path).name}")
    print(f"{'═'*68}\n")

    zones: list[ZoneBlock] = []
    current_section = "PREAMBULE"
    current_zone: ZoneBlock | None = None

    section_ranges = {}   # section → (first_page, last_page)
    section_first = {}

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)

        for page_num in range(total):
            page = pdf.pages[page_num]
            text = page.extract_text() or ""
            lines = text.splitlines()
            pnum = page_num + 1

            # ── Mise à jour grande section ────────────────────────────────────
            new_section = detect_grande_section(lines, current_section)
            if new_section != current_section:
                if current_section not in section_first:
                    section_first[current_section] = pnum
                section_ranges[current_section] = (
                    section_first.get(current_section, pnum), pnum - 1
                )
                section_first[new_section] = pnum
                current_section = new_section

            # ── Détection entrée de zone ──────────────────────────────────────
            if current_section in ("S2", "S3", "S4", "S5"):
                zone_code, trigger_line, trigger_pat = detect_zone_entry(lines)

                if zone_code and (
                    current_zone is None or zone_code != current_zone.zone_code
                ):
                    # Clore le bloc précédent
                    if current_zone is not None:
                        current_zone.page_end = pnum - 1
                        full_text = "\n".join(current_zone.page_texts.values())
                        current_zone.subsections_found, current_zone.subsections_missing = \
                            check_subsections(full_text)

                    # Ouvrir nouveau bloc
                    current_zone = ZoneBlock(
                        zone_code=zone_code,
                        section=current_section,
                        page_start=pnum,
                        trigger_line=trigger_line,
                        trigger_pattern=trigger_pat,
                    )
                    zones.append(current_zone)

            # Changer de section → fermer la zone en cours
            elif current_zone is not None and new_section != current_section:
                current_zone.page_end = pnum - 1
                full_text = "\n".join(current_zone.page_texts.values())
                current_zone.subsections_found, current_zone.subsections_missing = \
                    check_subsections(full_text)
                current_zone = None

            # Accumuler le texte de la zone courante (extrait 500 chars / page)
            if current_zone is not None:
                current_zone.page_texts[pnum] = text[:500]

            if pnum % 50 == 0:
                print(f"  … {pnum}/{total} pages")

    # Clore dernier bloc
    if current_zone is not None:
        current_zone.page_end = total
        full_text = "\n".join(current_zone.page_texts.values())
        current_zone.subsections_found, current_zone.subsections_missing = \
            check_subsections(full_text)

    section_ranges[current_section] = (section_first.get(current_section, 1), total)

    # ═══════════════════════════════════════════════════════════════════════════
    # RAPPORT
    # ═══════════════════════════════════════════════════════════════════════════

    # 1. Zones détectées
    print(f"\n{'─'*68}")
    print(f"  ZONES DÉTECTÉES ({len(zones)} blocs)")
    print(f"{'─'*68}")
    print(f"  {'Zone':<12} {'Section':<6} {'Pages':<14} {'Nb pages':<10} {'Trigger'}")
    print(f"  {'─'*12} {'─'*6} {'─'*14} {'─'*10} {'─'*30}")
    for z in zones:
        pages_str = f"p.{z.page_start}–{z.page_end}" if z.page_end else f"p.{z.page_start}–?"
        nb = (z.page_end - z.page_start + 1) if z.page_end else "?"
        trigger_short = z.trigger_line[:40] + ("…" if len(z.trigger_line) > 40 else "")
        print(f"  {z.zone_code:<12} {z.section:<6} {pages_str:<14} {str(nb):<10} {trigger_short}")

    # 2. Zones attendues vs trouvées
    print(f"\n{'─'*68}")
    print(f"  COUVERTURE DES ZONES ATTENDUES")
    print(f"{'─'*68}")
    found_codes = {z.zone_code for z in zones}

    for section_name, expected in [
        ("S2 – Zones urbaines",      EXPECTED_ZONES_S2),
        ("S3 – Zones à urbaniser",   EXPECTED_ZONES_S3),
        ("S4 – Zone agricole",       EXPECTED_ZONES_S4),
        ("S5 – Zone naturelle",      EXPECTED_ZONES_S5),
    ]:
        ok      = [z for z in expected if z in found_codes]
        missing = [z for z in expected if z not in found_codes]
        print(f"\n  {section_name}")
        print(f"    ✓ Trouvées  : {', '.join(ok) if ok else '—'}")
        print(f"    ✗ Manquantes: {', '.join(missing) if missing else 'aucune'}")

    # Zones détectées non attendues (faux positifs potentiels)
    all_expected = set(EXPECTED_ZONES_S2 + EXPECTED_ZONES_S3 + EXPECTED_ZONES_S4 + EXPECTED_ZONES_S5)
    unexpected = [z for z in zones if z.zone_code not in all_expected]
    if unexpected:
        print(f"\n  ⚠ Zones détectées NON attendues (vérifier faux positifs) :")
        for z in unexpected:
            print(f"    {z.zone_code:<12} {z.section}  p.{z.page_start}  ← \"{z.trigger_line}\"")

    # 3. Sous-sections dans chaque zone
    print(f"\n{'─'*68}")
    print(f"  SOUS-SECTIONS DÉTECTÉES PAR ZONE")
    print(f"{'─'*68}")
    for z in zones:
        if not z.page_end:
            continue
        ok_str  = " ".join(f"✓{s}" for s in z.subsections_found)
        mis_str = " ".join(f"✗{s}" for s in z.subsections_missing)
        status  = "OK" if not z.subsections_missing else f"INCOMPLET ({len(z.subsections_missing)} manquantes)"
        print(f"  Zone {z.zone_code:<8}  [{status}]")
        if z.subsections_found:
            print(f"    {ok_str}")
        if z.subsections_missing:
            print(f"    {mis_str}")

    # 4. Pages ambiguës : changements de zone sans trigger clair
    print(f"\n{'─'*68}")
    print(f"  PAGES DE TRANSITION – VÉRIFICATION MANUELLE")
    print(f"{'─'*68}")
    for i, z in enumerate(zones):
        prev = zones[i-1] if i > 0 else None
        if prev and z.page_start == prev.page_end + 1:
            print(f"  Zone {prev.zone_code} → Zone {z.zone_code} : transition p.{prev.page_end}→p.{z.page_start}  OK")
        elif prev:
            gap = z.page_start - (prev.page_end or z.page_start)
            if gap > 1:
                print(f"  ⚠ GAP entre Zone {prev.zone_code} (fin p.{prev.page_end}) et Zone {z.zone_code} (début p.{z.page_start}) → {gap-1} pages non attribuées")

    # 5. Stats de couverture globale
    zone_pages_covered = set()
    for z in zones:
        if z.page_end:
            zone_pages_covered.update(range(z.page_start, z.page_end + 1))

    s2_range = section_ranges.get("S2", (0, 0))
    s3_range = section_ranges.get("S3", (0, 0))
    s2_s3_pages = set(range(s2_range[0], s2_range[1]+1)) | set(range(s3_range[0], s3_range[1]+1))
    uncovered = s2_s3_pages - zone_pages_covered

    print(f"\n{'─'*68}")
    print(f"  COUVERTURE PAGES S2+S3")
    print(f"{'─'*68}")
    print(f"  S2 : p.{s2_range[0]}–{s2_range[1]}  ({s2_range[1]-s2_range[0]+1} pages)")
    print(f"  S3 : p.{s3_range[0]}–{s3_range[1]}  ({s3_range[1]-s3_range[0]+1} pages)")
    print(f"  Pages couvertes par une zone : {len(zone_pages_covered & s2_s3_pages)}")
    print(f"  Pages NON attribuées à une zone : {len(uncovered)}")
    if uncovered:
        print(f"  Pages orphelines : {sorted(uncovered)}")

    print()


if __name__ == "__main__":
    run_audit(PDF_PATH)