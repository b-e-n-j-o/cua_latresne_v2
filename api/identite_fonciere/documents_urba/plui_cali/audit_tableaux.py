"""
Audit des tableaux dans le PLUi CALi
=====================================
Détecte deux types de tableaux :
  TYPE A – Tableaux "désalignés" : détectés via espaces consécutifs dans pdftotext
            (l'ancien algo — fonctionne pour les tableaux typographiques)
  TYPE B – Tableaux Word natifs : cellules de texte structurées, pas de grands
            espaces mais une grille réelle détectée par pdfplumber via les lignes
            et rectangles de la page

Usage : python audit_tables.py <chemin_pdf> [--pages 174-180]
"""

import sys
import re
import argparse
from pathlib import Path

import pdfplumber
import pdfplumber.utils


def detect_type_a(text: str, threshold: int = 4) -> tuple[bool, float]:
    """Ancien algo : espaces consécutifs dans le texte extrait."""
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        return False, 0.0
    table_lines = [l for l in lines if re.search(r" {%d,}" % threshold, l)]
    ratio = len(table_lines) / len(lines)
    return ratio >= 0.25, round(ratio, 3)


def detect_type_b_via_words(page) -> tuple[bool, int, list]:
    """
    Détecte les tableaux Word natifs via l'alignement des mots sur la page.
    pdfplumber expose les mots avec leurs coordonnées x0, y0, x1, y1.
    Un tableau Word produit des colonnes d'alignement régulières.

    Retourne (has_table, nb_clusters_colonnes, colonnes_x)
    """
    words = page.extract_words(
        x_tolerance=3,
        y_tolerance=3,
        keep_blank_chars=False,
    )
    if len(words) < 6:
        return False, 0, []

    # Collecter les positions x0 de début de mot
    x0s = [round(w["x0"]) for w in words]

    # Clustering simple : regrouper les x0 proches (±8px)
    x0s_sorted = sorted(set(x0s))
    clusters = []
    current = [x0s_sorted[0]]
    for x in x0s_sorted[1:]:
        if x - current[-1] <= 8:
            current.append(x)
        else:
            clusters.append(current)
            current = [x]
    clusters.append(current)

    # Garder les clusters qui regroupent plusieurs mots (vrais alignements)
    cluster_centers = []
    for cl in clusters:
        center = sum(cl) / len(cl)
        # Compter combien de mots s'alignent sur ce cluster
        count = sum(1 for x in x0s if abs(x - center) <= 8)
        if count >= 3:  # au moins 3 mots alignés = colonne probable
            cluster_centers.append((round(center), count))

    # Un tableau a au moins 2 colonnes bien définies
    has_table = len(cluster_centers) >= 2
    return has_table, len(cluster_centers), cluster_centers


def detect_type_b_via_rects(page) -> tuple[bool, int]:
    """
    Détecte les tableaux Word natifs via les rectangles (bordures de cellules).
    Word exporte les bordures de tableau comme des <rect> dans le PDF.
    """
    # pdfplumber expose les éléments graphiques via page.rects
    try:
        rects = page.rects
    except AttributeError:
        return False, 0

    # Filtrer les rectangles plausibles pour des bordures de cellule :
    # - pas trop petits (bruit graphique)
    # - pas trop grands (toute la page)
    page_w = page.width
    page_h = page.height

    cell_rects = [
        r for r in rects
        if 20 < r["width"] < page_w * 0.95
        and 5 < r["height"] < page_h * 0.5
        and r.get("non_stroking_color") is not None  # rectangle avec remplissage
        or (20 < r.get("width", 0) < page_w * 0.95
            and 5 < r.get("height", 0) < 60)  # petites hauteurs = lignes de tableau
    ]

    # Si on détecte plusieurs rectangles de hauteur similaire et alignés verticalement
    if len(cell_rects) >= 3:
        return True, len(cell_rects)
    return False, 0


def detect_type_b_via_extract_tables(page) -> tuple[bool, list]:
    """
    Utilise directement l'extracteur de tableaux de pdfplumber.
    Plus fiable mais plus lent — on l'utilise en confirmation.
    """
    try:
        tables = page.extract_tables({
            "vertical_strategy":   "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance":      5,
            "join_tolerance":      5,
            "edge_min_length":     10,
        })
        if tables:
            return True, tables

        # 2e tentative avec strategy "text" pour les tableaux sans bordures
        tables2 = page.extract_tables({
            "vertical_strategy":   "text",
            "horizontal_strategy": "lines",
            "snap_tolerance":      5,
        })
        if tables2:
            return True, tables2

    except Exception:
        pass
    return False, []


def analyze_page(page, page_num: int) -> dict:
    text = page.extract_text() or ""

    type_a, ratio_a = detect_type_a(text)
    type_b_words, nb_cols, col_centers = detect_type_b_via_words(page)
    type_b_rects, nb_rects = detect_type_b_via_rects(page)
    type_b_extract, tables = detect_type_b_via_extract_tables(page)

    # Synthèse
    has_table = type_a or type_b_words or type_b_rects or type_b_extract
    table_type = []
    if type_a:          table_type.append("A(espaces)")
    if type_b_words:    table_type.append(f"B-words({nb_cols} cols)")
    if type_b_rects:    table_type.append(f"B-rects({nb_rects})")
    if type_b_extract:  table_type.append(f"B-extract({len(tables)} tableaux)")

    # Extrait du texte pour contexte
    snippet = " | ".join(
        l.strip() for l in text.splitlines()
        if l.strip() and not re.match(r"^\[?\d+\]?$", l.strip())
        and "PLUi" not in l and "CALi" not in l
    )[:200]

    return {
        "page":          page_num,
        "has_table":     has_table,
        "table_types":   table_type,
        "type_a":        type_a,
        "ratio_a":       ratio_a,
        "type_b_words":  type_b_words,
        "nb_cols":       nb_cols,
        "col_centers":   col_centers,
        "type_b_rects":  type_b_rects,
        "nb_rects":      nb_rects,
        "type_b_extract": type_b_extract,
        "nb_tables_extracted": len(tables),
        "tables_content": [
            [[cell or "" for cell in row] for row in t]
            for t in (tables[:2] if tables else [])  # max 2 tableaux affichés
        ],
        "text_snippet":  snippet,
        "total_chars":   len(text.strip()),
    }


def run_audit(pdf_path: str, page_range: tuple[int, int] | None = None,
              verbose: bool = False):
    W = 72
    print(f"\n{'═'*W}")
    print(f"  AUDIT TABLEAUX – {Path(pdf_path).name}")
    if page_range:
        print(f"  Pages {page_range[0]}–{page_range[1]}")
    print(f"{'═'*W}\n")

    results = []

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        start = (page_range[0] - 1) if page_range else 0
        end   = (page_range[1])     if page_range else total

        for page_num in range(start, min(end, total)):
            page = pdf.pages[page_num]
            info = analyze_page(page, page_num + 1)
            results.append(info)

            if page_num % 50 == 0 and not page_range:
                print(f"  … {page_num+1}/{total}", file=sys.stderr)

    # ── Rapport ───────────────────────────────────────────────────────────────
    pages_with_tables = [r for r in results if r["has_table"]]
    pages_type_b_only = [r for r in results
                         if r["has_table"] and not r["type_a"]]

    print(f"  Pages analysées    : {len(results)}")
    print(f"  Pages avec tableau : {len(pages_with_tables)}")
    print(f"    dont Type A (espaces)     : {sum(1 for r in results if r['type_a'])}")
    print(f"    dont Type B Word natif    : {sum(1 for r in results if r['type_b_words'] or r['type_b_rects'] or r['type_b_extract'])}")
    print(f"    dont Type B non vu avant  : {len(pages_type_b_only)}  ← angle mort de l'ancien script")

    if pages_type_b_only:
        print(f"\n  {'─'*W}")
        print(f"  PAGES TABLEAU TYPE B (non détectées par l'ancien algo)")
        print(f"  {'─'*W}")
        for r in pages_type_b_only:
            print(f"\n  Page {r['page']}  [{', '.join(r['table_types'])}]")
            print(f"  Contexte : {r['text_snippet'][:120]}")

    print(f"\n  {'─'*W}")
    print(f"  DÉTAIL PAGE PAR PAGE")
    print(f"  {'─'*W}")
    print(f"  {'Page':<6} {'Tableau?':<10} {'Types détectés':<35} {'Cols':<6} {'Rects':<7} {'Extrait'}")
    print(f"  {'─'*6} {'─'*10} {'─'*35} {'─'*6} {'─'*7} {'─'*6}")

    for r in results:
        flag   = "✓ OUI" if r["has_table"] else "—"
        types  = ", ".join(r["table_types"]) if r["table_types"] else "—"
        print(f"  {r['page']:<6} {flag:<10} {types:<35} "
              f"{r['nb_cols']:<6} {r['nb_rects']:<7} "
              f"{r['nb_tables_extracted']}")

    # Affichage du contenu extrait des tableaux si verbose
    if verbose:
        print(f"\n  {'─'*W}")
        print(f"  CONTENU EXTRAIT DES TABLEAUX")
        print(f"  {'─'*W}")
        for r in results:
            if r["tables_content"]:
                print(f"\n  ── Page {r['page']} ──")
                for t_idx, table in enumerate(r["tables_content"]):
                    print(f"  Tableau {t_idx+1} ({len(table)} lignes × {max(len(row) for row in table) if table else 0} cols):")
                    for row in table[:8]:  # max 8 lignes affichées
                        cells = [str(c)[:30].replace("\n", " ") for c in row]
                        print("    " + " | ".join(f"{c:<30}" for c in cells))
                    if len(table) > 8:
                        print(f"    … {len(table)-8} lignes supplémentaires")

    print()


def main():
    parser = argparse.ArgumentParser(description="Audit tableaux PLUi")
    parser.add_argument("pdf", help="Chemin vers le PDF")
    parser.add_argument("--pages", "-p",
                        help="Plage de pages ex: 174-180",
                        default=None)
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Afficher le contenu extrait des tableaux")
    args = parser.parse_args()

    page_range = None
    if args.pages:
        parts = args.pages.split("-")
        page_range = (int(parts[0]), int(parts[1]))

    run_audit(args.pdf, page_range, args.verbose)


if __name__ == "__main__":
    main()