#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Détecteur de pages cadastrales CERFA 13410*12
Identifie automatiquement les pages contenant les références cadastrales
"""

import pypdf
from pathlib import Path


def detecter_pages_cadastrales(pdf_path: str, debug: bool = True) -> dict:
    """
    Détecte les pages contenant les parcelles cadastrales
    
    Args:
        pdf_path: Chemin du PDF
        debug: Afficher le texte extrait de chaque page
    
    Returns:
        {
            "page_principale": int,
            "page_annexe": int | None,
            "nb_pages_total": int,
            "pages_a_extraire": list
        }
    """
    
    if not Path(pdf_path).exists():
        raise FileNotFoundError(f"PDF introuvable: {pdf_path}")
    
    reader = pypdf.PdfReader(pdf_path)
    nb_pages = len(reader.pages)
    
    page_principale = None
    page_annexe = None
    
    # Patterns de détection (flexibles)
    pattern_principale = "4.2"
    pattern_annexe_1 = "ANNEXE"
    pattern_annexe_2 = "Références cadastrales complémentaires"
    
    if debug:
        print(f"📄 Analyse de {nb_pages} pages...\n")
    
    for i, page in enumerate(reader.pages, start=1):
        text = page.extract_text()
        
        if debug:
            print(f"{'='*60}")
            print(f"PAGE {i}")
            print(f"{'='*60}")
        
        if debug:
            # Afficher les 500 premiers caractères
            print(text[:500])
            print(f"\n[...tronqué, total {len(text)} chars]\n")
        
        # Détection page principale (4.2)
        if pattern_principale in text and "cadastral" in text.lower() and page_principale is None:
            page_principale = i
            if debug:
                print(f"✅ PAGE PRINCIPALE DÉTECTÉE (Section 4.2)\n")
        
        # Détection page annexe (patterns flexibles)
        if (pattern_annexe_1 in text and pattern_annexe_2.lower() in text.lower()):
            page_annexe = i
            if debug:
                print(f"✅ PAGE ANNEXE DÉTECTÉE (Annexe cadastrale)\n")
    
    # Fallback
    if page_principale is None:
        page_principale = 2
        if debug:
            print(f"⚠️  Page principale non détectée → fallback page 2")
    
    if page_annexe is None:
        if debug:
            print(f"ℹ️  Aucune page annexe détectée")
    
    result = {
        "page_principale": page_principale,
        "page_annexe": page_annexe,
        "nb_pages_total": nb_pages,
        "pages_a_extraire": [page_principale] + ([page_annexe] if page_annexe else [])
    }
    
    if debug:
        print(f"\n{'='*60}")
        print(f"📌 RÉSULTAT : Pages à extraire = {result['pages_a_extraire']}")
        print(f"{'='*60}\n")
    
    return result


if __name__ == "__main__":
    pdf = "/Users/benjaminbenoit/Downloads/cerfa_CU_13410-2025-10-27.pdf"
    
    result = detecter_pages_cadastrales(pdf, debug=True)
    
    print(f"📊 Résumé :")
    print(f"   Page principale : {result['page_principale']}")
    print(f"   Page annexe     : {result['page_annexe'] or 'N/A'}")
    print(f"   Total pages     : {result['nb_pages_total']}")