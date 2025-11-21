#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_cerfa_to_header.py — Test unitaire d'intégration entre :
- analyse_gemini.py (analyse CERFA)
- cua_header.py (génération en-tête DOCX)

Usage :
    python3 test_cerfa_to_header.py --pdf cerfa_test.pdf --logo logos/logo_latresne.png
"""

import os
import sys
import argparse
import json
from pathlib import Path
from docx import Document

# ============================================================
# 🔧 Configuration du PYTHONPATH pour imports locaux
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent

# Ajouter les chemins nécessaires pour les imports
sys.path.append(str(PARENT_DIR / "CERFA_ANALYSE"))
sys.path.append(str(BASE_DIR))

# Maintenant les imports fonctionnent
from analyse_gemini import analyse_cerfa
from cua_header import render_first_page_header

def test_cerfa_header(pdf_path: str, logo_path: str = None, out_path: str = "test_header.docx"):
    print(f"📄 Analyse du CERFA : {pdf_path}")
    
    # 1️⃣ Analyse du CERFA pour obtenir le JSON
    result = analyse_cerfa(pdf_path, out_json="cerfa_result_test.json", retry_if_incomplete=True)
    data = result.get("data") or {}
    
    print(f"✅ Analyse terminée avec succès : modèle {result.get('model_used')}")
    print(f"\n🔍 Champs principaux extraits :")
    print(f"   - Commune : {data.get('commune_nom')} ({data.get('commune_insee') or 'INSEE non trouvé'})")
    print(f"   - Type CU : {data.get('type_cu')}")
    print(f"   - Numéro CU : {data.get('numero_cu')}")
    print(f"   - Date dépôt : {data.get('date_depot')}")
    
    demandeur = data.get('demandeur') or {}
    print(f"\n👤 Informations demandeur :")
    print(f"   - Type : {demandeur.get('type') or 'non spécifié'}")
    
    if demandeur.get('type') == 'personne_morale':
        print(f"   - Dénomination : {demandeur.get('denomination') or '—'}")
        print(f"   - Représentant : {demandeur.get('representant_prenom')} {demandeur.get('representant_nom')}")
        print(f"   - SIRET : {demandeur.get('siret') or '—'}")
    else:
        print(f"   - Nom : {demandeur.get('nom') or '—'}")
        print(f"   - Prénom : {demandeur.get('prenom') or '—'}")
    
    adresse = demandeur.get('adresse') or {}
    print(f"   - Adresse : {adresse.get('numero') or ''} {adresse.get('voie') or ''}")
    print(f"   - Ville : {adresse.get('code_postal') or ''} {adresse.get('ville') or ''}")
    print(f"   - Email : {adresse.get('email') or '—'}")
    print(f"   - Téléphone : {adresse.get('telephone') or '—'}")
    
    print(f"\n📍 Parcelles : {len(data.get('references_cadastrales') or [])} parcelle(s)")
    
    # 2️⃣ Génération d’un DOCX temporaire avec uniquement le header
    doc = Document()
    
    qr_url = f"https://www.kerelia.fr/carte/{data.get('commune_nom', '').lower()}/{data.get('numero_cu', 'demo')}"
    
    render_first_page_header(
        doc,
        {"data": data},  # le module attend un cerfa dict avec clé "data"
        logo_commune_path=logo_path,
        qr_url=qr_url,
        qr_logo_path="logos/logo_kerelia.png" if os.path.exists("logos/logo_kerelia.png") else None
    )
    
    # 3️⃣ Sauvegarde du fichier DOCX
    doc.save(out_path)
    print(f"📁 Header généré avec succès : {out_path}")
    
    # 4️⃣ Vérification visuelle minimale
    if Path(out_path).exists():
        print("🧩 Test terminé : ouvrez le fichier pour vérifier le rendu visuel.")
    else:
        print("❌ Le fichier DOCX n’a pas été généré.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Test unitaire CERFA → Header DOCX")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA à analyser")
    ap.add_argument("--logo", default=None, help="Chemin du logo de la commune (optionnel)")
    ap.add_argument("--out", default="test_header.docx", help="Nom du fichier DOCX de sortie")
    args = ap.parse_args()

    test_cerfa_header(args.pdf, args.logo, args.out)
