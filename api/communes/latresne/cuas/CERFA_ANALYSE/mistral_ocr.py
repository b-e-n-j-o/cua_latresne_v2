#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
insee_extractor_mistral.py — Extraction du code INSEE avec Mistral OCR + Chat
"""
import os
import re
import logging
import base64
import pandas as pd
from mistralai import Mistral
from pypdf import PdfReader
from pathlib import Path
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()
logger = logging.getLogger("insee_extractor")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")

# Chemin du CSV des communes
INSEE_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv"))

# ============================================================
# PROMPT DÉDIÉ INSEE
# ============================================================
INSEE_EXTRACTION_PROMPT = """
MISSION CRITIQUE : Extraire les informations nécessaires pour identifier le code INSEE de la commune.
📍 LOCALISATION PRÉCISE DANS LE CERFA 13410*12 :
1. PAGE 1, CADRE SUPÉRIEUR DROIT (en-tête du certificat) :
┌─────────────────────────────────────────────────────┐
│ Cadre réservé à la mairie du lieu du projet        │
│                                                     │
│ C U  [XXX] [YYY] [AA] [NNNNN]                     │
│     033    234    25   00078                       │
└─────────────────────────────────────────────────────┘
   → XXX = code département FORMATÉ sur 3 chiffres (ex: 033 pour la Gironde)
   → YYY = code commune sur 3 chiffres (ex: 234 pour commune)

   ⚠️ ATTENTION : Le département a un 0 initial d'affichage !
      033 → le vrai code département est 33
      Code INSEE final = 33234 (5 chiffres : 33 + 234)
2. PAGE 2, SECTION 4.1 "Adresse du (ou des) terrain(s)" :
┌─────────────────────────────────────────────────────┐
│ 4.1 Adresse du (ou des) terrain(s)                │
│ Localité : [NOM DE LA COMMUNE]  ← ICI             │
│ Code postal : [XXXXX]                              │
└─────────────────────────────────────────────────────┘
⚠️ ATTENTION : Prendre la commune du TERRAIN (section 4.1),
              PAS la commune du demandeur (section 3) !
RENVOIE UNIQUEMENT CE JSON (sans texte avant/après) :
{
  "header_dept": "033",
  "header_commune": "234",
  "commune_nom": "commune",
  "code_postal": "33360"
}
RÈGLES STRICTES :
- header_dept : exactement les 3 chiffres affichés dans le header CU (ex: "033")
- header_commune : exactement les 3 chiffres affichés dans le header CU (ex: "234")
- commune_nom : nom exact de la commune tel qu'écrit section 4.1 "Localité"
- code_postal : 5 chiffres de la section 4.1
- Si une valeur est absente, mettre null (ne pas inventer)
EXEMPLE CONCRET :
Si le header affiche "CU 033 234 25 00078" et la localité section 4.1 est "commune"
→ {"header_dept": "033", "header_commune": "234", "commune_nom": "commune", "code_postal": "33360"}
Le code INSEE sera calculé automatiquement : 33234 (sans le 0 initial)
"""

# ============================================================
# UTILITAIRES
# ============================================================
def normalize_commune_name(s):
    """Normalise un nom de commune pour comparaison"""
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r"^(le |la |l'|les )", "", s)
    s = re.sub(r"[-']", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s

def load_communes_db():
    """Charge le CSV des communes avec normalisation"""
    try:
        df = pd.read_csv(INSEE_CSV, dtype=str)
        df['LIBELLE_normalized'] = df['LIBELLE'].apply(normalize_commune_name)
        df['DEP'] = df['DEP'].str.zfill(2)
        return df
    except Exception as e:
        logger.error(f"Impossible de charger {INSEE_CSV}: {e}")
        raise

def extract_json_from_response(text):
    """Extrait le JSON d'une réponse LLM"""
    import json
    try:
        # Trouver le premier bloc JSON valide
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        json_str = text[start:end]
        return json.loads(json_str)
    except Exception:
        return None

# ============================================================
# EXTRACTION LLM AVEC MISTRAL OCR
# ============================================================
def extract_insee_from_pdf(pdf_path, model="mistral-ocr-latest", chat_model="mistral-large-latest"):
    """
    Extrait le texte du PDF avec Mistral OCR, puis utilise un modèle de chat pour extraire le code INSEE.
    """
    client = Mistral(api_key=MISTRAL_API_KEY)

    # Étape 1 : Upload du PDF et extraction du texte avec Mistral OCR
    uploaded_pdf = client.files.upload(
        file={"file_name": os.path.basename(pdf_path), "content": open(pdf_path, "rb")},
        purpose="ocr"
    )
    signed_url = client.files.get_signed_url(file_id=uploaded_pdf.id)

    # Extraction OCR
    ocr_response = client.ocr.process(
        model=model,
        document={"type": "document_url", "document_url": signed_url.url},
        include_image_base64=False
    )

    # Récupérer le texte extrait
    ocr_text = "\n\n".join([page.markdown for page in ocr_response.pages])

    # Étape 2 : Utiliser un modèle de chat pour extraire le code INSEE
    chat_response = client.chat.complete(
        model=chat_model,
        messages=[
            {"role": "system", "content": "Tu es un expert en extraction de données depuis des documents administratifs. Réponds UNIQUEMENT avec un JSON valide."},
            {"role": "user", "content": f"{INSEE_EXTRACTION_PROMPT}\n\nTEXTE DU DOCUMENT:\n{ocr_text}"}
        ],
        temperature=0.0
    )

    # Extraire le JSON de la réponse
    response_text = chat_response.choices[0].message.content
    return extract_json_from_response(response_text)

# ============================================================
# VALIDATION CROISÉE
# ============================================================
def validate_insee_with_csv(llm_data, df_communes):
    """Valide et corrige l'INSEE extrait par le LLM en croisant avec le CSV"""
    if not llm_data:
        return None, 'critical', 'llm_failed', {}

    details = {'llm_raw': llm_data, 'checks': {}}

    # Construction INSEE depuis le header
    header_dept = llm_data.get('header_dept', '')
    header_commune = llm_data.get('header_commune', '')
    commune_nom = llm_data.get('commune_nom', '')
    code_postal = llm_data.get('code_postal', '')

    # Vérification du format
    if not (header_dept and header_commune and len(header_dept) == 3 and len(header_commune) == 3):
        details['checks']['header_format'] = False
    else:
        details['checks']['header_format'] = True

    # Construction INSEE : 033234 → 33234
    insee_from_header = None
    if details['checks'].get('header_format'):
        dept_2 = header_dept[1:] if header_dept[0] == '0' else header_dept[:2]
        insee_from_header = f"{dept_2}{header_commune}"
        details['insee_from_header'] = insee_from_header

    # Recherche par code INSEE dans le CSV
    row_by_code = None
    if insee_from_header:
        row_by_code = df_communes[df_communes['COM'] == insee_from_header]
        details['checks']['code_exists'] = len(row_by_code) == 1

    # Recherche par nom de commune
    row_by_name = None
    if commune_nom:
        commune_normalized = normalize_commune_name(commune_nom)
        row_by_name = df_communes[df_communes['LIBELLE_normalized'] == commune_normalized]
        if len(row_by_name) == 0:
            row_by_name = df_communes[df_communes['LIBELLE_normalized'].str.contains(commune_normalized, na=False, regex=False)]
        details['checks']['name_found'] = len(row_by_name) >= 1

    # Vérification cohérence département
    dept_from_cp = code_postal[:2] if code_postal and len(code_postal) == 5 else None
    if dept_from_cp and header_dept:
        dept_header_2 = header_dept[1:] if header_dept[0] == '0' else header_dept[:2]
        details['checks']['dept_coherent'] = dept_from_cp == dept_header_2

    # Décision : Stratégie de validation croisée
    if details['checks'].get('code_exists') and details['checks'].get('name_found'):
        insee = row_by_code.iloc[0]['COM']
        details['commune_officiel'] = row_by_code.iloc[0]['LIBELLE']
        return insee, 'high', 'header+name_match', details
    elif details['checks'].get('code_exists'):
        insee = insee_from_header
        details['commune_officiel'] = row_by_code.iloc[0]['LIBELLE']
        return insee, 'medium', 'header_only', details
    elif details['checks'].get('name_found'):
        insee = row_by_name.iloc[0]['COM']
        details['commune_officiel'] = row_by_name.iloc[0]['LIBELLE']
        return insee, 'low', 'name_only', details
    else:
        return None, 'critical', 'validation_failed', details

# ============================================================
# FONCTION PRINCIPALE
# ============================================================
def extract_insee_robust(pdf_path):
    """Extraction robuste du code INSEE avec validation croisée"""
    logger.info(f"🎯 Début extraction INSEE pour {Path(pdf_path).name}")

    # Charger le CSV des communes
    try:
        df_communes = load_communes_db()
        logger.info(f"📊 CSV communes chargé: {len(df_communes)} communes")
    except Exception as e:
        return {
            'insee': None,
            'confidence': 'critical',
            'method': 'csv_load_failed',
            'error': str(e)
        }

    # Extraction LLM
    llm_data = extract_insee_from_pdf(pdf_path)
    if not llm_data:
        return {
            'insee': None,
            'confidence': 'critical',
            'method': 'llm_extraction_failed',
            'error': 'Échec extraction LLM'
        }

    # Validation croisée
    insee, confidence, method, details = validate_insee_with_csv(llm_data, df_communes)

    result = {
        'insee': insee,
        'confidence': confidence,
        'method': method,
        'commune_nom_officiel': details.get('commune_officiel'),
        'details': details
    }

    if insee:
        logger.info(f"✅ INSEE extrait: {insee} (confiance: {confidence}, méthode: {method})")
    else:
        logger.error(f"❌ Échec extraction INSEE (confiance: {confidence})")

    return result

# ============================================================
# CLI pour tests
# ============================================================
if __name__ == "__main__":
    import argparse
    import json

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    ap = argparse.ArgumentParser(description="Extraction robuste du code INSEE depuis un CERFA")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA")
    ap.add_argument("--out", default=None, help="Fichier JSON de sortie (optionnel)")

    args = ap.parse_args()
    result = extract_insee_robust(args.pdf)

    print("\n" + "="*60)
    print("RÉSULTAT EXTRACTION INSEE")
    print("="*60)
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f"\n💾 Résultat sauvegardé: {args.out}")
