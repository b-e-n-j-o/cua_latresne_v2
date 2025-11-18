#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
insee_extractor.py — Module de pré-extraction robuste du code INSEE
Extraction LLM + validation CSV croisée pour garantir la fiabilité
"""

import os
import re
import logging
import pandas as pd
import google.generativeai as genai
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("insee_extractor")

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
   → YYY = code commune sur 3 chiffres (ex: 234 pour Latresne)
   
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
  "commune_nom": "Latresne",
  "code_postal": "33360"
}

RÈGLES STRICTES :
- header_dept : exactement les 3 chiffres affichés dans le header CU (ex: "033")
- header_commune : exactement les 3 chiffres affichés dans le header CU (ex: "234")
- commune_nom : nom exact de la commune tel qu'écrit section 4.1 "Localité"
- code_postal : 5 chiffres de la section 4.1
- Si une valeur est absente, mettre null (ne pas inventer)

EXEMPLE CONCRET :
Si le header affiche "CU 033 234 25 00078" et la localité section 4.1 est "Latresne"
→ {"header_dept": "033", "header_commune": "234", "commune_nom": "Latresne", "code_postal": "33360"}
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
    # Supprime articles, tirets, espaces multiples
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
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j == -1:
        return None
    raw = text[i:j+1]
    try:
        import json
        return json.loads(raw)
    except Exception:
        # Tentative de nettoyage
        raw = re.sub(r",\s*}", "}", raw)
        raw = re.sub(r",\s*]", "]", raw)
        try:
            return json.loads(raw)
        except:
            return None

# ============================================================
# EXTRACTION LLM
# ============================================================
def extract_insee_from_llm(pdf_path, model="gemini-2.5-flash"):
    """Extrait les données INSEE via Gemini"""
    try:
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        model_instance = genai.GenerativeModel(model)
        
        pdf = Path(pdf_path)
        response = model_instance.generate_content([
            {"mime_type": "application/pdf", "data": pdf.read_bytes()},
            INSEE_EXTRACTION_PROMPT
        ])
        
        data = extract_json_from_response(response.text or "")
        if not data:
            logger.warning(f"Échec parsing JSON depuis {model}")
            return None
        
        return data
    except Exception as e:
        logger.error(f"Erreur extraction LLM avec {model}: {e}")
        return None

# ============================================================
# VALIDATION CROISÉE
# ============================================================
def validate_insee_with_csv(llm_data, df_communes):
    """
    Valide et corrige l'INSEE extrait par le LLM en croisant avec le CSV
    
    Returns:
        tuple: (insee_code, confidence, method, details)
    """
    if not llm_data:
        return None, 'critical', 'llm_failed', {}
    
    details = {
        'llm_raw': llm_data,
        'checks': {}
    }
    
    # Construction INSEE depuis header
    header_dept = llm_data.get('header_dept', '')
    header_commune = llm_data.get('header_commune', '')
    commune_nom = llm_data.get('commune_nom', '')
    code_postal = llm_data.get('code_postal', '')
    
    # Vérification format
    if not (header_dept and header_commune and len(header_dept) == 3 and len(header_commune) == 3):
        details['checks']['header_format'] = False
        logger.warning("Format header invalide")
    else:
        details['checks']['header_format'] = True
    
    # Construction INSEE : 033234 → 33234 (enlever le 0 initial du dept)
    insee_from_header = None
    if details['checks'].get('header_format'):
        # Le département du CERFA est sur 3 chiffres (033), on prend les 2 derniers
        dept_2 = header_dept[1:] if header_dept[0] == '0' else header_dept[:2]
        insee_from_header = f"{dept_2}{header_commune}"
        details['insee_from_header'] = insee_from_header
    
    # Recherche par code INSEE dans le CSV
    row_by_code = None
    if insee_from_header:
        row_by_code = df_communes[df_communes['COM'] == insee_from_header]
        details['checks']['code_exists'] = len(row_by_code) == 1
        if len(row_by_code) == 1:
            logger.info(f"✓ Code INSEE {insee_from_header} trouvé dans CSV: {row_by_code.iloc[0]['LIBELLE']}")
    
    # Recherche par nom de commune dans le CSV
    row_by_name = None
    if commune_nom:
        commune_normalized = normalize_commune_name(commune_nom)
        # Recherche exacte
        row_by_name = df_communes[df_communes['LIBELLE_normalized'] == commune_normalized]
        
        # Si pas trouvé, recherche fuzzy (contient)
        if len(row_by_name) == 0:
            row_by_name = df_communes[df_communes['LIBELLE_normalized'].str.contains(commune_normalized, na=False, regex=False)]
        
        details['checks']['name_found'] = len(row_by_name) >= 1
        details['name_candidates'] = len(row_by_name)
        if len(row_by_name) >= 1:
            logger.info(f"✓ Commune '{commune_nom}' trouvée dans CSV ({len(row_by_name)} correspondance(s))")
    
    # Vérification cohérence département
    dept_from_cp = code_postal[:2] if code_postal and len(code_postal) == 5 else None
    if dept_from_cp and header_dept:
        dept_header_2 = header_dept[1:] if header_dept[0] == '0' else header_dept[:2]
        details['checks']['dept_coherent'] = dept_from_cp == dept_header_2
        if details['checks']['dept_coherent']:
            logger.info(f"✓ Cohérence département: CP {code_postal} ↔ header {header_dept}")
    
    # DÉCISION : Stratégie de validation croisée
    
    # CAS 1 : Header + Nom concordent parfaitement (HAUTE CONFIANCE)
    if (details['checks'].get('code_exists') and 
        details['checks'].get('name_found') and 
        len(row_by_name) == 1 and
        row_by_code.iloc[0]['COM'] == row_by_name.iloc[0]['COM']):
        
        insee = row_by_code.iloc[0]['COM']
        details['commune_officiel'] = row_by_code.iloc[0]['LIBELLE']
        logger.info(f"✅ HAUTE CONFIANCE: header + nom concordent → {insee} ({details['commune_officiel']})")
        return insee, 'high', 'header+name_match', details
    
    # CAS 2 : Conflit header/nom → Priorité au nom (MOYENNE CONFIANCE)
    if (details['checks'].get('code_exists') and 
        details['checks'].get('name_found') and
        len(row_by_name) == 1):
        
        insee_name = row_by_name.iloc[0]['COM']
        if insee_from_header != insee_name:
            logger.warning(f"⚠️ Conflit: header={insee_from_header} vs nom={insee_name} → Priorité nom")
            details['conflict'] = f"header={insee_from_header}, name={insee_name}"
        
        details['commune_officiel'] = row_by_name.iloc[0]['LIBELLE']
        return insee_name, 'medium', 'name_priority', details
    
    # CAS 3 : Seul le header est valide (MOYENNE CONFIANCE)
    if details['checks'].get('code_exists'):
        details['commune_officiel'] = row_by_code.iloc[0]['LIBELLE']
        logger.info(f"✓ MOYENNE CONFIANCE: validation par header uniquement → {insee_from_header}")
        return insee_from_header, 'medium', 'header_only', details
    
    # CAS 4 : Seul le nom est trouvé (FAIBLE CONFIANCE)
    if details['checks'].get('name_found') and len(row_by_name) == 1:
        insee = row_by_name.iloc[0]['COM']
        details['commune_officiel'] = row_by_name.iloc[0]['LIBELLE']
        logger.warning(f"⚠️ FAIBLE CONFIANCE: validation par nom uniquement → {insee}")
        return insee, 'low', 'name_only', details
    
    # CAS 5 : Nom ambigu (plusieurs correspondances)
    if details['checks'].get('name_found') and len(row_by_name) > 1:
        # Filtrer par département si possible
        if dept_from_cp:
            row_filtered = row_by_name[row_by_name['DEP'] == dept_from_cp]
            if len(row_filtered) == 1:
                insee = row_filtered.iloc[0]['COM']
                details['commune_officiel'] = row_filtered.iloc[0]['LIBELLE']
                logger.info(f"✓ Disambiguation par département: {insee}")
                return insee, 'medium', 'name_disambiguated', details
        
        logger.warning(f"⚠️ Nom ambigu: {len(row_by_name)} correspondances pour '{commune_nom}'")
        details['ambiguous_matches'] = row_by_name[['COM', 'LIBELLE', 'DEP']].to_dict('records')
    
    # CAS 6 : Échec total
    logger.error("❌ Impossible de déterminer l'INSEE de manière fiable")
    return None, 'critical', 'validation_failed', details

# ============================================================
# FONCTION PRINCIPALE
# ============================================================
def extract_insee_robust(pdf_path, primary_model="gemini-2.5-flash", fallback_model="gemini-2.5-flash"):
    """
    Extraction robuste du code INSEE avec validation croisée
    
    Args:
        pdf_path: Chemin vers le PDF CERFA
        primary_model: Modèle Gemini principal
        fallback_model: Modèle de fallback
    
    Returns:
        dict: {
            'insee': '33234',
            'confidence': 'high/medium/low/critical',
            'method': 'description de la méthode utilisée',
            'commune_nom_officiel': 'Latresne',
            'details': {...}
        }
    """
    logger.info(f"🎯 Début extraction INSEE pour {Path(pdf_path).name}")
    
    # Chargement CSV
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
    
    # Extraction LLM avec fallback
    llm_data = extract_insee_from_llm(pdf_path, primary_model)
    model_used = primary_model
    
    if not llm_data:
        logger.warning(f"⚠️ Échec {primary_model}, fallback vers {fallback_model}")
        import time, random
        time.sleep(random.uniform(2, 4))
        llm_data = extract_insee_from_llm(pdf_path, fallback_model)
        model_used = fallback_model
    
    # Validation croisée
    insee, confidence, method, details = validate_insee_with_csv(llm_data, df_communes)
    
    result = {
        'insee': insee,
        'confidence': confidence,
        'method': method,
        'commune_nom_officiel': details.get('commune_officiel'),
        'model_used': model_used,
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