#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pre_analyse_cerfa.py — Pré-analyse complète du CERFA
Extraction simultanée : INSEE + Parcelles + Superficie totale
Analyse uniquement les 4 premières pages via Gemini
"""

import os
import re
import logging
import tempfile
import pandas as pd
import google.generativeai as genai
from pathlib import Path
from pypdf import PdfReader, PdfWriter
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("pre_analyse_cerfa")

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_PRIMARY = "gemini-2.5-pro"
MODEL_FALLBACK = "gemini-2.5-flash"

# Chemin du CSV des communes
INSEE_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv"))

# ============================================================
# PROMPT UNIFIÉ POUR LES 3 EXTRACTIONS
# ============================================================
PRE_ANALYSE_PROMPT = """
MISSION CRITIQUE : Extraire simultanément 3 éléments essentiels depuis les 4 premières pages d'un CERFA 13410*12.

═══════════════════════════════════════════════════════════════
📌 ÉLÉMENT 1 : CODE INSEE DE LA COMMUNE
═══════════════════════════════════════════════════════════════

📍 LOCALISATION PRÉCISE :

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

═══════════════════════════════════════════════════════════════
📌 ÉLÉMENT 2 : PARCELLES CADASTRALES
═══════════════════════════════════════════════════════════════

📍 LOCALISATION :

💠 Page 2 — Section 4.1 (première apparition possible)
On trouve parfois une seule parcelle, sous forme éclatée :
Préfixe : 000 Section : XXXX Numéro : XXXX

💠 Page 4 — Section 4.2 (toutes les références cadastrales)
Exemples de formats possibles :
- Section : AI  Numéro : 0310  Superficie : 5755 m²
- Section : AC  Numéro : 0058  Superficie : 256 m²
- Section : AC  Numéro : 0311  Superficie : 1368 m²
→ Il peut y avoir plusieurs lignes.

RÈGLES :
- Extraire TOUTES les parcelles trouvées sur les pages 2 ET 4
- Une parcelle = section (1-2 lettres majuscules) + numéro (4 chiffres avec zéros initiaux)
- Si une même parcelle apparaît plusieurs fois → une seule occurrence
- Ne devine rien : si un numéro est incomplet, mets null

═══════════════════════════════════════════════════════════════
📌 ÉLÉMENT 3 : SUPERFICIE TOTALE DU TERRAIN
═══════════════════════════════════════════════════════════════

📍 LOCALISATION : PAGE 4, EN BAS

1. Chercher en bas de la page 4 :
   "Superficie totale du terrain (en m2) : XXXX m2"
   ou
   "Superficie totale du terrain (en m²) : XXXX m²"
   ou variantes similaires

2. Si cette ligne n'existe pas, calculer la somme de toutes les superficies 
   des parcelles listées sur la page 4.

RÈGLES :
- superficie_totale_m2 : nombre entier en m² (sans unité dans le JSON)
- Si impossible à déterminer, mettre null

═══════════════════════════════════════════════════════════════
📋 FORMAT DE RÉPONSE JSON STRICT
═══════════════════════════════════════════════════════════════

RENVOIE UNIQUEMENT CE JSON (sans texte avant/après) :

{
  "insee": {
    "header_dept": "033",
    "header_commune": "234",
    "commune_nom": "Latresne",
    "code_postal": "33360"
  },
  "parcelles": [
    {"section": "AC", "numero": "0310"},
    {"section": "AI", "numero": "0058"},
    {"section": "AC", "numero": "0311"}
  ],
  "superficie_totale_m2": 12310
}

RÈGLES STRICTES :
- insee.header_dept : exactement les 3 chiffres affichés dans le header CU (ex: "033")
- insee.header_commune : exactement les 3 chiffres affichés dans le header CU (ex: "234")
- insee.commune_nom : nom exact de la commune tel qu'écrit section 4.1 "Localité"
- insee.code_postal : 5 chiffres de la section 4.1
- parcelles : liste de toutes les parcelles uniques trouvées (section + numero)
- superficie_totale_m2 : nombre entier en m² ou null
- Si une valeur est absente, mettre null (ne pas inventer)

AUCUNE EXPLICATION. UNIQUEMENT LE JSON.
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
        start = text.find("{")
        end = text.rfind("}") + 1
        if start == -1 or end == 0:
            return None
        json_str = text[start:end]
        return json.loads(json_str)
    except Exception:
        # Tentative de nettoyage
        try:
            json_str = re.sub(r",\s*}", "}", json_str)
            json_str = re.sub(r",\s*]", "]", json_str)
            return json.loads(json_str)
        except:
            return None

def create_first_4_pages_pdf(pdf_path):
    """
    Crée un PDF temporaire contenant uniquement les 4 premières pages
    
    Returns:
        str: Chemin vers le PDF temporaire, ou None en cas d'erreur
    """
    try:
        reader = PdfReader(pdf_path)
        num_pages = len(reader.pages)
        
        if num_pages < 4:
            logger.warning(f"⚠️ Le PDF n'a que {num_pages} page(s), on prend toutes les pages disponibles")
            pages_to_extract = num_pages
        else:
            pages_to_extract = 4
        
        # Créer un PDF temporaire avec les premières pages
        writer = PdfWriter()
        for i in range(pages_to_extract):
            writer.add_page(reader.pages[i])
        
        # Créer un fichier temporaire
        temp_fd, temp_path = tempfile.mkstemp(suffix='.pdf', prefix='cerfa_pages1-4_')
        os.close(temp_fd)
        
        with open(temp_path, 'wb') as temp_file:
            writer.write(temp_file)
        
        logger.info(f"📄 PDF temporaire créé avec {pages_to_extract} page(s): {temp_path}")
        return temp_path
    except Exception as e:
        logger.error(f"Erreur création PDF pages 1-4: {e}")
        return None

# ============================================================
# EXTRACTION LLM AVEC GEMINI
# ============================================================
def extract_pre_analyse_from_pdf(pdf_path, model=MODEL_PRIMARY):
    """
    Extrait les 3 éléments (INSEE, parcelles, superficie) depuis les 4 premières pages via Gemini
    
    Args:
        pdf_path: Chemin vers le PDF CERFA complet
        model: Modèle Gemini à utiliser
    
    Returns:
        dict: {
            'insee': {...},
            'parcelles': [...],
            'superficie_totale_m2': 12310
        } ou None en cas d'erreur
    """
    if not GEMINI_API_KEY:
        raise RuntimeError("❌ Clé GEMINI_API_KEY manquante dans .env")
    
    genai.configure(api_key=GEMINI_API_KEY)
    
    # Créer un PDF temporaire avec les 4 premières pages
    temp_pdf_path = create_first_4_pages_pdf(pdf_path)
    if not temp_pdf_path:
        return None
    
    try:
        # Charger le PDF temporaire
        pdf_bytes = Path(temp_pdf_path).read_bytes()
        
        # Appel Gemini avec le PDF
        model_instance = genai.GenerativeModel(model)
        response = model_instance.generate_content([
            {"mime_type": "application/pdf", "data": pdf_bytes},
            PRE_ANALYSE_PROMPT
        ])
        
        response_text = response.text or ""
        logger.info(f"🤖 Réponse Gemini (pré-analyse):\n{response_text[:500]}...")
        
        result = extract_json_from_response(response_text)
        
        if result:
            logger.info(f"📊 JSON parsé avec succès")
        else:
            logger.warning(f"⚠️ Échec parsing JSON depuis la réponse Gemini")
        
        return result
        
    except Exception as e:
        logger.error(f"Erreur extraction Gemini avec {model}: {e}")
        return None
    finally:
        # Nettoyer le fichier temporaire
        if temp_pdf_path and os.path.exists(temp_pdf_path):
            try:
                os.unlink(temp_pdf_path)
                logger.info(f"🧹 Fichier temporaire supprimé: {temp_pdf_path}")
            except Exception as e:
                logger.warning(f"⚠️ Impossible de supprimer le fichier temporaire {temp_pdf_path}: {e}")

# ============================================================
# VALIDATION INSEE (identique à insee_extractor.py)
# ============================================================
def validate_insee_with_csv(llm_data, df_communes):
    """
    Valide et corrige l'INSEE extrait par le LLM en croisant avec le CSV
    
    Returns:
        tuple: (insee_code, confidence, method, details)
    """
    if not llm_data or not llm_data.get('insee'):
        return None, 'critical', 'llm_failed', {}
    
    insee_data = llm_data['insee']
    details = {
        'llm_raw': insee_data,
        'checks': {}
    }
    
    # Construction INSEE depuis header
    header_dept = insee_data.get('header_dept', '')
    header_commune = insee_data.get('header_commune', '')
    commune_nom = insee_data.get('commune_nom', '')
    code_postal = insee_data.get('code_postal', '')
    
    # Vérification format
    if not (header_dept and header_commune and len(header_dept) == 3 and len(header_commune) == 3):
        details['checks']['header_format'] = False
        logger.warning("Format header invalide")
    else:
        details['checks']['header_format'] = True
    
    # Construction INSEE : 033234 → 33234 (enlever le 0 initial du dept)
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
        if len(row_by_code) == 1:
            logger.info(f"✓ Code INSEE {insee_from_header} trouvé dans CSV: {row_by_code.iloc[0]['LIBELLE']}")
    
    # Recherche par nom de commune dans le CSV
    row_by_name = None
    if commune_nom:
        commune_normalized = normalize_commune_name(commune_nom)
        row_by_name = df_communes[df_communes['LIBELLE_normalized'] == commune_normalized]
        
        if len(row_by_name) == 0:
            row_by_name = df_communes[df_communes['LIBELLE_normalized'].str.contains(commune_normalized, na=False, regex=False)]
        
        details['checks']['name_found'] = len(row_by_name) >= 1
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
def pre_analyse_cerfa(pdf_path, primary_model=MODEL_PRIMARY, fallback_model=MODEL_FALLBACK):
    """
    Pré-analyse complète du CERFA : INSEE + Parcelles + Superficie
    
    Args:
        pdf_path: Chemin vers le PDF CERFA
        primary_model: Modèle Gemini principal
        fallback_model: Modèle de fallback
    
    Returns:
        dict: {
            'insee': {
                'code': '33234',
                'confidence': 'high/medium/low/critical',
                'method': 'description',
                'commune_nom_officiel': 'Latresne',
                'details': {...}
            },
            'parcelles': [
                {'section': 'AC', 'numero': '0310'},
                ...
            ],
            'superficie_totale_m2': 12310,
            'model_used': 'gemini-2.5-flash'
        }
    """
    logger.info(f"🎯 Début pré-analyse CERFA pour {Path(pdf_path).name}")
    
    # Chargement CSV pour validation INSEE
    try:
        df_communes = load_communes_db()
        logger.info(f"📊 CSV communes chargé: {len(df_communes)} communes")
    except Exception as e:
        logger.error(f"❌ Impossible de charger le CSV: {e}")
        df_communes = None
    
    # Extraction LLM avec fallback
    llm_data = extract_pre_analyse_from_pdf(pdf_path, primary_model)
    model_used = primary_model
    
    if not llm_data:
        logger.warning(f"⚠️ Échec {primary_model}, fallback vers {fallback_model}")
        import time, random
        time.sleep(random.uniform(2, 4))
        llm_data = extract_pre_analyse_from_pdf(pdf_path, fallback_model)
        model_used = fallback_model
    
    if not llm_data:
        logger.error("❌ Échec extraction LLM (tous modèles)")
        return {
            'insee': {'code': None, 'confidence': 'critical', 'method': 'llm_failed'},
            'parcelles': [],
            'superficie_totale_m2': None,
            'model_used': model_used,
            'error': 'Échec extraction LLM'
        }
    
    # Validation INSEE si CSV disponible
    insee_result = {
        'code': None,
        'confidence': 'critical',
        'method': 'not_validated',
        'commune_nom_officiel': None,
        'details': {}
    }
    
    if df_communes is not None and llm_data.get('insee'):
        insee_code, confidence, method, details = validate_insee_with_csv(llm_data, df_communes)
        insee_result = {
            'code': insee_code,
            'confidence': confidence,
            'method': method,
            'commune_nom_officiel': details.get('commune_officiel'),
            'details': details
        }
    elif llm_data.get('insee'):
        # Pas de CSV, on utilise directement les données LLM
        insee_data = llm_data['insee']
        header_dept = insee_data.get('header_dept', '')
        header_commune = insee_data.get('header_commune', '')
        if header_dept and header_commune:
            dept_2 = header_dept[1:] if header_dept[0] == '0' else header_dept[:2]
            insee_code = f"{dept_2}{header_commune}"
            insee_result = {
                'code': insee_code,
                'confidence': 'low',
                'method': 'llm_only',
                'commune_nom_officiel': insee_data.get('commune_nom'),
                'details': {'llm_raw': insee_data}
            }
    
    # Extraction parcelles
    parcelles = llm_data.get('parcelles', [])
    if not isinstance(parcelles, list):
        parcelles = []
    
    # Extraction superficie
    superficie = llm_data.get('superficie_totale_m2')
    
    result = {
        'insee': insee_result,
        'parcelles': parcelles,
        'superficie_totale_m2': superficie,
        'model_used': model_used
    }
    
    # Logging résumé
    logger.info("="*60)
    logger.info("📊 RÉSULTAT PRÉ-ANALYSE")
    logger.info("="*60)
    logger.info(f"📍 INSEE: {insee_result['code']} (confiance: {insee_result['confidence']})")
    logger.info(f"📋 Parcelles: {len(parcelles)} trouvée(s)")
    logger.info(f"📏 Superficie: {superficie} m²" if superficie else "📏 Superficie: non trouvée")
    logger.info("="*60)
    
    return result

# ============================================================
# CLI pour tests
# ============================================================
if __name__ == "__main__":
    import argparse
    import json
    
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    
    ap = argparse.ArgumentParser(description="Pré-analyse complète d'un CERFA (INSEE + Parcelles + Superficie)")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA")
    ap.add_argument("--out", default=None, help="Fichier JSON de sortie (optionnel)")
    
    args = ap.parse_args()
    
    result = pre_analyse_cerfa(args.pdf)
    
    print("\n" + "="*60)
    print("RÉSULTAT PRÉ-ANALYSE CERFA")
    print("="*60)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    
    if args.out:
        Path(args.out).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding='utf-8')
        print(f"\n💾 Résultat sauvegardé: {args.out}")

