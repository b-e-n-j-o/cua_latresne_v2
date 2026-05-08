#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_mistral.py — Analyse d'un CERFA CU (13410*12)
Mistral OCR → JSON structuré conforme au CUA Builder
avec pré-extraction INSEE robuste, validation + relance intelligente

Utilise mistral-ocr-latest en priorité (méthode robuste : upload + OCR + chat),
avec fallback vers mistral-large-latest puis mistral-small-latest
"""

import os, json, re, time, random, logging, base64
from pathlib import Path
from pypdf import PdfReader
from mistralai import Mistral
import pandas as pd
from dotenv import load_dotenv

# Import du module d'extraction INSEE
from insee_extractor_mistral import extract_insee_robust
# Import du module d'extraction superficie
from superficie_extractor import extract_superficie_totale

# ============================================================
# CONFIG
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("cerfa_analyse")

MODEL_PRIMARY = "mistral-ocr-latest"  # Modèle OCR principal (utilise API OCR robuste)
MODEL_FALLBACK = "mistral-large-latest"  # Fallback vers large (texte extrait)
MODEL_FALLBACK2 = "mistral-small-latest"  # Fallback final vers small
# Chemin vers le CSV INSEE : CONFIG est au même niveau que CERFA_ANALYSE
INSEE_CSV = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv"))

# ============================================================
# INDICES VISUELS DE LOCALISATION
# ============================================================
VISUAL_LOCATION_HINTS = """
═══════════════════════════════════════════════════════════════════════════════
📍 GUIDE DE LOCALISATION VISUELLE - CERFA 13410*12
═══════════════════════════════════════════════════════════════════════════════

📌 EN-TÊTE DU CERTIFICAT (PAGE 1, coin supérieur droit)
┌─────────────────────────────────────────────────────────┐
│ Cadre réservé à la mairie du lieu du projet            │
│                                                         │
│ C U  [Dpt] [Commune] [Année] [N° de dossier]          │
│     033    234       25      00078                     │
│                                                         │
│ La présente déclaration a été reçue à la mairie       │
│ le [JJ]/[MM]/[AAAA]                                   │
└─────────────────────────────────────────────────────────┘

Structure header_cu :
• Département : 3 chiffres (ex: 033 = Gironde)
• Commune : 3 chiffres (ex: 234 = code commune)
• Année : 2 chiffres (ex: 25 = 2025)
• N° dossier : 5 chiffres (ex: 00078)

Code insee : [Dpt][Commune]
Exemple : 033234 = 33234, le code insee est à 5 chiffres (33 + 234)

📌 TYPE DE CERTIFICAT (PAGE 1, section 1)
┌─────────────────────────────────────────────────────────┐
│ 1 Objet de la demande de certificat d'urbanisme       │
│                                                         │
│ ☑ a) Certificat d'urbanisme d'information             │
│ ☐ b) Certificat d'urbanisme opérationnel              │
└─────────────────────────────────────────────────────────┘

Règle : Si case "a)" cochée → type_cu = "CUa"
        Si case "b)" cochée → type_cu = "CUb"

📌 IDENTITÉ DU DEMANDEUR (PAGE 1, section 2)

Pour un PARTICULIER (section 2.1) :
┌─────────────────────────────────────────────────────────┐
│ 2.1 Vous êtes un particulier                          │
│ Nom : [NOM]          Prénom : [PRENOM]                │
└─────────────────────────────────────────────────────────┘

Pour une PERSONNE MORALE (section 2.2) :
┌─────────────────────────────────────────────────────────┐
│ 2.2 Vous êtes une personne morale                     │
│ Dénomination : [RAISON SOCIALE]                        │
│ Raison sociale : [TYPE]                                │
│ N° SIRET : [14 CHIFFRES]  Type : [SARL/SA/SCI...]    │
│ Représentant : Nom [NOM]  Prénom [PRENOM]             │
└─────────────────────────────────────────────────────────┘

📌 ADRESSE DU TERRAIN (PAGE 2, section 4.1)
┌─────────────────────────────────────────────────────────┐
│ 4.1 Adresse du (ou des) terrain(s)                    │
│ Numéro : [N°]     Voie : [NOM DE RUE]                 │
│ Lieu-dit : [LIEU-DIT si présent]                      │
│ Localité : [NOM COMMUNE]     ← NOM DE LA COMMUNE ICI  │
│ Code postal : [5 CHIFFRES]   ← Dept = 2 premiers      │
└─────────────────────────────────────────────────────────┘

⚠️ ATTENTION : L'adresse du terrain (section 4) est DIFFÉRENTE de
              l'adresse du demandeur (section 3, page 2)

📌 RÉFÉRENCES CADASTRALES - ⚠️ TRÈS IMPORTANT : VÉRIFIER 2 PAGES

📍 PAGE 2, SECTION 4.2 (Parcelle principale) :
┌─────────────────────────────────────────────────────────┐
│ 4.2 Références cadastrales :                           │
│                                                         │
│ Section : [AI]  Numéro : [0310]  Superficie : 5755 m² │
│                                                         │
│ Superficie totale du terrain (en m²) : 12310 
    Si la superficie est manquante , la chercher en bas de page 4         │
└─────────────────────────────────────────────────────────┘

📍 PAGE 4 (Parcelles supplémentaires) - ⚠️ TOUJOURS VÉRIFIER :
┌─────────────────────────────────────────────────────────┐
│ Prefixe XXX Section [AI] Numero [0310]                │
│ Superficie de la parcelle cadastrale en m2 : 5755     │
│                                                         │
│ Prefixe XXX Section [AI] Numero [0058]                │
│ Superficie de la parcelle cadastrale en m2 : 256      │
│                                                         │
│ Prefixe XXX Section [AI] Numero [0311]                │
│ Superficie de la parcelle cadastrale en m2 : 1368     │
│                                                         │
│ Superficie totale du terrain (en m2) : 12310 m2       │
│                    ↑ TRÈS IMPORTANT : EN BAS DE PAGE 4 │
└─────────────────────────────────────────────────────────┘

Format parcelles :
• Section : 1-2 LETTRES MAJUSCULES (ex: AI, AC, ZA)
• Numéro : 4 CHIFFRES avec zéros initiaux (ex: 0310, 0058)
• Superficie : nombre entier en m²

⚠️ RÈGLES CRITIQUES PARCELLES :
1. La parcelle principale est à la PAGE 2, section 4.2
2. TOUJOURS vérifier la PAGE 4 pour d'autres parcelles supplémentaires
3. La PAGE 4 liste les parcelles une à une selon le format :
   "Prefixe XXX Section XXX Numero XXX Superficie de la parcelle cadastrale en m2 : XX"
4. EN BAS DE LA PAGE 4 : "Superficie totale du terrain (en m2) : XXXX m2"
   → Cette valeur est LA VRAIE superficie totale à utiliser
5. Il faut ABSOLUMENT extraire TOUTES les parcelles (page 2 + page 4)
6. La superficie totale DOIT correspondre à celle indiquée en bas de la page 4

📌 NUMÉRO CU COMPLET (à reconstruire)
Format final attendu : [Dept]-[Commune]-20[Année]-X[Dossier]
Exemple : 033-234-2025-X00078

═══════════════════════════════════════════════════════════════════════════════
⚠️ RÈGLES CRITIQUES
═══════════════════════════════════════════════════════════════════════════════
1. Le header_cu se trouve TOUJOURS page 1, cadre supérieur droit
2. La commune_nom vient de section 4.1 "Localité" (PAS section 3)
3. ⚠️ PARCELLES : TOUJOURS vérifier la PAGE 4 pour parcelles supplémentaires
   - Page 2, section 4.2 : parcelle principale
   - Page 4 : autres parcelles listées une à une
   - En bas de la page 4 : superficie totale du terrain (à utiliser)
4. La superficie totale DOIT être celle indiquée en bas de la page 4
5. Il faut ABSOLUMENT extraire TOUTES les parcelles (page 2 + page 4)
6. Ne JAMAIS inventer de valeurs absentes du document
═══════════════════════════════════════════════════════════════════════════════
"""

# ============================================================
# OUTILS
# ============================================================
def extract_json(text):
    i, j = text.find("{"), text.rfind("}")
    if i == -1 or j == -1:
        return None
    raw = text[i:j+1]
    try:
        return json.loads(raw)
    except Exception:
        raw = re.sub(r",\s*}", "}", raw)
        raw = re.sub(r",\s*]", "]", raw)
        try:
            return json.loads(raw)
        except:
            return None

def get_nested_value(data, keys):
    """Récupère une valeur imbriquée dans un dict via une liste de clés"""
    for k in keys:
        if '[' in k:  # Gestion listes (ex: "references_cadastrales[0].section")
            k_name, idx = k.split('[')
            idx = int(idx.rstrip(']'))
            if isinstance(data, dict) and k_name in data:
                data = data[k_name]
                if isinstance(data, list) and len(data) > idx:
                    data = data[idx]
                else:
                    return None
            else:
                return None
        else:
            data = data.get(k) if isinstance(data, dict) else None
        if data is None:
            return None
    return data

def set_nested_value(data, keys, value):
    """Définit une valeur imbriquée dans un dict via une liste de clés"""
    for i, k in enumerate(keys[:-1]):
        if '[' in k:
            k_name, idx = k.split('[')
            idx = int(idx.rstrip(']'))
            if k_name not in data:
                data[k_name] = []
            while len(data[k_name]) <= idx:
                data[k_name].append({})
            data = data[k_name][idx]
        else:
            if k not in data:
                data[k] = {}
            data = data[k]
    
    final_key = keys[-1]
    if '[' in final_key:
        k_name, idx = final_key.split('[')
        idx = int(idx.rstrip(']'))
        if k_name not in data:
            data[k_name] = []
        while len(data[k_name]) <= idx:
            data[k_name].append(None)
        data[k_name][idx] = value
    else:
        data[final_key] = value

def merge_extraction_results(base_data, new_data, missing_fields):
    """
    Fusionne en privilégiant les champs non-null de base_data,
    sauf pour les champs explicitement manquants à corriger
    """
    merged = json.loads(json.dumps(base_data))  # Deep copy
    
    for field in missing_fields:
        keys = field.split('.')
        new_value = get_nested_value(new_data, keys)
        if new_value is not None:
            set_nested_value(merged, keys, new_value)
            logger.info(f"  ↳ Champ complété: {field}")
    
    return merged

# ============================================================
# PROMPTS
# ============================================================
BASE_PROMPT = f"""Tu es un expert en lecture de formulaires CERFA et en extraction d'informations structurées.

Analyse le document PDF fourni (CERFA 13410*12) et renvoie **UNIQUEMENT** un JSON strict conforme au schéma ci-dessous.

⚠️ NE FOURNIS AUCUN TEXTE HORS DU JSON. NE COMMENTE RIEN. N'EXPLIQUE RIEN.

───────────────────────────────────────────────
SCHÉMA JSON STRICT À RESPECTER :
───────────────────────────────────────────────
{{
  "cerfa_reference": "13410*12",
  "commune_nom": null,
  "commune_insee": null,
  "departement_code": null,
  "numero_cu": null,
  "type_cu": null,
  "date_depot": null,
  "demandeur": {{
    "type": "particulier" ou "personne_morale",
    "nom": null,
    "prenom": null,
    "denomination": null,
    "representant_nom": null,
    "representant_prenom": null,
    "siret": null,
    "adresse": {{
      "numero": null,
      "voie": null,
      "lieu_dit": null,
      "code_postal": null,
      "ville": null,
      "email": null,
      "telephone": null
    }}
  }},
  "adresse_terrain": {{
    "numero": null,
    "voie": null,
    "lieu_dit": null,
    "code_postal": null,
    "ville": null
  }},
  "references_cadastrales": [{{"section": null, "numero": null, "surface_m2": null}}],
  "superficie_totale_m2": null,
  "header_cu": {{
    "dept": null,
    "commune_code": null,
    "annee": null,
    "numero_dossier": null
  }}
}}

───────────────────────────────────────────────
RÈGLES D'EXTRACTION :
───────────────────────────────────────────────
1. Si le cadre « Vous êtes un particulier » (2.1) est coché → type = "particulier"
   - Extraire : nom, prénom, adresse complète, email, téléphone.

2. Si le cadre « Vous êtes une personne morale » (2.2) est coché → type = "personne_morale"
   - Extraire : dénomination, SIRET, type (SARL/SCI...), nom et prénom du représentant légal.
   - Extraire également l'adresse, email, téléphone si présents.

3. L'adresse du demandeur vient de la section 3 du CERFA.
   L'adresse du terrain vient de la section 4.1 (page 2).

4. Extraire TOUTES les références cadastrales (OBLIGATOIRE) :
   - PAGE 2, section 4.2 : parcelle principale (peut avoir une superficie)
   - PAGE 4 : TOUJOURS vérifier cette page pour d'autres parcelles supplémentaires
     Format page 4 : "Prefixe XXX Section XXX Numero XXX Superficie de la parcelle cadastrale en m2 : XX"
   - EN BAS DE LA PAGE 4 : "Superficie totale du terrain (en m2) : XXXX m2"
     → Cette valeur est LA VRAIE superficie totale à utiliser dans `superficie_totale_m2`
   - Chaque parcelle doit avoir `section`, `numero`, `surface_m2`
   - Il faut ABSOLUMENT extraire TOUTES les parcelles (page 2 + page 4)
   - La `superficie_totale_m2` DOIT être celle indiquée en bas de la page 4

5. Construire le numéro complet du certificat :
   [dept]-[commune_code]-20[annee]-X[numero_dossier]

6. Toujours inclure toutes les clés, même vides (null).

{VISUAL_LOCATION_HINTS}

───────────────────────────────────────────────
NE PAS :
- inventer de données
- traduire les valeurs (garde les noms et adresses français)
- omettre des clés
───────────────────────────────────────────────
"""

# ============================================================
# VALIDATION
# ============================================================
EXPECTED_FIELDS = {
    "cerfa_reference", "commune_nom", "departement_code",
    "numero_cu", "type_cu", "date_depot",
    "demandeur", "adresse_terrain", "references_cadastrales",
    "superficie_totale_m2", "header_cu"
}

FIELD_TRANSLATIONS = {
    "cerfa_reference": "la référence CERFA",
    "commune_nom": "le nom de la commune (section 4.1 Localité)",
    "departement_code": "le code du département",
    "numero_cu": "le numéro du certificat d'urbanisme",
    "type_cu": "le type de certificat (CUa ou CUb)",
    "date_depot": "la date de dépôt",
    "demandeur": "les informations complètes du demandeur",
    "demandeur.type": "le type de demandeur (particulier ou personne_morale, section 2.1 ou 2.2)",
    "demandeur.nom": "le nom du demandeur ou du représentant (section 2)",
    "demandeur.adresse": "l'adresse complète du demandeur (section 3)",
    "demandeur.adresse.code_postal": "le code postal du demandeur (section 3)",
    "demandeur.adresse.ville": "la ville du demandeur (section 3)",
    "adresse_terrain": "l'adresse du terrain (section 4.1)",
    "references_cadastrales": "les parcelles cadastrales avec section, numéro et surface (section 4.2 + annexes)",
    "references_cadastrales[].section": "la section cadastrale",
    "references_cadastrales[].numero": "le numéro de parcelle",
    "superficie_totale_m2": "la superficie totale du terrain (section 4.2)",
    "header_cu": "l'en-tête du numéro CU (page 1, cadre supérieur droit)",
    "header_cu.dept": "le code département (3 chiffres, ex: 033)",
    "header_cu.commune_code": "le code commune (3 chiffres, ex: 234)",
    "header_cu.annee": "l'année (2 chiffres, ex: 25)",
    "header_cu.numero_dossier": "le numéro de dossier (5 chiffres, ex: 00078)"
}

def validate_cerfa_json(data):
    """
    Valide que le JSON contient tous les champs essentiels.
    Vérifie aussi les sous-structures (demandeur, adresse_terrain, références cadastrales).
    """
    missing = []
    
    # Validation des champs de premier niveau
    for f in EXPECTED_FIELDS:
        if f not in data or data[f] in (None, "", []):
            missing.append(f)
    
    # Validation spécifique du demandeur
    if "demandeur" in data and isinstance(data["demandeur"], dict):
        demandeur = data["demandeur"]
        # Type obligatoire
        if not demandeur.get("type"):
            missing.append("demandeur.type")
        # Nom obligatoire (particulier ou représentant)
        if not demandeur.get("nom"):
            missing.append("demandeur.nom")
        # Adresse obligatoire
        if not demandeur.get("adresse") or not isinstance(demandeur["adresse"], dict):
            missing.append("demandeur.adresse")
        elif demandeur.get("adresse"):
            # Vérifier les champs minimums de l'adresse
            adresse = demandeur["adresse"]
            if not adresse.get("code_postal"):
                missing.append("demandeur.adresse.code_postal")
            if not adresse.get("ville"):
                missing.append("demandeur.adresse.ville")
    
    # Validation des références cadastrales
    if "references_cadastrales" in data and isinstance(data["references_cadastrales"], list):
        if len(data["references_cadastrales"]) > 0:
            for idx, ref in enumerate(data["references_cadastrales"]):
                if not isinstance(ref, dict):
                    continue
                if not ref.get("section"):
                    missing.append(f"references_cadastrales[{idx}].section")
                if not ref.get("numero"):
                    missing.append(f"references_cadastrales[{idx}].numero")
    
    # Validation du header_cu
    if "header_cu" in data and isinstance(data["header_cu"], dict):
        header = data["header_cu"]
        required_header_fields = ["dept", "commune_code", "annee", "numero_dossier"]
        for field in required_header_fields:
            if not header.get(field):
                missing.append(f"header_cu.{field}")
    
    if missing:
        logger.warning(f"⚠️ Champs manquants ou vides : {missing}")
        return False, missing
    
    return True, []

def missing_fields_message(missing):
    """Génère un message décrivant les champs manquants"""
    parts = [FIELD_TRANSLATIONS.get(f, f) for f in missing]
    return "Certains champs essentiels sont absents : " + ", ".join(parts) + "."

def build_correction_prompt(previous_data, missing):
    """Construit un prompt de correction avec contexte des données déjà extraites"""
    # Extraire les données déjà validées (non manquantes)
    validated_data = {}
    for key, value in previous_data.items():
        # Garder seulement les champs qui ne sont pas dans missing
        if key not in [m.split('.')[0] for m in missing]:
            validated_data[key] = value
    
    correction_hint = f"""
───────────────────────────────────────────────
CONTEXTE : CORRECTION DE CHAMPS MANQUANTS
───────────────────────────────────────────────

DONNÉES DÉJÀ EXTRAITES (À CONSERVER TELLES QUELLES) :
{json.dumps(validated_data, indent=2, ensure_ascii=False)}

CHAMPS À COMPLÉTER UNIQUEMENT :
{missing_fields_message(missing)}

INSTRUCTIONS :
- Relis attentivement le document PDF en suivant le GUIDE DE LOCALISATION VISUELLE
- Complète UNIQUEMENT les champs manquants listés ci-dessus
- Renvoie le JSON COMPLET en incluant :
  1. Toutes les données déjà extraites ci-dessus (inchangées)
  2. Les champs manquants maintenant complétés
- Ne modifie PAS les données déjà validées
- Respecte strictement le schéma JSON
"""
    return correction_hint

# ============================================================
# MAIN PIPELINE
# ============================================================
def analyse_cerfa(pdf_path, out_json="cerfa_result.json", max_retries=2):
    """
    Analyse complète d'un CERFA avec extraction robuste
    
    Args:
        pdf_path: Chemin du PDF CERFA
        out_json: Fichier de sortie JSON
        max_retries: Nombre de tentatives maximum (0 = pas de retry, 2 = 3 essais au total)
    
    Returns:
        dict: Résultat complet avec succès, données, erreurs, métadonnées
    """
    mistral_key = os.getenv("MISTRAL_API_KEY")
    if not mistral_key:
        raise RuntimeError("❌ Clé MISTRAL_API_KEY manquante dans .env")
    
    client = Mistral(api_key=mistral_key)
    pdf = Path(pdf_path)
    logger.info(f"📄 Analyse du fichier {pdf.name}")
    
    # Extraction du texte du PDF pour les modèles non-OCR (réutilisé pour tous les appels)
    reader = PdfReader(pdf)
    pdf_text = "\n".join([page.extract_text() or "" for page in reader.pages])
    pdf_bytes_base64 = base64.b64encode(pdf.read_bytes()).decode('utf-8')  # Pour le modèle OCR
    
    # ============================================================
    # ÉTAPE 1 : PRÉ-EXTRACTION INSEE DÉDIÉE
    # ============================================================
    logger.info("="*60)
    logger.info("🎯 ÉTAPE 1/3 : PRÉ-EXTRACTION INSEE")
    logger.info("="*60)
    
    insee_result = extract_insee_robust(pdf_path)  # INSEE utilise la méthode OCR robuste
    
    # ============================================================
    # ÉTAPE 2 : EXTRACTION COMPLÈTE AVEC RETRY PROGRESSIF
    # ============================================================
    logger.info("="*60)
    logger.info("📋 ÉTAPE 2/3 : EXTRACTION COMPLÈTE DU CERFA")
    logger.info("="*60)
    
    model_used = MODEL_PRIMARY
    previous_data = None
    
    def _run_mistral(prompt, model, max_retries=3, retry_delay=5):
        """Exécute une requête Mistral avec retry automatique en cas d'erreur 429
        
        Pour mistral-ocr-latest : utilise l'API OCR robuste (upload + OCR + chat)
        Pour les autres modèles : envoie le texte extrait
        """
        last_error = None
        is_ocr_model = "ocr" in model.lower()
        
        for attempt in range(max_retries):
            try:
                if is_ocr_model:
                    # Modèle OCR : méthode robuste avec upload + OCR + chat
                    # Étape 1 : Upload du PDF
                    uploaded_pdf = client.files.upload(
                        file={"file_name": pdf.name, "content": open(pdf_path, "rb")},
                        purpose="ocr"
                    )
                    signed_url = client.files.get_signed_url(file_id=uploaded_pdf.id)
                    
                    # Étape 2 : Extraction OCR
                    ocr_response = client.ocr.process(
                        model=model,
                        document={"type": "document_url", "document_url": signed_url.url},
                        include_image_base64=False
                    )
                    
                    # Récupérer le texte extrait
                    ocr_text = "\n\n".join([page.markdown for page in ocr_response.pages])
                    
                    # Étape 3 : Utiliser un modèle de chat pour extraire les données depuis le texte OCR
                    # On utilise large-latest pour le chat (pas OCR)
                    chat_model = MODEL_FALLBACK  # mistral-large-latest
                    response = client.chat.complete(
                        model=chat_model,
                        messages=[
                            {"role": "system", "content": "Tu es un expert CERFA et ton rôle est de produire un JSON structuré strict."},
                            {"role": "user", "content": f"{prompt}\n\n---\nTEXTE DU DOCUMENT (extrait par OCR):\n{ocr_text}"}
                        ],
                        temperature=0.0
                    )
                else:
                    # Modèles standards : envoie le texte extrait
                    response = client.chat.complete(
                        model=model,
                        messages=[
                            {"role": "system", "content": "Tu es un expert CERFA et ton rôle est de produire un JSON structuré strict."},
                            {"role": "user", "content": f"{prompt}\n\n---\nTEXTE DU DOCUMENT:\n{pdf_text}"}
                        ],
                        temperature=0.0
                    )
                
                text = response.choices[0].message.content
                parsed = extract_json(text or "")
                if not parsed:
                    raise RuntimeError("Échec parsing JSON Mistral")
                return parsed
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # Détecter l'erreur 429 (Too Many Requests)
                is_rate_limit = (
                    "429" in error_str or 
                    "rate limit" in error_str.lower() or 
                    "service_tier_capacity_exceeded" in error_str or
                    "Too Many Requests" in error_str or
                    (hasattr(e, 'status_code') and e.status_code == 429) or
                    (hasattr(e, 'code') and str(e.code) == "3505")
                )
                
                if is_rate_limit and attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)  # Délai croissant : 5s, 10s, 15s
                    logger.warning(f"⚠️ Erreur 429 (rate limit) avec {model}, tentative {attempt + 1}/{max_retries}. Attente {wait_time}s avant retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    # Pour les autres erreurs ou dernière tentative, on lève l'exception
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Échec après {max_retries} tentatives avec {model}")
                    raise
        
        # Ne devrait jamais être atteint, mais au cas où
        raise RuntimeError(f"Échec après {max_retries} tentatives: {last_error}")
    
    # Boucle de retry progressive
    for attempt in range(max_retries + 1):
        logger.info(f"\n🔄 Tentative {attempt + 1}/{max_retries + 1}")
        
        try:
            if attempt == 0:
                # Premier essai avec prompt de base
                logger.info(f"🤖 Extraction avec {MODEL_PRIMARY}...")
                data = _run_mistral(BASE_PROMPT, MODEL_PRIMARY)
                model_used = MODEL_PRIMARY
            else:
                # Retry avec prompt enrichi et merge
                logger.info(f"🔧 Correction des champs manquants...")
                correction_prompt = BASE_PROMPT + "\n\n" + build_correction_prompt(previous_data, missing)
                
                # Essayer avec le modèle qui a marché précédemment
                try:
                    data = _run_mistral(correction_prompt, model_used)
                except Exception:
                    # Fallback si le modèle échoue
                    if model_used == MODEL_PRIMARY:
                        logger.info(f"⚠️ Fallback vers {MODEL_FALLBACK}...")
                        time.sleep(random.uniform(2, 4))
                        data = _run_mistral(correction_prompt, MODEL_FALLBACK)
                        model_used = MODEL_FALLBACK
                    else:
                        raise
                
                # Merge intelligent : garde les bonnes valeurs, complète les manquantes
                data = merge_extraction_results(previous_data, data, missing)
        
        except Exception as e:
            # Fallback progressif : OCR → Large → Small
            if attempt == 0 and model_used == MODEL_PRIMARY:
                logger.warning(f"⚠️ Échec {MODEL_PRIMARY}, fallback vers {MODEL_FALLBACK}...")
                time.sleep(random.uniform(2, 4))
                try:
                    data = _run_mistral(BASE_PROMPT, MODEL_FALLBACK)
                    model_used = MODEL_FALLBACK
                except Exception as e2:
                    logger.warning(f"⚠️ Échec {MODEL_FALLBACK}, fallback vers {MODEL_FALLBACK2}...")
                    time.sleep(random.uniform(2, 4))
                    try:
                        data = _run_mistral(BASE_PROMPT, MODEL_FALLBACK2)
                        model_used = MODEL_FALLBACK2
                    except Exception as e3:
                        logger.error(f"❌ Échec total (OCR, Large et Small) : {e3}")
                        return {
                            "success": False,
                            "data": None,
                            "errors": ["extraction_failed"],
                            "model_used": None,
                            "insee_extraction": insee_result,
                            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                        }
            else:
                logger.error(f"❌ Échec extraction tentative {attempt + 1}: {e}")
                if attempt == max_retries:
                    return {
                        "success": False,
                        "data": previous_data,
                        "errors": missing if previous_data else ["extraction_failed"],
                        "model_used": model_used,
                        "insee_extraction": insee_result,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                continue
        
        # Validation
        ok, missing = validate_cerfa_json(data)
        
        if ok:
            logger.info(f"✅ Extraction complète réussie !")
            break
        
        # Acceptation partielle si < 3 champs manquants au dernier essai
        if len(missing) < 3 and attempt == max_retries:
            logger.warning(f"⚠️ Acceptation partielle : {len(missing)} champ(s) manquant(s)")
            break
        
        # Sauvegarder pour le prochain retry
        previous_data = data
        
        if attempt < max_retries:
            logger.warning(f"⚠️ {len(missing)} champ(s) manquant(s), nouvelle tentative...")
            time.sleep(random.uniform(2, 5))
    
    # ============================================================
    # ÉTAPE 3 : ENRICHISSEMENT ET NORMALISATION
    # ============================================================
    logger.info("="*60)
    logger.info("🔧 ÉTAPE 3/3 : ENRICHISSEMENT DES DONNÉES")
    logger.info("="*60)
    
    # Injection de l'INSEE pré-extrait (priorité haute)
    if insee_result.get('insee'):
        data['commune_insee'] = insee_result['insee']
        if insee_result.get('commune_nom_officiel'):
            data['commune_nom'] = insee_result['commune_nom_officiel']
        data['_insee_confidence'] = insee_result['confidence']
        data['_insee_method'] = insee_result['method']
        logger.info(f"✅ INSEE injecté: {insee_result['insee']} (confiance: {insee_result['confidence']})")
    
    # Extraction de la superficie totale depuis la page 4 (via OCR dédié)
    logger.info("📐 Extraction superficie totale depuis page 4...")
    superficie_result = extract_superficie_totale(pdf_path)
    if superficie_result.get('superficie_totale_m2'):
        data['superficie_totale_m2'] = superficie_result['superficie_totale_m2']
        data['_superficie_method'] = superficie_result.get('methode', 'inconnue')
        data['_superficie_details'] = superficie_result.get('details', '')
        logger.info(f"✅ Superficie totale injectée: {superficie_result['superficie_totale_m2']} m² (méthode: {superficie_result.get('methode', 'inconnue')})")
    else:
        logger.warning(f"⚠️ Superficie totale non extraite, conservation de la valeur du modèle si présente")
    
    # Métadonnées
    data["source_file"] = pdf.name
    
    # Normalisation du numéro CU
    num = data.get("numero_cu", "")
    if re.match(r"^CU\d{8}X\d+$", num):
        data["numero_cu"] = f"{num[2:4]}-{num[4:7]}-20{num[7:9]}-{num[9:]}"
    
    # Normalisation type_cu
    if data.get("type_cu", "").lower().startswith("info"):
        data["type_cu"] = "CUa"
    
    # Résultat final
    final = {
        "success": ok,
        "data": data,
        "errors": missing,
        "model_used": model_used,
        "insee_extraction": insee_result,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Sauvegarde
    Path(out_json).write_text(json.dumps(final, indent=2, ensure_ascii=False), encoding="utf-8")
    
    logger.info("="*60)
    if ok:
        logger.info(f"✅ SUCCÈS : JSON complet sauvegardé → {out_json}")
    else:
        logger.warning(f"⚠️ PARTIEL : JSON sauvegardé avec {len(missing)} champ(s) manquant(s) → {out_json}")
    logger.info("="*60)
    
    return final

# ============================================================
# CLI (compatible orchestrator)
# ============================================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Analyse CERFA Mistral (OCR + Fallback Large/Small) avec extraction INSEE robuste")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA à analyser")
    ap.add_argument("--out-json", default="cerfa_result.json", help="Chemin de sortie JSON")
    ap.add_argument("--out-dir", default=".", help="Dossier de sortie (compatibilité orchestrator)")
    ap.add_argument("--max-retries", type=int, default=2, help="Nombre de retries maximum (défaut: 2)")

    args = ap.parse_args()

    # Appel unique
    analyse_cerfa(args.pdf, args.out_json, max_retries=args.max_retries)