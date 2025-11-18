#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_gemini.py — Analyse d'un CERFA CU (13410*12)
Gemini 2.5 Flash → JSON structuré conforme au CUA Builder
avec pré-extraction INSEE robuste, validation + relance intelligente
"""

import os, json, re, time, random, logging
from pathlib import Path
from pypdf import PdfReader
import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv

# Import de la pré-analyse complète
from CERFA_ANALYSE.pre_analyse_cerfa import pre_analyse_cerfa

# ============================================================
# CONFIG
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("cerfa_analyse")

MODEL_PRIMARY = "gemini-2.5-pro"
MODEL_FALLBACK = "gemini-2.5-flash"
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

📌 RÉFÉRENCES CADASTRALES (PAGE 4, section 4.2)
┌─────────────────────────────────────────────────────────┐
│ 4.2 Références cadastrales :                           │
│                                                         │
│ Section : [AI]  Numéro : [0310]  Superficie : 5755 m² │
│ Section : [AI]  Numéro : [0058]  Superficie : 256 m²  │
│ Section : [AI]  Numéro : [0311]  Superficie : 1368 m² │
│                                                         │
│ Superficie totale du terrain (en m²) : 12310          │
└─────────────────────────────────────────────────────────┘

Format parcelles :
• Section : 1-2 LETTRES MAJUSCULES (ex: AI, AC, ZA)
• Numéro : 4 CHIFFRES avec zéros initiaux (ex: 0310, 0058)
• Superficie : nombre entier en m²

⚠️ Si > 1 parcelles→ CONTINUER SUR PAGE 4

📌 NUMÉRO CU COMPLET (à reconstruire)
Format final attendu : [Dept]-[Commune]-20[Année]-X[Dossier]
Exemple : 033-234-2025-X00078

═══════════════════════════════════════════════════════════════════════════════
⚠️ RÈGLES CRITIQUES
═══════════════════════════════════════════════════════════════════════════════
1. Le header_cu se trouve TOUJOURS page 1, cadre supérieur droit
2. La commune_nom vient de section 4.1 "Localité" (PAS section 3)
3. TOUJOURS vérifier la page annexe pour parcelles supplémentaires
4. La superficie totale DOIT être >= somme des surfaces individuelles
5. Ne JAMAIS inventer de valeurs absentes du document
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

4. Extraire toutes les références cadastrales (section 4.2 et annexes).
   - Chaque objet doit avoir `section`, `numero`, `surface_m2`.
   - Calculer la `superficie_totale_m2` si possible.

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
# AFFICHAGE ET CONFIRMATION PRÉ-ANALYSE
# ============================================================
def display_pre_analyse_results(pre_analyse_result):
    """
    Affiche les résultats de la pré-analyse de manière lisible
    """
    print("\n" + "="*70)
    print("📊 RÉSULTATS DE LA PRÉ-ANALYSE")
    print("="*70)
    
    # INSEE
    insee = pre_analyse_result.get('insee', {})
    print("\n📍 CODE INSEE DE LA COMMUNE")
    print("-" * 70)
    if insee.get('code'):
        print(f"  Code INSEE : {insee['code']}")
        print(f"  Confiance  : {insee.get('confidence', 'unknown')}")
        print(f"  Méthode    : {insee.get('method', 'unknown')}")
        if insee.get('commune_nom_officiel'):
            print(f"  Commune    : {insee['commune_nom_officiel']}")
    else:
        print("  ❌ Code INSEE non trouvé")
    
    # Parcelles
    parcelles = pre_analyse_result.get('parcelles', [])
    print(f"\n📋 PARCELLES CADASTRALES ({len(parcelles)} trouvée(s))")
    print("-" * 70)
    if parcelles:
        for idx, parcelle in enumerate(parcelles, 1):
            section = parcelle.get('section', 'N/A')
            numero = parcelle.get('numero', 'N/A')
            print(f"  {idx}. Section: {section:4s} | Numéro: {numero}")
    else:
        print("  ❌ Aucune parcelle trouvée")
    
    # Superficie
    superficie = pre_analyse_result.get('superficie_totale_m2')
    print(f"\n📏 SUPERFICIE TOTALE DU TERRAIN")
    print("-" * 70)
    if superficie:
        print(f"  Superficie : {superficie:,} m²")
    else:
        print("  ❌ Superficie non trouvée")
    
    print("\n" + "="*70)
    
    return True

def ask_user_confirmation():
    """
    Demande confirmation à l'utilisateur avant de continuer
    """
    while True:
        response = input("\n❓ Voulez-vous continuer avec l'analyse complète du CERFA ? (o/n) : ").strip().lower()
        if response in ['o', 'oui', 'y', 'yes']:
            return True
        elif response in ['n', 'non', 'no']:
            return False
        else:
            print("⚠️  Réponse invalide. Veuillez répondre 'o' (oui) ou 'n' (non).")

# ============================================================
# MAIN PIPELINE
# ============================================================
def analyse_cerfa(pdf_path, out_json="cerfa_result.json", max_retries=1, interactive=True):
    """
    Analyse complète d'un CERFA avec extraction robuste
    
    Args:
        pdf_path: Chemin du PDF CERFA
        out_json: Fichier de sortie JSON
        max_retries: Nombre de tentatives maximum (0 = pas de retry, 2 = 3 essais au total)
        interactive: Si True, affiche les résultats de pré-analyse et demande confirmation
    
    Returns:
        dict: Résultat complet avec succès, données, erreurs, métadonnées, pré-analyse
    """
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    pdf = Path(pdf_path)
    logger.info(f"📄 Analyse du fichier {pdf.name}")
    
    # ============================================================
    # ÉTAPE 1 : PRÉ-ANALYSE COMPLÈTE (INSEE + PARCELLES + SUPERFICIE)
    # ============================================================
    logger.info("="*60)
    logger.info("🎯 ÉTAPE 1/4 : PRÉ-ANALYSE COMPLÈTE")
    logger.info("="*60)
    logger.info("📋 Extraction simultanée : INSEE + Parcelles + Superficie")
    logger.info("   (Analyse des 4 premières pages uniquement)")
    
    pre_analyse_result = pre_analyse_cerfa(pdf_path, MODEL_PRIMARY, MODEL_FALLBACK)
    
    # Affichage des résultats et demande de confirmation
    if interactive:
        display_pre_analyse_results(pre_analyse_result)
        if not ask_user_confirmation():
            logger.info("❌ Analyse annulée par l'utilisateur")
            return {
                "success": False,
                "data": None,
                "errors": ["user_cancelled"],
                "model_used": None,
                "pre_analyse": pre_analyse_result,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
        logger.info("✅ Confirmation reçue, poursuite de l'analyse...")
    else:
        logger.info("📊 Résultats pré-analyse (mode non-interactif):")
        logger.info(f"   INSEE: {pre_analyse_result.get('insee', {}).get('code', 'N/A')}")
        logger.info(f"   Parcelles: {len(pre_analyse_result.get('parcelles', []))}")
        logger.info(f"   Superficie: {pre_analyse_result.get('superficie_totale_m2', 'N/A')} m²")
    
    # Préparer les données INSEE pour l'injection (compatibilité avec l'ancien format)
    insee_result = {
        'insee': pre_analyse_result.get('insee', {}).get('code'),
        'confidence': pre_analyse_result.get('insee', {}).get('confidence', 'unknown'),
        'method': pre_analyse_result.get('insee', {}).get('method', 'unknown'),
        'commune_nom_officiel': pre_analyse_result.get('insee', {}).get('commune_nom_officiel')
    }
    
    # ============================================================
    # ÉTAPE 2 : EXTRACTION COMPLÈTE AVEC RETRY PROGRESSIF
    # ============================================================
    logger.info("="*60)
    logger.info("📋 ÉTAPE 2/4 : EXTRACTION COMPLÈTE DU CERFA")
    logger.info("="*60)
    
    # Enrichir le prompt avec les données de pré-analyse
    pre_analyse_context = ""
    if pre_analyse_result.get('insee', {}).get('code'):
        pre_analyse_context += f"\n📌 CONTEXTE DE PRÉ-ANALYSE (à utiliser comme référence) :\n"
        pre_analyse_context += f"- Code INSEE détecté : {pre_analyse_result['insee']['code']}\n"
        if pre_analyse_result.get('insee', {}).get('commune_nom_officiel'):
            pre_analyse_context += f"- Commune : {pre_analyse_result['insee']['commune_nom_officiel']}\n"
        if pre_analyse_result.get('parcelles'):
            pre_analyse_context += f"- Parcelles détectées : {len(pre_analyse_result['parcelles'])} parcelle(s)\n"
            for p in pre_analyse_result['parcelles'][:3]:  # Afficher les 3 premières
                pre_analyse_context += f"  • Section {p.get('section', 'N/A')} - Numéro {p.get('numero', 'N/A')}\n"
        if pre_analyse_result.get('superficie_totale_m2'):
            pre_analyse_context += f"- Superficie totale détectée : {pre_analyse_result['superficie_totale_m2']} m²\n"
        pre_analyse_context += "\n⚠️ Utilise ces informations comme référence, mais vérifie-les dans le document complet.\n"
    
    enriched_base_prompt = BASE_PROMPT + pre_analyse_context
    
    model_used = MODEL_PRIMARY
    previous_data = None
    
    def _run_gemini(prompt, model):
        """Exécute une requête Gemini et parse le JSON"""
        try:
            model_instance = genai.GenerativeModel(model)
            response = model_instance.generate_content(
                [
                    {"mime_type": "application/pdf", "data": pdf.read_bytes()},
                    prompt
                ]
            )
            parsed = extract_json(response.text or "")
            if not parsed:
                raise RuntimeError("Échec parsing JSON Gemini")
            return parsed
        except Exception as e:
            logger.warning(f"⚠️ Erreur avec {model}: {e}")
            raise
    
    # Boucle de retry progressive
    for attempt in range(max_retries + 1):
        logger.info(f"\n🔄 Tentative {attempt + 1}/{max_retries + 1}")
        
        try:
            if attempt == 0:
                # Premier essai avec prompt enrichi (incluant pré-analyse)
                logger.info(f"🤖 Extraction avec {MODEL_PRIMARY}...")
                data = _run_gemini(enriched_base_prompt, MODEL_PRIMARY)
                model_used = MODEL_PRIMARY
            else:
                # Retry avec prompt enrichi et merge
                logger.info(f"🔧 Correction des champs manquants...")
                correction_prompt = enriched_base_prompt + "\n\n" + build_correction_prompt(previous_data, missing)
                
                # Essayer avec le modèle qui a marché précédemment
                try:
                    data = _run_gemini(correction_prompt, model_used)
                except Exception:
                    # Fallback si le modèle échoue
                    if model_used == MODEL_PRIMARY:
                        logger.info(f"⚠️ Fallback vers {MODEL_FALLBACK}...")
                        time.sleep(random.uniform(2, 4))
                        data = _run_gemini(correction_prompt, MODEL_FALLBACK)
                        model_used = MODEL_FALLBACK
                    else:
                        raise
                
                # Merge intelligent : garde les bonnes valeurs, complète les manquantes
                data = merge_extraction_results(previous_data, data, missing)
        
        except Exception as e:
            # Fallback vers Flash si Pro échoue au premier essai
            if attempt == 0 and model_used == MODEL_PRIMARY:
                logger.warning(f"⚠️ Échec {MODEL_PRIMARY}, fallback vers {MODEL_FALLBACK}...")
                time.sleep(random.uniform(2, 4))
                try:
                    data = _run_gemini(enriched_base_prompt, MODEL_FALLBACK)
                    model_used = MODEL_FALLBACK
                except Exception as e2:
                    logger.error(f"❌ Échec total (Pro et Flash) : {e2}")
                    return {
                        "success": False,
                        "data": None,
                        "errors": ["extraction_failed"],
                        "model_used": None,
                        "insee_extraction": insee_result,
                        "pre_analyse": pre_analyse_result,
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
                        "pre_analyse": pre_analyse_result,
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
    logger.info("🔧 ÉTAPE 3/4 : ENRICHISSEMENT DES DONNÉES")
    logger.info("="*60)
    
    # Injection des données de pré-analyse (priorité haute)
    # INSEE
    if insee_result.get('insee'):
        data['commune_insee'] = insee_result['insee']
        if insee_result.get('commune_nom_officiel'):
            data['commune_nom'] = insee_result['commune_nom_officiel']
        data['_insee_confidence'] = insee_result['confidence']
        data['_insee_method'] = insee_result['method']
        logger.info(f"✅ INSEE injecté: {insee_result['insee']} (confiance: {insee_result['confidence']})")
    
    # Parcelles (si non trouvées ou incomplètes dans l'extraction complète)
    pre_parcelles = pre_analyse_result.get('parcelles', [])
    if pre_parcelles:
        extracted_parcelles = data.get('references_cadastrales', [])
        if not extracted_parcelles or len(extracted_parcelles) == 0:
            # Convertir le format de pré-analyse vers le format attendu
            data['references_cadastrales'] = [
                {
                    'section': p.get('section'),
                    'numero': p.get('numero'),
                    'surface_m2': None  # Pas de surface dans la pré-analyse
                }
                for p in pre_parcelles
            ]
            logger.info(f"✅ Parcelles injectées depuis pré-analyse: {len(pre_parcelles)} parcelle(s)")
        elif len(pre_parcelles) > len(extracted_parcelles):
            logger.info(f"⚠️ Pré-analyse a trouvé plus de parcelles ({len(pre_parcelles)}) que l'extraction complète ({len(extracted_parcelles)})")
    
    # Superficie (si non trouvée dans l'extraction complète)
    pre_superficie = pre_analyse_result.get('superficie_totale_m2')
    if pre_superficie and not data.get('superficie_totale_m2'):
        data['superficie_totale_m2'] = pre_superficie
        logger.info(f"✅ Superficie injectée depuis pré-analyse: {pre_superficie} m²")
    
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
        "pre_analyse": pre_analyse_result,
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

    ap = argparse.ArgumentParser(description="Analyse CERFA Gemini (Pro + Fallback Flash) avec pré-analyse et extraction robuste")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA à analyser")
    ap.add_argument("--out-json", default="cerfa_result.json", help="Chemin de sortie JSON")
    ap.add_argument("--out-dir", default=".", help="Dossier de sortie (compatibilité orchestrator)")
    ap.add_argument("--max-retries", type=int, default=2, help="Nombre de retries maximum (défaut: 2)")
    ap.add_argument("--non-interactive", action="store_true", help="Mode non-interactif (pas de confirmation)")

    args = ap.parse_args()

    # Appel unique
    analyse_cerfa(args.pdf, args.out_json, max_retries=args.max_retries, interactive=not args.non_interactive)