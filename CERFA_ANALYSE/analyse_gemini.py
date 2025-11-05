#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_gemini.py — Analyse d’un CERFA CU (13410*11)
Gemini 2.5 Flash → JSON structuré conforme au CUA Builder
avec validation + relance intelligente en cas de champs manquants.
"""

import os, json, re, time, random, logging
from pathlib import Path
from pypdf import PdfReader
import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("cerfa_analyse")

MODEL_PRIMARY = "gemini-2.5-pro"
MODEL_FALLBACK = "gemini-2.5-flash"
INSEE_CSV = os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv")

# ============================================================
# OUTILS
# ============================================================
def normalize_name(s):
    return re.sub(r"\s+", " ", s.strip().lower()) if s else ""

def get_insee(commune, dep):
    try:
        df = pd.read_csv(INSEE_CSV, dtype=str)
        df["LIBELLE_n"] = df["LIBELLE"].map(normalize_name)
        df["DEP"] = df["DEP"].str.zfill(2)
        row = df[(df["LIBELLE_n"] == normalize_name(commune)) &
                 (df["DEP"] == str(dep).zfill(2))]
        return str(row.iloc[0]["COM"]) if len(row) == 1 else None
    except Exception as e:
        logger.warning(f"INSEE lookup failed: {e}")
        return None

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

# ============================================================
# PROMPTS
# ============================================================
BASE_PROMPT = """Tu es un expert en lecture de formulaires CERFA.
Analyse le PDF fourni et renvoie UNIQUEMENT un JSON strict selon ce schéma :

{
  "cerfa_reference": null,
  "commune_nom": null,
  "commune_insee": null,
  "departement_code": null,
  "numero_cu": null,
  "type_cu": null,
  "date_depot": null,
  "demandeur": {"type": null, "nom": null, "prenom": null},
  "coord_demandeur": {},
  "mandataire": {},
  "adresse_terrain": {},
  "references_cadastrales": [{"section": null, "numero": null}],
  "superficie_totale_m2": null,
  "header_cu": {"dept": null, "commune_code": null, "annee": null, "numero_dossier": null}
}

Contraintes :
- Ne renvoie que du JSON, sans texte ou explication.
- Toutes les clés doivent être présentes, même si certaines sont nulles.
- `commune_insee` reste null (il sera ajouté ensuite).
- Ne pas inventer de valeurs absentes du document.
"""

# ============================================================
# VALIDATION
# ============================================================
EXPECTED_FIELDS = {
    "cerfa_reference", "commune_nom", "departement_code",
    "numero_cu", "type_cu", "date_depot",
    "demandeur", "references_cadastrales",
    "header_cu"
}

FIELD_TRANSLATIONS = {
    "cerfa_reference": "la référence CERFA",
    "commune_nom": "le nom de la commune",
    "departement_code": "le code du département",
    "numero_cu": "le numéro du certificat d’urbanisme",
    "type_cu": "le type de certificat (CUa ou CUb)",
    "date_depot": "la date de dépôt",
    "demandeur": "les informations du demandeur",
    "references_cadastrales": "les parcelles cadastrales",
    "header_cu": "l’en-tête du numéro CU"
}

def validate_cerfa_json(data):
    missing = [f for f in EXPECTED_FIELDS if f not in data or data[f] in (None, "", [])]
    if missing:
        logger.warning(f"⚠️ Champs manquants ou vides : {missing}")
        return False, missing
    return True, []

def missing_fields_message(missing):
    parts = [FIELD_TRANSLATIONS.get(f, f) for f in missing]
    return "Certains champs essentiels sont absents : " + ", ".join(parts) + ". " \
           "Relis attentivement le document et complète uniquement ces champs manquants dans le JSON final."

# ============================================================
# MAIN PIPELINE
# ============================================================
def analyse_cerfa(pdf_path, out_json="cerfa_result.json", retry_if_incomplete=True):
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    pdf = Path(pdf_path)
    logger.info(f"Analyse du fichier {pdf.name}")
    
    model_used = MODEL_PRIMARY

    def _run_gemini(prompt, model):
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

    # Premier essai avec Pro
    logger.info(f"🤖 Analyse avec {MODEL_PRIMARY}...")
    try:
        data = _run_gemini(BASE_PROMPT, MODEL_PRIMARY)
        ok, missing = validate_cerfa_json(data)
    except Exception as e:
        # Fallback vers Flash en cas d'échec Pro
        logger.info(f"🔄 Fallback vers {MODEL_FALLBACK} suite à l'échec de Pro...")
        time.sleep(random.uniform(2, 4))
        try:
            data = _run_gemini(BASE_PROMPT, MODEL_FALLBACK)
            model_used = MODEL_FALLBACK
            ok, missing = validate_cerfa_json(data)
        except Exception as e2:
            logger.error(f"❌ Échec total (Pro et Flash) : {e2}")
            raise RuntimeError(f"Impossible d'analyser le PDF avec Pro ni Flash : {e2}")

    # Relance intelligente si champs manquants
    if not ok and retry_if_incomplete:
        correction_hint = missing_fields_message(missing)
        enhanced_prompt = BASE_PROMPT + "\n\n" + correction_hint + \
            "\nNe réécris pas tout le JSON, mais renvoie-le complet et corrigé selon le même format strict."
        logger.info(f"🔄 Relance pour compléter les champs manquants...")
        time.sleep(random.uniform(3, 6))
        try:
            # Essayer d'abord avec le modèle qui a fonctionné
            data = _run_gemini(enhanced_prompt, model_used)
            ok, missing = validate_cerfa_json(data)
        except Exception:
            # Si échec, tenter avec Flash en fallback
            if model_used == MODEL_PRIMARY:
                logger.info(f"🔄 Fallback vers {MODEL_FALLBACK} pour la relance...")
                time.sleep(random.uniform(2, 4))
                try:
                    data = _run_gemini(enhanced_prompt, MODEL_FALLBACK)
                    model_used = MODEL_FALLBACK
                    ok, missing = validate_cerfa_json(data)
                except Exception as e:
                    logger.warning(f"⚠️ Relance échouée même avec Flash : {e}")
            else:
                logger.warning("⚠️ Relance échouée")

    # Normalisation
    data["source_file"] = pdf.name
    if data.get("commune_nom") and data.get("departement_code"):
        insee = get_insee(data["commune_nom"], data["departement_code"])
        if insee:
            data["commune_insee"] = insee

    num = data.get("numero_cu", "")
    if re.match(r"^CU\d{8}X\d+$", num):
        data["numero_cu"] = f"{num[2:4]}-{num[4:7]}-20{num[7:9]}-{num[9:]}"
    if data.get("type_cu", "").lower().startswith("info"):
        data["type_cu"] = "CUa"

    final = {
        "success": ok,
        "data": data,
        "errors": missing,
        "model_used": model_used,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    Path(out_json).write_text(json.dumps(final, indent=2, ensure_ascii=False), encoding="utf-8")

    if ok:
        logger.info(f"✅ JSON complet sauvegardé avec {model_used} : {out_json}")
    else:
        logger.warning(f"⚠️ JSON partiel sauvegardé avec {model_used} ({len(missing)} champs manquants) : {out_json}")

    return final

# ============================================================
# CLI (compatible orchestrator)
# ============================================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Analyse CERFA Gemini (Pro + Fallback Flash)")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA à analyser")
    ap.add_argument("--out-json", default="cerfa_result.json", help="Chemin de sortie JSON")
    ap.add_argument("--out-dir", default=".", help="Dossier de sortie (non utilisé pour l'instant, compatibilité orchestrator)")
    ap.add_argument("--insee-csv", default=os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv"),
                    help="Chemin vers le CSV INSEE des communes")

    args = ap.parse_args()

    # Appel unique — seul --out-json est utile ici
    analyse_cerfa(args.pdf, args.out_json)
