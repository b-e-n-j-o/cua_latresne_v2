#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analyse_mistral.py — Analyse d'un CERFA CU (13410*12)
-----------------------------------------------------
Remplace l’analyse Gemini par Mistral (via API officielle mistralai)
1️⃣ Lecture du PDF
2️⃣ Extraction texte
3️⃣ Analyse avec Mistral (large-latest + fallback small)
4️⃣ Validation & relance intelligente
5️⃣ Sauvegarde JSON structuré conforme au CUA Builder
-----------------------------------------------------
"""

import os, json, re, time, random, logging
from pathlib import Path
from pypdf import PdfReader
import pandas as pd
from mistralai import Mistral
from dotenv import load_dotenv

# ============================================================
# CONFIGURATION GÉNÉRALE
# ============================================================
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("cerfa_analyse")

MODEL_PRIMARY = "mistral-large-latest"
MODEL_FALLBACK = "mistral-small-latest"
INSEE_CSV = os.path.join(os.path.dirname(__file__), "..", "CONFIG", "v_commune_2025.csv")

# ============================================================
# OUTILS GÉNÉRAUX
# ============================================================
def normalize_name(s):
    return re.sub(r"\s+", " ", s.strip().lower()) if s else ""

def get_insee(commune, dep):
    """Recherche le code INSEE à partir du nom de commune et du code département."""
    try:
        df = pd.read_csv(INSEE_CSV, dtype=str)
        df["LIBELLE_n"] = df["LIBELLE"].map(normalize_name)
        df["DEP"] = df["DEP"].str.zfill(2)
        row = df[(df["LIBELLE_n"] == normalize_name(commune)) & (df["DEP"] == str(dep).zfill(2))]
        return str(row.iloc[0]["COM"]) if len(row) == 1 else None
    except Exception as e:
        logger.warning(f"INSEE lookup failed: {e}")
        return None

def extract_json(text):
    """Extrait un JSON valide depuis le texte brut renvoyé par Mistral."""
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
# PROMPT DE BASE
# ============================================================
BASE_PROMPT = """Tu es un expert en lecture de formulaires CERFA et en extraction d'informations structurées.

Analyse le document PDF fourni (CERFA 13410*12) et renvoie **UNIQUEMENT** un JSON strict conforme au schéma ci-dessous.

⚠️ NE FOURNIS AUCUN TEXTE HORS DU JSON. NE COMMENTE RIEN. N'EXPLIQUE RIEN.

───────────────────────────────────────────────
SCHÉMA JSON STRICT À RESPECTER :
───────────────────────────────────────────────
{
  "cerfa_reference": "13410*12",
  "commune_nom": null,
  "commune_insee": null,
  "departement_code": null,
  "numero_cu": null,
  "type_cu": null,
  "date_depot": null,
  "demandeur": {
    "type": "particulier" ou "personne_morale",
    "nom": null,
    "prenom": null,
    "denomination": null,
    "representant_nom": null,
    "representant_prenom": null,
    "siret": null,
    "adresse": {
      "numero": null,
      "voie": null,
      "lieu_dit": null,
      "code_postal": null,
      "ville": null,
      "email": null,
      "telephone": null
    }
  },
  "adresse_terrain": {
    "numero": null,
    "voie": null,
    "lieu_dit": null,
    "code_postal": null,
    "ville": null
  },
  "references_cadastrales": [{"section": null, "numero": null, "surface_m2": null}],
  "superficie_totale_m2": null,
  "header_cu": {
    "dept": null,
    "commune_code": null,
    "annee": null,
    "numero_dossier": null
  }
}

───────────────────────────────────────────────
RÈGLES :
───────────────────────────────────────────────
1. Extrais uniquement ce qui est visible dans le document.
2. Toujours inclure toutes les clés, même vides (null).
3. Ne jamais inventer de données manquantes.
4. Respecte le format JSON strict ci-dessus.
───────────────────────────────────────────────
"""

# ============================================================
# VALIDATION DU JSON
# ============================================================
EXPECTED_FIELDS = {
    "cerfa_reference", "commune_nom", "departement_code",
    "numero_cu", "type_cu", "date_depot",
    "demandeur", "adresse_terrain", "references_cadastrales",
    "superficie_totale_m2", "header_cu"
}

def validate_cerfa_json(data):
    missing = []
    for f in EXPECTED_FIELDS:
        if f not in data or data[f] in (None, "", []):
            missing.append(f)
    if "demandeur" in data and isinstance(data["demandeur"], dict):
        d = data["demandeur"]
        if not d.get("type"): missing.append("demandeur.type")
        if not d.get("nom"): missing.append("demandeur.nom")
        if not d.get("adresse") or not isinstance(d["adresse"], dict):
            missing.append("demandeur.adresse")
        else:
            a = d["adresse"]
            if not a.get("code_postal"): missing.append("demandeur.adresse.code_postal")
            if not a.get("ville"): missing.append("demandeur.adresse.ville")
    if "references_cadastrales" in data and isinstance(data["references_cadastrales"], list):
        for idx, ref in enumerate(data["references_cadastrales"]):
            if not isinstance(ref, dict): continue
            if not ref.get("section"): missing.append(f"references_cadastrales[{idx}].section")
            if not ref.get("numero"): missing.append(f"references_cadastrales[{idx}].numero")
    if "header_cu" in data and isinstance(data["header_cu"], dict):
        for f in ["dept", "commune_code", "annee", "numero_dossier"]:
            if not data["header_cu"].get(f):
                missing.append(f"header_cu.{f}")
    return (len(missing) == 0, missing)

def missing_fields_message(missing):
    return "Certains champs essentiels sont absents : " + ", ".join(missing) + \
        ". Relis attentivement le document et complète uniquement ces champs manquants dans le JSON final."

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================
def analyse_cerfa(pdf_path, out_json="cerfa_result.json", retry_if_incomplete=True):
    mistral_key = os.getenv("MISTRAL_API_KEY")
    if not mistral_key:
        raise RuntimeError("❌ Clé MISTRAL_API_KEY manquante dans .env")

    client = Mistral(api_key=mistral_key)
    pdf = Path(pdf_path)
    logger.info(f"📄 Analyse du fichier : {pdf.name}")

    # Lecture du texte du PDF
    reader = PdfReader(pdf)
    pdf_text = "\n".join([page.extract_text() or "" for page in reader.pages])

    def _run_mistral(prompt, model):
        try:
            response = client.chat.complete(
                model=model,
                messages=[
                    {"role": "system", "content": "Tu es un expert CERFA et ton rôle est de produire un JSON structuré strict."},
                    {"role": "user", "content": f"{prompt}\n\n---\nTEXTE DU DOCUMENT:\n{pdf_text}"}
                ],
                temperature=0.0,
                max_output_tokens=4000
            )
            text = response.choices[0].message.content
            parsed = extract_json(text or "")
            if not parsed:
                raise RuntimeError("Échec parsing JSON Mistral")
            return parsed
        except Exception as e:
            logger.warning(f"⚠️ Erreur Mistral ({model}) : {e}")
            raise

    # Premier essai
    logger.info(f"🤖 Analyse avec {MODEL_PRIMARY}...")
    try:
        data = _run_mistral(BASE_PROMPT, MODEL_PRIMARY)
        ok, missing = validate_cerfa_json(data)
        model_used = MODEL_PRIMARY
    except Exception as e:
        logger.warning(f"🔄 Fallback vers {MODEL_FALLBACK} : {e}")
        data = _run_mistral(BASE_PROMPT, MODEL_FALLBACK)
        model_used = MODEL_FALLBACK
        ok, missing = validate_cerfa_json(data)

    # Relance intelligente si nécessaire
    if not ok and retry_if_incomplete:
        correction_hint = missing_fields_message(missing)
        enhanced_prompt = BASE_PROMPT + "\n\n" + correction_hint
        logger.info("🔁 Relance pour compléter les champs manquants...")
        data = _run_mistral(enhanced_prompt, model_used)
        ok, missing = validate_cerfa_json(data)

    # Post-traitement
    data["source_file"] = pdf.name
    if data.get("commune_nom") and data.get("departement_code"):
        insee = get_insee(data["commune_nom"], data["departement_code"])
        if insee:
            data["commune_insee"] = insee

    final = {
        "success": ok,
        "data": data,
        "errors": missing,
        "model_used": model_used,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    Path(out_json).write_text(json.dumps(final, indent=2, ensure_ascii=False), encoding="utf-8")

    if ok:
        logger.info(f"✅ JSON complet sauvegardé ({model_used}) : {out_json}")
    else:
        logger.warning(f"⚠️ JSON partiel sauvegardé ({model_used}) ({len(missing)} champs manquants) : {out_json}")

    return final

# ============================================================
# CLI COMPATIBLE ORCHESTRATOR
# ============================================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Analyse CERFA via Mistral (large + fallback small)")
    ap.add_argument("--pdf", required=True, help="Chemin du PDF CERFA à analyser")
    ap.add_argument("--out-json", default="cerfa_result.json", help="Chemin du JSON de sortie")
    args = ap.parse_args()

    analyse_cerfa(args.pdf, args.out_json)
