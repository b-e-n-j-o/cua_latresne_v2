"""
Extraction complète CERFA 13410*12 avec Gemini Vision
Toutes infos sauf parcelles (module séparé)
"""

import os
import json
import re
import logging
import time
from pathlib import Path
from typing import Optional, Dict
import google.generativeai as genai
from pdf2image import convert_from_path
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("cerfa.extractor.info")


class CERFAInfoExtractor:
    """Extracteur infos générales CERFA"""
    
    VISUAL_HINTS = """
📍 LOCALISATION VISUELLE
═══════════════════════════════════════════════════════════════════════════════
📌 EN-TÊTE (PAGE 1, coin supérieur droit)
CU [Dpt] [Commune] [Année] [N° dossier]
CU 033-234-24-X-0078

📌 TYPE CU (PAGE 1, section 1)
☑ a) Information → "CUa"
☐ b) Opérationnel → "CUb"

📌 DEMANDEUR (PAGE 1, section 2)
2.1 Particulier → type="particulier"
2.2 Personne morale → type="personne_morale" + SIRET

📌 ADRESSE TERRAIN (PAGE 2, section 4.1)
Localité = commune_nom
Code postal = 5 chiffres

⚠️ Adresse terrain ≠ adresse demandeur (section 3)
═══════════════════════════════════════════════════════════════════════════════
"""
    
    PROMPT = """Tu es un expert CERFA 13410*12. Extrais les infos (SAUF parcelles cadastrales).

⚠️ IMPORTANT : Retourne UNIQUEMENT un JSON valide, sans texte avant ou après. Commence directement par {{ et termine par }}.

JSON strict :
{{
  "cerfa_reference": "13410*12",
  "commune_nom": null,
  "commune_insee": null,
  "departement_code": null,
  "numero_cu": null,
  "type_cu": null,
  "date_depot": null,
  "demandeur": {{
    "type": null,
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
  "header_cu": {{
    "dept": null,
    "commune_code": null,
    "annee": null,
    "numero_dossier": null
  }}
}}

RÈGLES :
1. Code INSEE = dept (2 chiffres) + commune_code (3 chiffres) → 5 chiffres
   Ex: dept=033, commune_code=234 → insee=33234
2. commune_nom = section 4.1 "Localité" (PAS section 3)
3. Type demandeur : "particulier" (2.1) ou "personne_morale" (2.2)
4. Type CU : case a) → "CUa", case b) → "CUb"
5. Header CU : page 1 coin supérieur droit
6. Date dépôt : si présente dans header CU

{visual_hints}

Retourne UNIQUEMENT le JSON."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY requise")
        genai.configure(api_key=self.api_key)
    
    def _pdf_to_images(self, pdf_path: str, pages: list = None, dpi: int = 150):
        """Convertit PDF en images PIL"""
        if pages is None:
            pages = [1, 2]
        
        images = []
        for page_num in pages:
            imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_num, last_page=page_num)
            images.append(imgs[0])
        
        return images
    
    def _parse_json(self, text: str) -> dict:
        """Parse JSON depuis réponse LLM avec nettoyage robuste"""
        if not text:
            raise ValueError("Texte vide")
        
        text = text.strip()
        
        if "```" in text:
            parts = text.split("```")
            for part in parts:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if "{" in part:
                    text = part
                    break
        
        i, j = text.find("{"), text.rfind("}")
        if i == -1 or j == -1 or j <= i:
            raise ValueError(f"Aucun JSON trouvé dans la réponse. Texte: {text[:200]}...")
        
        raw_json = text[i:j+1]
        
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as e:
            cleaned = re.sub(r",\s*}", "}", raw_json)
            cleaned = re.sub(r",\s*]", "]", cleaned)
            cleaned = re.sub(r'//.*?$', '', cleaned, flags=re.MULTILINE)
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
            
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e2:
                error_msg = f"Impossible de parser le JSON: {e2}"
                error_msg += f"\nPosition erreur: ligne {e2.lineno}, colonne {e2.colno}"
                error_msg += f"\nContexte (50 chars autour): {raw_json[max(0, e2.pos-25):e2.pos+25]}"
                error_msg += f"\n\nJSON brut (500 premiers chars):\n{raw_json[:500]}"
                raise ValueError(error_msg) from e2
    
    def extraire(
        self,
        pdf_path: str,
        model: str = "gemini-2.5-flash",
        pages: list = None
    ) -> Dict:
        """Extrait infos générales CERFA"""
        
        if not Path(pdf_path).exists():
            logger.error("PDF file not found", extra={"path": pdf_path})
            return {"success": False, "error": "Fichier introuvable"}
        
        try:
            logger.info("Calling Gemini for general info extraction", extra={
                "model": model,
                "pages": pages or [1, 2],
            })
            
            t_start = time.time()
            
            # Conversion images
            images = self._pdf_to_images(pdf_path, pages)
            logger.debug(f"Converted {len(images)} pages to images")
            
            # Message
            prompt = self.PROMPT.format(visual_hints=self.VISUAL_HINTS)
            content = [prompt] + images
            
            # Analyse
            try:
                model_instance = genai.GenerativeModel(model)
                response = model_instance.generate_content(content)
            except Exception as api_error:
                logger.exception("Gemini API error", extra={
                    "model": model,
                    "error_type": type(api_error).__name__,
                })
                raise
            
            # Usage et coût
            usage = response.usage_metadata if hasattr(response, 'usage_metadata') else None
            duration_ms = int((time.time() - t_start) * 1000)
            
            usage_dict = {}
            if usage:
                # Gemini Flash coûts approximatifs
                cost_input = (usage.prompt_token_count / 1_000_000) * 0.075  # $0.075/M
                cost_output = (usage.candidates_token_count / 1_000_000) * 0.30  # $0.30/M
                total_cost = cost_input + cost_output
                
                usage_dict = {
                    "prompt_tokens": usage.prompt_token_count,
                    "completion_tokens": usage.candidates_token_count,
                    "total_tokens": usage.total_token_count,
                    "cost_input_usd": cost_input,
                    "cost_output_usd": cost_output,
                    "cost_total_usd": total_cost
                }
                
                logger.info("Gemini API call completed", extra={
                    "model": model,
                    **usage_dict,
                    "duration_ms": duration_ms,
                })
            
            # Parse
            result_text = response.text
            try:
                data = self._parse_json(result_text)
                logger.debug("JSON parsing successful")
            except Exception as parse_error:
                logger.error("JSON parsing failed", extra={
                    "error": str(parse_error),
                    "raw_preview": result_text[:200] if result_text else None,
                })
                return {"success": False, "error": str(parse_error), "raw": result_text}
            
            self._normalize(data)
            
            return {
                "success": True,
                "data": data,
                "usage": usage_dict,
                "model": model
            }
            
        except Exception as e:
            logger.exception("Unexpected error in info extraction", extra={
                "error_type": type(e).__name__,
            })
            return {"success": False, "error": str(e)}
    
    def _normalize(self, data: dict):
        """Normalise les données"""
        if data.get("commune_nom") and data.get("departement_code"):
            dept = str(data["departement_code"]).zfill(2)
            commune_code = data.get("header_cu", {}).get("commune_code", "")
            if commune_code:
                data["commune_insee"] = dept + str(commune_code).zfill(3)
        
        header = data.get("header_cu", {})
        if all(header.get(k) for k in ["dept", "commune_code", "annee", "numero_dossier"]):
            dept = str(header["dept"]).zfill(3)
            comm = str(header["commune_code"]).zfill(3)
            annee = str(header["annee"]).zfill(2)
            dossier = str(header["numero_dossier"]).zfill(5)
            data["numero_cu"] = f"{dept}-{comm}-20{annee}-X{dossier}"


def extraire_info_cerfa(pdf_path: str, api_key: Optional[str] = None) -> Dict:
    """Helper rapide"""
    extractor = CERFAInfoExtractor(api_key=api_key)
    return extractor.extraire(pdf_path)