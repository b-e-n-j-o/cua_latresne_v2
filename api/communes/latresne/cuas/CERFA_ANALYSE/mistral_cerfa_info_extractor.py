"""
Extraction complète CERFA 13410*12 avec Mistral Vision
Toutes infos sauf parcelles (module séparé)
"""

import os
import base64
import json
import re
from pathlib import Path
from typing import Optional, Dict
from mistralai import Mistral
from pdf2image import convert_from_path
from dotenv import load_dotenv

load_dotenv()


class CERFAInfoExtractor:
    """Extracteur infos générales CERFA"""
    
    VISUAL_HINTS = """
📍 LOCALISATION VISUELLE
═══════════════════════════════════════════════════════════════════════════════
📌 EN-TÊTE (PAGE 1, coin supérieur droit)
CU [Dpt] [Commune] [Année] [N° dossier]
   033   234       25      00078

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
   Ex: dept=33, commune_code=234 → insee=33234
2. commune_nom = section 4.1 "Localité" (PAS section 3)
3. Type demandeur : "particulier" (2.1) ou "personne_morale" (2.2)
4. Type CU : case a) → "CUa", case b) → "CUb"
5. Header CU : page 1 coin supérieur droit
6. Date dépôt : si présente dans header CU

{visual_hints}

Retourne UNIQUEMENT le JSON."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            raise ValueError("MISTRAL_API_KEY requise")
        self.client = Mistral(api_key=self.api_key)
    
    def _pdf_to_images(self, pdf_path: str, pages: list = None, dpi: int = 250) -> list:
        """Convertit PDF en images base64"""
        # Pages 1-4 seulement (infos utiles)
        if pages is None:
            pages = [1, 2, 3, 4]
        
        images_b64 = []
        for page_num in pages:
            imgs = convert_from_path(pdf_path, dpi=dpi, first_page=page_num, last_page=page_num)
            tmp = f"/tmp/cerfa_info_{page_num}.png"
            imgs[0].save(tmp, "PNG")
            
            with open(tmp, "rb") as f:
                images_b64.append(base64.b64encode(f.read()).decode())
            os.remove(tmp)
        
        return images_b64
    
    def _parse_json(self, text: str) -> dict:
        """
        Parse JSON depuis réponse LLM avec nettoyage robuste
        Gère les cas où le LLM ajoute du texte avant/après, des markdown, etc.
        """
        if not text:
            raise ValueError("Texte vide")
        
        text = text.strip()
        
        # Retirer les markdown code blocks si présents
        if "```" in text:
            parts = text.split("```")
            # Chercher le bloc qui contient le JSON
            for part in parts:
                part = part.strip()
                # Retirer le préfixe "json" si présent
                if part.startswith("json"):
                    part = part[4:].strip()
                # Chercher le premier { qui marque le début du JSON
                if "{" in part:
                    text = part
                    break
        
        # Trouver le premier { et le dernier } pour extraire le JSON brut
        i, j = text.find("{"), text.rfind("}")
        if i == -1 or j == -1 or j <= i:
            raise ValueError(f"Aucun JSON trouvé dans la réponse. Texte: {text[:200]}...")
        
        raw_json = text[i:j+1]
        
        # Première tentative de parsing
        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as e:
            # Nettoyer les erreurs courantes : virgules en trop avant } ou ]
            cleaned = re.sub(r",\s*}", "}", raw_json)
            cleaned = re.sub(r",\s*]", "]", cleaned)
            # Retirer les commentaires JSON potentiels (non standard mais parfois présents)
            cleaned = re.sub(r'//.*?$', '', cleaned, flags=re.MULTILINE)
            # Retirer les caractères de contrôle invisibles
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)
            
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e2:
                # Afficher plus de détails pour le debug
                error_msg = f"Impossible de parser le JSON: {e2}"
                error_msg += f"\nPosition erreur: ligne {e2.lineno}, colonne {e2.colno}"
                error_msg += f"\nContexte (50 chars autour): {raw_json[max(0, e2.pos-25):e2.pos+25]}"
                error_msg += f"\n\nJSON brut (500 premiers chars):\n{raw_json[:500]}"
                raise ValueError(error_msg) from e2
    
    def extraire(
        self,
        pdf_path: str,
        model: str = "mistral-large-2512",
        pages: list = None
    ) -> Dict:
        """
        Extrait infos générales CERFA
        
        Args:
            pdf_path: Chemin PDF
            model: Modèle Mistral
            pages: Pages à analyser (défaut: 1-3)
            
        Returns:
            {"success": bool, "data": dict, "tokens": int}
        """
        
        if not Path(pdf_path).exists():
            return {"success": False, "error": "Fichier introuvable"}
        
        try:
            # Conversion images
            images = self._pdf_to_images(pdf_path, pages)
            
            # Message
            content = [{"type": "text", "text": self.PROMPT.format(visual_hints=self.VISUAL_HINTS)}]
            for img in images:
                content.append({
                    "type": "image_url",
                    "image_url": f"data:image/png;base64,{img}"
                })
            
            # Analyse
            response = self.client.chat.complete(
                model=model,
                messages=[{"role": "user", "content": content}],
                max_tokens=3000,
                temperature=0.0
            )
            
            # 📊 Logging usage et coût
            usage = response.usage
            cost_input = (usage.prompt_tokens / 1_000_000) * 0.5   # $0.5/M tokens
            cost_output = (usage.completion_tokens / 1_000_000) * 1.5  # $1.5/M tokens
            total_cost = cost_input + cost_output
            
            print(f"\n📊 Usage mistral-large-2512:")
            print(f"   Input:  {usage.prompt_tokens:6,} tokens → ${cost_input:.6f}".replace(",", " "))
            print(f"   Output: {usage.completion_tokens:6,} tokens → ${cost_output:.6f}".replace(",", " "))
            print(f"   Total:  {usage.total_tokens:6,} tokens → ${total_cost:.6f}\n".replace(",", " "))
            
            # Parse
            result_text = response.choices[0].message.content
            try:
                data = self._parse_json(result_text)
            except Exception as parse_error:
                return {"success": False, "error": str(parse_error), "raw": result_text}
            
            self._normalize(data)
            
            return {
                "success": True,
                "data": data,
                "usage": {
                    "prompt_tokens": usage.prompt_tokens,
                    "completion_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "cost_input_usd": cost_input,
                    "cost_output_usd": cost_output,
                    "cost_total_usd": total_cost
                },
                "model": response.model
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _normalize(self, data: dict):
        """Normalise les données"""
        # Code INSEE
        if data.get("commune_nom") and data.get("departement_code"):
            dept = str(data["departement_code"]).zfill(2)
            commune_code = data.get("header_cu", {}).get("commune_code", "")
            if commune_code:
                data["commune_insee"] = dept + str(commune_code).zfill(3)
        
        # Numéro CU
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


if __name__ == "__main__":
    pdf = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    result = extraire_info_cerfa(pdf)
    
    if result["success"]:
        data = result["data"]
        usage = result.get("usage", {})
        
        print(f"✅ Extraction OK")
        if usage:
            print(f"   Tokens: {usage.get('total_tokens', 0):,} | Coût: ${usage.get('cost_total_usd', 0):.6f}")
        print()
        print(f"Commune    : {data.get('commune_nom')} ({data.get('commune_insee')})")
        print(f"N° CU      : {data.get('numero_cu')}")
        print(f"Type       : {data.get('type_cu')}")
        print(f"Demandeur  : {data.get('demandeur', {}).get('type')}")
        
        with open("cerfa_info.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print("\n💾 cerfa_info.json")
    else:
        print(f"❌ {result['error']}")