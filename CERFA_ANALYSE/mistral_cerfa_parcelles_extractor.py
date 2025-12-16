"""
Module d'extraction de parcelles cadastrales CERFA 13410*12
Extraction des pages 2 et 4 → références cadastrales + superficie totale
"""

import os
import re
import base64
import json
from pathlib import Path
from typing import List, Dict, Optional
from mistralai import Mistral
from pdf2image import convert_from_path
from dotenv import load_dotenv
load_dotenv()
import logging
logger = logging.getLogger("mistral_pdf")

API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise ValueError("⚠️ Tu dois définir MISTRAL_API_KEY dans ton environnement export MISTRAL_API_KEY='xxx'")

logger.info("🔐 Clé API chargée.")

class CERFAParcellesExtractor:
    """Extracteur de parcelles cadastrales depuis CERFA 13410"""
    
    PROMPT = """Extrais TOUTES les références cadastrales de ces images du CERFA 13410.

Image 1 : Section 4.2 avec premières parcelles
Image 2 : Page annexe avec parcelles complémentaires

Recherche aussi la "Superficie totale du terrain (en m²)" mentionnée.
Voici un exemple de JSON que tu dois retourner, ce ne sont pas les vraies valeurs, tu dois les extraire du PDF :
JSON strict :
{
  "references_cadastrales": [
    {"section": "AC", "numero": "0494", "surface_m2": 5755},
    {"section": "AK", "numero": "0058", "surface_m2": 256},
    {"section": "AM", "numero": "0311", "surface_m2": 1368}
  ],
  "superficie_totale_m2": 9520
}
Si les superficies de chaucne des parcelles ne sont pas mentionnées, ne les inclues pas dans le JSON, ecris null dans le champ surface_m2.

La superficie totale est mentionnée en bas de la page numero 2

Règles :
- Section : 1-2 lettres majuscules (AI, AC, ZA)
- Numéro : 4 chiffres (0494, 0058, 0311)
- Surface : entier en m²
- Extraire TOUTES les lignes de parcelles
- superficie_totale_m2 = champ explicite du formulaire

Retourne UNIQUEMENT le JSON."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Args:
            api_key: Clé API Mistral (défaut: variable MISTRAL_API_KEY)
        """
        self.api_key = api_key or os.getenv("MISTRAL_API_KEY")
        if not self.api_key:
            raise ValueError("MISTRAL_API_KEY requise")
        self.client = Mistral(api_key=self.api_key)
    
    def _pdf_pages_to_images(self, pdf_path: str, pages: List[int], dpi: int = 300) -> List[str]:
        """Convertit pages PDF en images base64"""
        images_b64 = []
        
        for page_num in pages:
            images = convert_from_path(pdf_path, dpi=dpi, first_page=page_num, last_page=page_num)
            
            tmp_path = f"/tmp/cerfa_page_{page_num}.png"
            images[0].save(tmp_path, "PNG")
            
            with open(tmp_path, "rb") as f:
                images_b64.append(base64.b64encode(f.read()).decode())
            
            os.remove(tmp_path)
        
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
            logger.warning(f"Premier parsing JSON échoué: {e}. Tentative de nettoyage...")
            
            # Nettoyer les erreurs courantes : virgules en trop avant } ou ]
            cleaned = re.sub(r",\s*}", "}", raw_json)
            cleaned = re.sub(r",\s*]", "]", cleaned)
            # Retirer les commentaires JSON potentiels (non standard mais parfois présents)
            cleaned = re.sub(r'//.*?$', '', cleaned, flags=re.MULTILINE)
            
            try:
                return json.loads(cleaned)
            except json.JSONDecodeError as e2:
                logger.error(f"Échec parsing même après nettoyage: {e2}")
                logger.debug(f"JSON brut (200 premiers chars): {raw_json[:200]}")
                raise ValueError(f"Impossible de parser le JSON: {e2}. Réponse LLM: {text[:300]}...")
    
    def extraire(
        self,
        pdf_path: str,
        model: str = "ministral-8b-2512",
        dpi: int = 300
    ) -> Dict:
        """
        Extrait parcelles cadastrales et superficie totale depuis CERFA
        
        Args:
            pdf_path: Chemin vers PDF CERFA 13410*12
            model: Modèle Mistral (14b recommandé pour précision)
            dpi: Résolution images (300 optimal)
            
        Returns:
            {
                "success": bool,
                "data": {
                    "references_cadastrales": [
                        {"section": str, "numero": str, "surface_m2": int}
                    ],
                    "superficie_totale_m2": int
                },
                "stats": {
                    "nb_parcelles": int,
                    "somme_surfaces": int,
                    "ecart_total": int,
                    "tokens": int
                }
            }
        """
        
        if not Path(pdf_path).exists():
            return {"success": False, "error": f"Fichier introuvable: {pdf_path}"}
        
        try:
            # 🔍 Détection automatique des pages cadastrales
            try:
                from CERFA_ANALYSE.detection_pages_cadastrales import (
                    detecter_pages_cadastrales,
                )

                pages_info = detecter_pages_cadastrales(pdf_path, debug=False)
                pages = pages_info["pages_a_extraire"]
                print(f"📄 Pages cadastrales détectées : {pages}")
            except Exception as e:
                # Fallback : comportement historique
                pages = [2, 4]
                print(f"⚠️ Détection pages cadastrales échouée ({e}) → fallback {pages}")

            # Conversion PDF → Images
            images_b64 = self._pdf_pages_to_images(pdf_path, pages, dpi)
            
            # Construction message
            content = [{"type": "text", "text": self.PROMPT}]
            for img_b64 in images_b64:
                content.append({
                    "type": "image_url",
                    "image_url": f"data:image/png;base64,{img_b64}"
                })
            
            # Appel API
            response = self.client.chat.complete(
                model=model,
                messages=[{"role": "user", "content": content}],
                max_tokens=2000,
                temperature=0.0
            )
            
            # Parse résultat
            result_text = response.choices[0].message.content
            logger.debug(f"Réponse brute LLM (200 premiers chars): {result_text[:200]}...")
            
            try:
                data = self._parse_json(result_text)
            except Exception as parse_error:
                logger.error(f"Erreur parsing JSON: {parse_error}")
                logger.error(f"Réponse complète LLM: {result_text}")
                raise ValueError(f"Échec parsing JSON: {parse_error}") from parse_error
            
            # Calcul stats
            parcelles = data.get("references_cadastrales", [])
            total = data.get("superficie_totale_m2") or 0
            # Somme des surfaces (ignorer None)
            somme = sum(p.get("surface_m2") or 0 for p in parcelles if p.get("surface_m2") is not None)
            
            return {
                "success": True,
                "data": data,
                "stats": {
                    "nb_parcelles": len(parcelles),
                    "somme_surfaces": somme,
                    "ecart_total": abs(somme - total) if total else None,
                    "tokens": response.usage.total_tokens
                }
            }
            
        except Exception as e:
            return {"success": False, "error": str(e)}


def extraire_parcelles_cerfa(pdf_path: str, api_key: Optional[str] = None) -> Dict:
    """
    Fonction helper pour extraction rapide
    
    Args:
        pdf_path: Chemin vers PDF CERFA
        api_key: Clé API Mistral (optionnel)
        
    Returns:
        Résultat extraction avec data et stats
    """
    extractor = CERFAParcellesExtractor(api_key=api_key)
    return extractor.extraire(pdf_path)


# Exemple d'utilisation
if __name__ == "__main__":
    pdf_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    result = extraire_parcelles_cerfa(pdf_path)
    
    if result["success"]:
        data = result["data"]
        stats = result["stats"]
        
        print(f"✅ {stats['nb_parcelles']} parcelles extraites\n")
        
        for i, p in enumerate(data["references_cadastrales"], 1):
            section = str(p.get('section') or 'N/A')
            numero = str(p.get('numero') or 'N/A')
            surface = p.get('surface_m2')
            if surface is not None:
                surface_str = f"{surface:8,} m²".replace(",", " ")
            else:
                surface_str = "     N/A"
            print(f"{i:2d}. {section:4s} {numero:6s} → {surface_str}")
        
        print(f"\nSomme    : {stats['somme_surfaces']:,} m²".replace(",", " "))
        total_aff = data.get('superficie_totale_m2')
        if total_aff is not None:
            print(f"Total    : {total_aff:,} m²".replace(",", " "))
        else:
            print(f"Total    : N/A")
        ecart_aff = stats.get('ecart_total')
        if ecart_aff is not None:
            print(f"Écart    : {ecart_aff:,} m²".replace(",", " "))
        else:
            print(f"Écart    : N/A")
        print(f"Tokens   : {stats['tokens']}")
        
        # Sauvegarde
        with open("parcelles_output.json", "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print("\n💾 Sauvegardé : parcelles_output.json")
    else:
        print(f"❌ Erreur : {result['error']}")