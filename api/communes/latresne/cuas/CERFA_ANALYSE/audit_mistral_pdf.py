"""
Extraction structurée de données CERFA 13410*12 avec Mistral Vision API
"""

import os
import json
from pathlib import Path
from mistralai import Mistral
from dotenv import load_dotenv

load_dotenv()

# Configuration
API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise ValueError("MISTRAL_API_KEY manquante")

client = Mistral(api_key=API_KEY)

# ============================================================
# PROMPT D'EXTRACTION
# ============================================================
EXTRACTION_PROMPT = """Tu es un expert en lecture de formulaires CERFA et en extraction d'informations structurées.

Analyse le document PDF fourni (CERFA 13410*12) et renvoie **UNIQUEMENT** un JSON strict conforme au schéma ci-dessous.

⚠️ NE FOURNIS AUCUN TEXTE HORS DU JSON. NE COMMENTE RIEN. N'EXPLIQUE RIEN.

Les informations les plus importantes sont les données coordonnées du demandeur, et les informations cadastrales, soit la liste complète et exhaustive et correcte des parcelles concernées par le cerfa.

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
Exemple : 033234 = 33234, le code insee est à 5 chiffres, ex: 33234 et est composé du departement en 2 chiffres, puis la commune en 3 chiffres, ex: 33234 = 33 et 234. 

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

📌 RÉFÉRENCES CADASTRALES (PAGE 2, section 4.2)
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

⚠️ Si > 3 parcelles → CONTINUER SUR PAGE ANNEXE 8
┌─────────────────────────────────────────────────────────┐
│ ANNEXE - Références cadastrales complémentaires        │
│ (dernière page du PDF)                                 │
│                                                         │
│ Section : [AI]  Numéro : [0313]  Superficie : 4931 m² │
│ Section : [__]  Numéro : [____]  Superficie : ____ m² │
└─────────────────────────────────────────────────────────┘

📌 NUMÉRO CU COMPLET (à reconstruire)
Format final attendu : [Dept]-[Commune]-20[Année]-X[Dossier]
Exemple : 033-234-2025-X00078

Construction depuis header_cu :
• Dept = 033 → "033"
• Commune = 234 → "234"  
• Année = 25 → "2025"
• Dossier = 00078 → "X00078"

═══════════════════════════════════════════════════════════════════════════════
⚠️ RÈGLES CRITIQUES
═══════════════════════════════════════════════════════════════════════════════
1. Le header_cu se trouve TOUJOURS page 1, cadre supérieur droit
2. La commune_nom vient de section 4.1 "Localité" (PAS section 3)
3. TOUJOURS vérifier la page annexe pour parcelles supplémentaires
4. La superficie totale DOIT être >= somme des surfaces individuelles
5. Ne JAMAIS inventer de valeurs absentes du document
═══════════════════════════════════════════════════════════════════════════════


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
    "type": "particulier ou personne_morale",
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
RÈGLES D'EXTRACTION :
───────────────────────────────────────────────
0. Extraire impérativement le code INSEE à 5 chiffres (ex: 33234 = dept 33 + commune 234).

1. Si « Vous êtes un particulier » (2.1) coché → type = "particulier"
   Extraire : nom, prénom, adresse, email, téléphone.

2. Si « Vous êtes une personne morale » (2.2) coché → type = "personne_morale"
   Extraire : dénomination, SIRET, type société, représentant légal.

3. Adresse demandeur = section 3
   Adresse terrain = section 4.1 (page 2)

4. Références cadastrales = section 4.2 + annexes
   Format : section, numéro, surface_m2
   Calculer superficie_totale_m2

5. Numéro CU complet : [dept]-[commune_code]-20[annee]-X[numero_dossier]
   À partir du cadre supérieur droit page 1.

6. Inclure toutes les clés, même si null.

LOCALISATION VISUELLE :
- Page 1, haut : numéro CU (cadre avec champs dept/commune/année/dossier)
- Page 1, sections 2.1/2.2 : type demandeur
- Page 1, section 3 : coordonnées demandeur
- Page 2, section 4.1 : adresse terrain
- Page 2, section 4.2 : parcelles cadastrales (+ annexes éventuelles)

───────────────────────────────────────────────
IMPORTANT :
- Ne pas inventer de données
- Garder noms/adresses en français
- Toujours retourner un JSON valide
───────────────────────────────────────────────
"""


def extraire_json_de_texte(texte: str) -> dict:
    """Extrait le JSON du texte LLM (nettoie les balises markdown)"""
    texte = texte.strip()
    
    # Retirer balises markdown
    if texte.startswith("```json"):
        texte = texte[7:]
    elif texte.startswith("```"):
        texte = texte[3:]
    
    if texte.endswith("```"):
        texte = texte[:-3]
    
    texte = texte.strip()
    
    return json.loads(texte)


def extraire_cerfa(pdf_path: str, model: str = "ministral-8b-2512") -> dict:
    """
    Extrait les données structurées d'un CERFA avec Mistral Vision
    
    Args:
        pdf_path: Chemin vers le PDF CERFA
        model: Modèle Mistral (ministral-8b/14b recommandés pour extraction précise)
        
    Returns:
        Dictionnaire avec données extraites
    """
    
    print(f"📄 Traitement de : {Path(pdf_path).name}")
    
    # Upload PDF
    print("📤 Upload du PDF...")
    uploaded = client.files.upload(
        file={
            "file_name": Path(pdf_path).name,
            "content": open(pdf_path, "rb")
        },
        purpose="ocr"
    )
    
    url = client.files.get_signed_url(file_id=uploaded.id).url
    print(f"✅ Upload OK (ID: {uploaded.id})")
    
    # Analyse Vision
    print(f"🤖 Extraction avec {model}...")
    
    try:
        response = client.chat.complete(
            model=model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": EXTRACTION_PROMPT},
                    {"type": "document_url", "document_url": url}
                ]
            }],
            max_tokens=4096,
            temperature=0.0  # Déterministe pour extraction
        )
        
        content = response.choices[0].message.content
        
        print(f"✅ Réponse reçue ({response.usage.total_tokens} tokens)")
        
        # Parser JSON
        data = extraire_json_de_texte(content)
        
        # Cleanup
        client.files.delete(file_id=uploaded.id)
        
        return {
            "success": True,
            "data": data,
            "tokens": response.usage.total_tokens,
            "model": response.model
        }
        
    except json.JSONDecodeError as e:
        print(f"❌ Erreur parsing JSON: {e}")
        print(f"Contenu brut:\n{content[:500]}...")
        
        return {
            "success": False,
            "error": f"JSON invalide: {e}",
            "raw_content": content
        }
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return {"success": False, "error": str(e)}
    
    finally:
        # Nettoyage garanti
        try:
            client.files.delete(file_id=uploaded.id)
        except:
            pass


def valider_extraction(data: dict) -> list:
    """Valide la complétude des données extraites"""
    
    manquants = []
    
    # Champs critiques
    if not data.get("commune_nom"):
        manquants.append("commune_nom")
    if not data.get("commune_insee"):
        manquants.append("commune_insee")
    if not data.get("demandeur", {}).get("type"):
        manquants.append("demandeur.type")
    if not data.get("adresse_terrain", {}).get("ville"):
        manquants.append("adresse_terrain.ville")
    if not data.get("references_cadastrales"):
        manquants.append("references_cadastrales")
    
    return manquants


def afficher_resume(data: dict):
    """Affiche un résumé des données extraites"""
    
    print("\n" + "="*80)
    print("📊 RÉSUMÉ DES DONNÉES EXTRAITES")
    print("="*80)
    
    print(f"\n🏛️  Commune : {data.get('commune_nom', 'N/A')} ({data.get('commune_insee', 'N/A')})")
    print(f"📋 Type CU : {data.get('type_cu', 'N/A')}")
    print(f"🔢 Numéro : {data.get('numero_cu', 'N/A')}")
    
    dem = data.get('demandeur', {})
    print(f"\n👤 Demandeur : {dem.get('type', 'N/A')}")
    if dem.get('type') == 'particulier':
        print(f"   {dem.get('nom', '')} {dem.get('prenom', '')}")
    else:
        print(f"   {dem.get('denomination', 'N/A')}")
        print(f"   SIRET: {dem.get('siret', 'N/A')}")
    
    terrain = data.get('adresse_terrain', {})
    print(f"\n🏠 Terrain : {terrain.get('numero', '')} {terrain.get('voie', '')}")
    print(f"   {terrain.get('code_postal', '')} {terrain.get('ville', '')}")
    
    parcelles = data.get('references_cadastrales', [])
    print(f"\n📐 Parcelles cadastrales : {len(parcelles)}")
    for p in parcelles[:3]:  # Afficher 3 premières
        print(f"   - Section {p.get('section', 'N/A')} n°{p.get('numero', 'N/A')} ({p.get('surface_m2', 'N/A')} m²)")
    if len(parcelles) > 3:
        print(f"   ... et {len(parcelles) - 3} autres")
    
    print(f"\n📏 Superficie totale : {data.get('superficie_totale_m2', 'N/A')} m²")
    print("="*80)


def main():
    """Point d'entrée"""
    
    # Chemin PDF
    pdf_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    if not os.path.exists(pdf_path):
        print(f"❌ Fichier introuvable: {pdf_path}")
        return
    
    print("\n" + "="*80)
    print("🔍 EXTRACTION STRUCTURÉE CERFA - MISTRAL VISION")
    print("="*80 + "\n")
    
    # Extraction
    result = extraire_cerfa(
        pdf_path=pdf_path,
        model="ministral-14b-2512"  # 14B pour meilleure précision
    )
    
    if not result["success"]:
        print(f"\n❌ Échec: {result.get('error')}")
        if "raw_content" in result:
            print("\n📄 Contenu brut:")
            print(result["raw_content"][:1000])
        return
    
    data = result["data"]
    
    # Affichage
    afficher_resume(data)
    
    # Validation
    manquants = valider_extraction(data)
    if manquants:
        print(f"\n⚠️  Champs manquants: {', '.join(manquants)}")
    else:
        print("\n✅ Extraction complète")
    
    # Sauvegarde JSON
    output_path = "cerfa_extrait.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Données sauvegardées : {output_path}")
    print(f"🎯 Tokens utilisés : {result['tokens']}")


if __name__ == "__main__":
    main()