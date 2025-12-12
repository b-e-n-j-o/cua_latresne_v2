"""
AUDIT COMPLET DE L'API MISTRAL VISION POUR PDF
==============================================

Ce script analyse en détail ce qui se passe à chaque étape :
1. Upload du PDF
2. Traitement par l'API
3. Réponse brute
4. Analyse finale
"""

import os
import base64
import json
from pathlib import Path
from mistralai import Mistral
from dotenv import load_dotenv
import time

load_dotenv()

API_KEY = os.getenv("MISTRAL_API_KEY")
if not API_KEY:
    raise ValueError("⚠️ MISTRAL_API_KEY manquante")

client = Mistral(api_key=API_KEY)


def etape_1_upload_pdf(pdf_path: str):
    """
    ÉTAPE 1: Upload du PDF vers Mistral Cloud
    
    Ce qui se passe:
    - Le PDF est envoyé tel quel (binaire) vers Mistral
    - Mistral le stocke temporairement sur leur cloud
    - On récupère une URL signée (temporaire, ~1h)
    - Le PDF N'EST PAS encore traité/analysé
    """
    print("\n" + "="*80)
    print("🔍 ÉTAPE 1: UPLOAD DU PDF")
    print("="*80)
    
    pdf_size = os.path.getsize(pdf_path) / 1024  # KB
    print(f"📄 Fichier: {Path(pdf_path).name}")
    print(f"📊 Taille: {pdf_size:.2f} KB")
    
    print("\n⏳ Upload en cours vers Mistral Cloud...")
    start = time.time()
    
    uploaded_pdf = client.files.upload(
        file={
            "file_name": Path(pdf_path).name,
            "content": open(pdf_path, "rb"),  # Binaire brut, pas d'encodage
        },
        purpose="ocr"  # Catégorie de stockage
    )
    
    upload_time = time.time() - start
    
    print(f"✅ Upload réussi en {upload_time:.2f}s")
    print(f"\n📋 Métadonnées du fichier uploadé:")
    print(f"   - ID: {uploaded_pdf.id}")
    print(f"   - Nom: {uploaded_pdf.filename}")
    # Taille déjà affichée au début
    print(f"   - Purpose: {uploaded_pdf.purpose}")
    print(f"   - Créé: {uploaded_pdf.created_at}")
    
    # Obtenir l'URL signée
    signed_url = client.files.get_signed_url(file_id=uploaded_pdf.id)
    
    print(f"\n🔗 URL signée obtenue:")
    print(f"   {signed_url.url[:100]}...")
    print(f"\n⚠️  IMPORTANT: Le PDF est maintenant sur le cloud Mistral")
    print(f"   - Il N'est PAS encore traité/analysé")
    print(f"   - C'est juste un stockage temporaire")
    print(f"   - L'URL expire après ~1h")
    
    return uploaded_pdf.id, signed_url.url


def etape_2_analyse_vision(pdf_url: str, prompt: str, model: str = "ministral-3b-2512"):
    """
    ÉTAPE 2: Analyse du PDF par le LLM Vision
    
    Ce qui se passe:
    1. L'API reçoit l'URL du PDF + votre prompt
    2. Mistral convertit AUTOMATIQUEMENT le PDF en images (une par page)
    3. Le LLM Vision analyse ces images
    4. Le LLM génère une réponse basée sur ce qu'il "voit"
    
    IL N'Y A PAS D'OCR TRADITIONNEL
    - Le modèle "voit" le PDF comme des images
    - Il comprend le texte, les tableaux, les graphiques visuellement
    - C'est comme montrer des photos à un humain qui lit
    """
    print("\n" + "="*80)
    print("🔍 ÉTAPE 2: ANALYSE VISION DU PDF")
    print("="*80)
    
    print(f"🤖 Modèle: {model}")
    print(f"💬 Prompt: {prompt[:100]}...")
    
    # Construction de la requête
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "document_url", "document_url": pdf_url}
            ]
        }
    ]
    
    print(f"\n📤 Envoi de la requête à l'API Chat Completions...")
    print(f"   - Type de contenu: document_url")
    print(f"   - Le PDF sera converti en images par Mistral")
    print(f"   - Le LLM analysera visuellement chaque page")
    
    start = time.time()
    
    response = client.chat.complete(
        model=model,
        messages=messages,
        max_tokens=4096
    )
    
    inference_time = time.time() - start
    
    print(f"\n✅ Réponse reçue en {inference_time:.2f}s")
    
    return response, inference_time


def etape_3_analyse_reponse_brute(response):
    """
    ÉTAPE 3: Analyse de la réponse brute de l'API
    
    La réponse contient:
    - Le texte généré par le LLM (PAS un OCR brut)
    - Les métadonnées d'usage (tokens)
    - Le statut de complétion
    """
    print("\n" + "="*80)
    print("🔍 ÉTAPE 3: STRUCTURE DE LA RÉPONSE BRUTE")
    print("="*80)
    
    print("\n📦 Objet response complet:")
    print(f"   Type: {type(response)}")
    
    # Structure principale
    print(f"\n📋 Attributs disponibles:")
    for attr in dir(response):
        if not attr.startswith('_'):
            print(f"   - {attr}")
    
    # Choices
    print(f"\n💬 response.choices:")
    print(f"   Nombre de choix: {len(response.choices)}")
    choice = response.choices[0]
    print(f"   - index: {choice.index}")
    print(f"   - finish_reason: {choice.finish_reason}")
    
    # Message
    print(f"\n📝 response.choices[0].message:")
    message = choice.message
    print(f"   - role: {message.role}")
    print(f"   - content (aperçu): {message.content[:200]}...")
    print(f"   - content (longueur): {len(message.content)} caractères")
    
    # Usage
    print(f"\n📊 response.usage (tokens consommés):")
    usage = response.usage
    print(f"   - prompt_tokens: {usage.prompt_tokens}")
    print(f"   - completion_tokens: {usage.completion_tokens}")
    print(f"   - total_tokens: {usage.total_tokens}")
    
    # Calcul approximatif du coût (pour ministral-3b)
    cost_input = (usage.prompt_tokens / 1_000_000) * 0.04  # $0.04/1M tokens
    cost_output = (usage.completion_tokens / 1_000_000) * 0.04
    total_cost = cost_input + cost_output
    
    print(f"\n💰 Coût estimé (ministral-3b-2512):")
    print(f"   - Input: ${cost_input:.6f}")
    print(f"   - Output: ${cost_output:.6f}")
    print(f"   - Total: ${total_cost:.6f}")
    
    # Autres métadonnées
    print(f"\n🏷️  Autres métadonnées:")
    print(f"   - id: {response.id}")
    print(f"   - model: {response.model}")
    print(f"   - created: {response.created}")
    
    print(f"\n⚠️  POINT CLÉ:")
    print(f"   - Le 'content' est la RÉPONSE DU LLM, pas un OCR brut")
    print(f"   - Le LLM a 'vu' le PDF et a répondu à votre question")
    print(f"   - Il n'y a PAS de sortie intermédiaire (texte OCR brut)")
    
    return message.content


def etape_4_comparaison_methodes():
    """
    ÉTAPE 4: Comparaison des différentes méthodes Mistral
    """
    print("\n" + "="*80)
    print("🔍 ÉTAPE 4: COMPARAISON DES MÉTHODES MISTRAL")
    print("="*80)
    
    comparison = """
┌─────────────────────┬──────────────────────────┬──────────────────────────┐
│                     │   VISION API (ce code)   │      OCR API             │
├─────────────────────┼──────────────────────────┼──────────────────────────┤
│ Endpoint            │ /v1/chat/completions     │ /v1/ocr                  │
│ Modèle              │ ministral-3b/8b/14b      │ mistral-ocr-latest       │
│ Input               │ PDF → Images (auto)      │ PDF → OCR direct         │
│ Traitement          │ LLM Vision "voit" pages  │ OCR extraction texte     │
│ Output              │ Réponse LLM au prompt    │ Markdown + images base64 │
│ Flexibilité         │ ✅ Haute (prompts)       │ ❌ Fixe (extraction)     │
│ Use case            │ Q&A, analyse, résumé     │ Extraction texte brut    │
│ Sortie brute        │ ❌ Non (direct LLM)      │ ✅ Oui (markdown/JSON)   │
└─────────────────────┴──────────────────────────┴──────────────────────────┘

WORKFLOW VISION API:
1. PDF uploadé → Stockage cloud Mistral
2. URL signée récupérée
3. API Chat /completions appelée avec document_url
4. Mistral convertit PDF → images (invisible pour toi)
5. LLM Vision analyse les images
6. LLM génère réponse basée sur le prompt
7. Tu reçois: réponse texte + métadonnées

WORKFLOW OCR API:
1. PDF uploadé OU envoyé en base64
2. API OCR appelée
3. Mistral fait OCR de toutes les pages
4. Tu reçois: markdown complet + bboxes images + base64

ENCODAGE:
- Vision API: PDF binaire brut → upload → URL
- OCR API: PDF base64 OU URL publique OU upload
- Dans les 2 cas: pas d'encodage manuel nécessaire avec upload
"""
    print(comparison)


def audit_complet(pdf_path: str):
    """
    AUDIT COMPLET: Exécuter toutes les étapes avec un PDF
    """
    print("\n" + "="*100)
    print("🔬 AUDIT COMPLET DE L'API MISTRAL VISION POUR PDF")
    print("="*100)
    
    if not os.path.exists(pdf_path):
        print(f"❌ Fichier non trouvé: {pdf_path}")
        return
    
    try:
        # ÉTAPE 1: Upload
        file_id, pdf_url = etape_1_upload_pdf(pdf_path)
        
        # ÉTAPE 2: Analyse Vision
        prompt = "Décris ce document: type, structure, contenu principal, données chiffrées."
        response, inference_time = etape_2_analyse_vision(pdf_url, prompt)
        
        # ÉTAPE 3: Analyse réponse brute
        content = etape_3_analyse_reponse_brute(response)
        
        # ÉTAPE 4: Comparaison méthodes
        etape_4_comparaison_methodes()
        
        # Afficher le résultat final
        print("\n" + "="*80)
        print("📄 CONTENU DE LA RÉPONSE LLM")
        print("="*80)
        print(content)
        print("="*80)
        
        # Nettoyage
        print("\n🗑️  Nettoyage du fichier temporaire...")
        client.files.delete(file_id=file_id)
        print("✅ Fichier supprimé du cloud Mistral")
        
        # Résumé final
        print("\n" + "="*80)
        print("📊 RÉSUMÉ DE L'AUDIT")
        print("="*80)
        print(f"""
✅ Le PDF a été traité avec succès

POINTS CLÉS:
1. Le PDF est uploadé en BINAIRE (pas d'encodage base64)
2. Mistral le stocke temporairement et donne une URL signée
3. L'API Chat Completions CONVERTIT le PDF en images automatiquement
4. Le LLM Vision ANALYSE VISUELLEMENT ces images (pas d'OCR classique)
5. Tu reçois une RÉPONSE GÉNÉRÉE par le LLM, pas un texte OCR brut
6. Il n'y a PAS de sortie intermédiaire accessible

DIFFÉRENCE AVEC GEMINI:
- Gemini: Envoie PDF en base64 directement dans la requête
- Mistral: Upload d'abord, puis référence par URL
- Résultat: Même principe (LLM Vision), différente implémentation

POUR AVOIR L'OCR BRUT:
- Utilise l'API OCR: client.ocr.process()
- Elle retourne le markdown extrait + images
- Mais moins flexible pour l'analyse custom
""")
        
    except Exception as e:
        print(f"\n❌ Erreur pendant l'audit: {e}")
        import traceback
        traceback.print_exc()


def test_methode_base64(pdf_path: str):
    """
    TEST BONUS: Méthode base64 (sans upload)
    
    IMPORTANT: Mistral Vision n'accepte pas data:application/pdf en base64
    Il faut soit:
    1. Convertir PDF → image puis envoyer l'image en base64
    2. Utiliser la méthode upload (recommandée)
    """
    print("\n" + "="*80)
    print("🔍 TEST BONUS: MÉTHODE BASE64")
    print("="*80)
    
    print("⏳ Encodage du PDF en base64...")
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()
        base64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
    
    print(f"✅ PDF encodé: {len(base64_pdf)} caractères base64")
    print(f"   Ratio: {len(base64_pdf) / len(pdf_bytes):.2f}x la taille originale")
    
    print("\n⚠️  LIMITATION MISTRAL:")
    print("   - Mistral Vision n'accepte PAS 'data:application/pdf;base64,...'")
    print("   - Il faut 'data:image/<format>;base64,...' pour les images")
    print("   - Pour les PDFs: utiliser la méthode upload (RECOMMANDÉE)")
    
    print("\n💡 Solution: Convertir PDF → image avec pdf2image, puis envoyer l'image")
    print("   Mais c'est complexe et moins performant que l'upload direct")
    
    print("\n✅ RECOMMANDATION: Utilise toujours la méthode upload pour les PDFs")


def main():
    """
    Point d'entrée principal
    """
    # Ton PDF
    pdf_path = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    # Lancer l'audit complet
    audit_complet(pdf_path)
    
    # Test bonus de la méthode base64
    print("\n" + "="*100)
    test_methode_base64(pdf_path)


if __name__ == "__main__":
    main()