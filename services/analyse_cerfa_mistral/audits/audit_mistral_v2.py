import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from mistralai import Mistral

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'mistral_audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

def audit_api_key(client):
    """Tente de récupérer les infos sur la clé API"""
    try:
        # Mistral n'a pas d'endpoint public pour auditer les clés
        # On peut faire un appel minimal pour vérifier la validité
        logger.info("🔑 Test de validité de la clé API...")
        response = client.chat.complete(
            model="mistral-large-latest",
            messages=[{"role": "user", "content": "test"}],
            max_tokens=5
        )
        logger.info("✅ Clé API valide")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur de validation de clé: {type(e).__name__}: {str(e)}")
        return False

def audit_mistral_call():
    """Audit complet d'un appel API Mistral"""
    
    # Vérification de la clé API
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        logger.error("❌ MISTRAL_API_KEY non trouvée dans .env")
        return
    
    logger.info(f"🔑 Clé API trouvée: {api_key[:10]}...{api_key[-4:]}")
    
    try:
        # Initialisation du client
        client = Mistral(api_key=api_key)
        logger.info("✅ Client Mistral initialisé")
        
        # Audit de la clé
        audit_api_key(client)
        
        # Préparation de la requête
        model = "mistral-large-latest"
        messages = [
            {"role": "user", "content": "Explique-moi en 3 phrases ce qu'est l'intelligence artificielle."}
        ]
        
        logger.info("=" * 80)
        logger.info("📤 REQUÊTE API")
        logger.info(f"Model: {model}")
        logger.info(f"Messages: {json.dumps(messages, indent=2, ensure_ascii=False)}")
        logger.info("=" * 80)
        
        # Appel API avec mesure du temps
        start_time = datetime.now()
        logger.info(f"⏱️  Début de l'appel: {start_time}")
        
        response = client.chat.complete(
            model=model,
            messages=messages
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"⏱️  Fin de l'appel: {end_time}")
        logger.info(f"⏱️  Durée: {duration:.2f}s")
        
        # Affichage détaillé de la réponse
        logger.info("=" * 80)
        logger.info("📥 RÉPONSE API")
        logger.info("=" * 80)
        
        # Réponse complète en JSON
        response_dict = response.model_dump()
        logger.info(f"Réponse complète (JSON):\n{json.dumps(response_dict, indent=2, ensure_ascii=False)}")
        
        # Détails importants
        logger.info(f"\n🆔 ID: {response.id}")
        logger.info(f"📊 Model: {response.model}")
        logger.info(f"🎯 Object: {response.object}")
        logger.info(f"⏰ Created: {datetime.fromtimestamp(response.created)}")
        
        # Usage tokens
        if response.usage:
            logger.info(f"\n💰 USAGE TOKENS:")
            logger.info(f"  - Prompt tokens: {response.usage.prompt_tokens}")
            logger.info(f"  - Completion tokens: {response.usage.completion_tokens}")
            logger.info(f"  - Total tokens: {response.usage.total_tokens}")
        
        # Contenu de la réponse
        if response.choices:
            for i, choice in enumerate(response.choices):
                logger.info(f"\n📝 CHOICE {i}:")
                logger.info(f"  - Index: {choice.index}")
                logger.info(f"  - Finish reason: {choice.finish_reason}")
                logger.info(f"  - Message role: {choice.message.role}")
                logger.info(f"  - Message content:\n{choice.message.content}")
        
        logger.info("=" * 80)
        logger.info("✅ Audit terminé avec succès")
        
        return response
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ ERREUR DÉTECTÉE")
        logger.error("=" * 80)
        logger.error(f"Type d'erreur: {type(e).__name__}")
        logger.error(f"Message: {str(e)}")
        
        # Détails supplémentaires selon le type d'erreur
        if hasattr(e, 'status_code'):
            logger.error(f"Status code: {e.status_code}")
        if hasattr(e, 'response'):
            logger.error(f"Response: {e.response}")
        if hasattr(e, '__dict__'):
            logger.error(f"Attributs de l'erreur: {e.__dict__}")
        
        # Stack trace
        logger.exception("Stack trace complète:")
        
        logger.error("=" * 80)
        raise

if __name__ == "__main__":
    logger.info("🚀 Démarrage de l'audit Mistral API")
    audit_mistral_call()