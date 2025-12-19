# -*- coding: utf-8 -*-

"""
CAG PLU Engine
--------------
- Fetch des articles PLU depuis Supabase (table plu_embeddings)
- Filtrage heuristique par zonage si détectable
- Reconstruction du règlement
- Appel GPT-5-mini (reasoning=medium) en CAG
"""

from typing import Optional, List, Dict
import os
import logging

from supabase import create_client, Client

from utils.llm_openai import call_gpt_5

logger = logging.getLogger("cag_plu_engine")

# ============================================================
# CONFIG LLM
# ============================================================

MODEL_CAG_PLU = "gpt-5-mini"
REASONING_EFFORT = "medium"
MAX_OUTPUT_TOKENS = 5000

# ============================================================
# CONFIG SUPABASE
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SERVICE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

PLU_TABLE = "plu_embeddings"

# ============================================================
# PROMPT SYSTEM (VALIDÉ)
# ============================================================

SYSTEM_PROMPT_PLU = """
 Tu es un assistant spécialisé en analyse réglementaire des documents
 d’urbanisme (PLU).

 Tu analyses exclusivement le document fourni, sans utiliser de
 connaissances externes.

 Méthode d’analyse à respecter impérativement :

 1) Principe général
 - Toute occupation ou utilisation du sol est considérée comme interdite
   par défaut, sauf si le règlement étudié l’autorise explicitement.

 2) Autorisations
 - Une autorisation n’existe que si elle est formulée de manière explicite
   dans le règlement.
 - Une règle qui encadre une pratique (ex : conditions de stationnement,
   aspect des clôtures, implantation, etc.) n’est pas une autorisation
   en soi.

 3) Conditions
 - Toute autorisation doit être analysée avec l’ensemble de ses conditions.
 - Si plusieurs règles s’appliquent, elles sont cumulatives.

 4) Hiérarchie
 - Lorsqu’une règle générale et une règle particulière coexistent,
   la règle particulière prévaut dans son champ d’application.

 5) Interdictions
 - Lorsqu’une interdiction est formulée, elle s’applique sans exception,
   sauf mention explicite contraire dans le règlement.

 6) Formulation
 - Tu dois utiliser des formulations conditionnelles : “si … alors …”.
 - Tu ne dois jamais conclure par une autorisation sans en préciser
   toutes les conditions exactes.
 - En cas d’absence d’autorisation explicite, la conclusion doit être :
   “non autorisé selon le règlement analysé”.

 7) Références
 - Tu dois citer précisément les articles, sections ou paragraphes
   du règlement sur lesquels tu t’appuies.
 - Lorsque l’information est disponible dans le document analysé,
   tu dois également indiquer le numéro de page.

 FORMAT DE SORTIE OBLIGATOIRE :

 - Tu dois produire UNIQUEMENT un objet JSON valide.
 - Aucun texte avant ou après le JSON.
 - Les clés attendues sont STRICTEMENT :
   - "content" : string
   - "sources" : array

 - Chaque élément de "sources" doit être un objet avec AU MINIMUM :
   - "article" : string
   - "page" : number ou null si non identifiable

 - Si une information (ex : page) n’est pas clairement identifiable,
   tu dois utiliser la valeur null, mais tu ne dois jamais omettre la clé.

 - Si tu ne peux pas répondre correctement,
   retourne exactement :
   {
     "content": "",
     "sources": []
   }

 Exemple de sortie valide :

 {
   "content": "Analyse textuelle détaillée et conditionnelle…",
   "sources": [
     {
       "article": "Article N12.1",
       "page": 62
     },
     {
       "article": "Article N13.3",
       "page": 64
     }
   ]
 }

 N'hesite pas à ajouter des sources si tu le penses utile.
 """

# ============================================================
# ZONAGE HEURISTIQUE
# ============================================================

ZONES_KNOWN = {"UA", "UB", "UC", "UX", "UE", "N", "A", "1AU", "ALL"}

def detect_zonage_from_question(question: str) -> Optional[str]:
    """
    Détecte un zonage explicitement mentionné dans la question.
    Heuristique volontairement stricte.
    """
    q = question.upper()

    for zone in ZONES_KNOWN:
        if f"ZONE {zone}" in q or f"EN {zone}" in q or f" {zone} " in q:
            return zone

    return None


# ============================================================
# FETCH SUPABASE
# ============================================================

def fetch_plu_articles(
    *,
    insee: str,
    zonage: Optional[str] = None,
) -> List[Dict]:
    """
    Récupère les articles PLU depuis Supabase.
    - Filtre par INSEE
    - Filtre par zonage si fourni
    """

    query = (
        supabase
        .table(PLU_TABLE)
        .select("article, article_uid, title, text, zonage")
        .eq("insee", insee)
        .eq("document", "reglement")
        .order("article_uid")
    )

    if zonage:
        query = query.eq("zonage", zonage)

    res = query.execute()

    if not res.data:
        raise RuntimeError(
            f"Aucun article PLU trouvé (insee={insee}, zonage={zonage})"
        )

    return res.data


# ============================================================
# RECONSTRUCTION RÈGLEMENT
# ============================================================

def build_reglement_text(articles: List[Dict]) -> str:
    """
    Reconstruit un texte de règlement stable et lisible
    à partir des articles Supabase.
    """

    blocks = []

    for art in articles:
        header = f"### {art.get('article') or art['article_uid']}"
        if art.get("title"):
            header += f" – {art['title']}"

        body = art.get("text") or ""

        blocks.append(f"{header}\n{body}")

    return "\n\n".join(blocks)


# ============================================================
# ENGINE PRINCIPAL
# ============================================================

def run_cag_plu(
    *,
    question: str,
    commune_insee: str,
) -> str:
    """
    Exécute un CAG PLU complet :
    - détection zonage
    - fetch Supabase
    - reconstruction règlement
    - appel GPT-5-mini
    """

    # 1) Détection zonage (heuristique)
    zonage = detect_zonage_from_question(question)

    # 2) Fetch articles PLU
    articles = fetch_plu_articles(
        insee=commune_insee,
        zonage=zonage,
    )

    # 3) Reconstruction texte règlement
    reglement_text = build_reglement_text(articles)

    logger.info(
        "CAG ENGINE | zonage=%s | articles=%d | context_chars=%d",
        zonage or "ALL",
        len(articles),
        len(reglement_text),
    )

    # 4) Prompt utilisateur
    user_prompt = f"""
CONTEXTE (RÈGLEMENT PLU) :
<<<
{reglement_text}
>>>

QUESTION :
{question}
"""

    # 5) Appel CAG LLM
    result = call_gpt_5(
        model=MODEL_CAG_PLU,
        system_prompt=SYSTEM_PROMPT_PLU,
        user_prompt=user_prompt,
        reasoning_effort=REASONING_EFFORT,
        max_output_tokens=MAX_OUTPUT_TOKENS,
    )

    logger.info(
        "CAG ENGINE | output_chars=%d | preview=%s",
        len(result),
        result[:200].replace("\n", " "),
    )

    return result
