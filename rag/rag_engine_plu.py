# backend/rag/rag_engine_plu.py

import os
import time
from typing import List, Dict, Optional, Any

from supabase import create_client, Client
from dotenv import load_dotenv

from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser

load_dotenv()


class PLURAGEngine:
    """
    Moteur RAG spécialisé PLU.
    Multi-communes via filtre INSEE.
    - insee: obligatoire (scope juridique)
    - document: optionnel (None => tous types de documents)
    """

    RPC_MATCH_FN = "match_plu_embeddings"

    def __init__(self, verbose: bool = True):
        self.verbose = verbose

        supabase_url = os.getenv("SUPABASE_URL")
        service_key = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not supabase_url or not service_key:
            raise RuntimeError("SUPABASE_URL / SERVICE_KEY manquants dans l'environnement")

        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            raise RuntimeError("OPENAI_API_KEY manquant dans l'environnement")

        self.supabase: Client = create_client(supabase_url, service_key)

        self.embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            openai_api_key=openai_key
        )

        # LLM pour la réponse PLU (tu peux le changer plus tard)
        self.llm = ChatOpenAI(
            model=os.getenv("RAG_PLU_MODEL", "gpt-4.1-nano"),
            openai_api_key=openai_key,
            temperature=0.2
        )

        self.rag_chain = self._build_rag_chain()

        if self.verbose:
            print("✅ PLU RAG Engine initialisé")

    # --------------------------------------------------
    # PROMPT
    # --------------------------------------------------

    def _build_rag_chain(self):
        prompt = ChatPromptTemplate.from_messages([
            ("system", """
Tu es un assistant expert en urbanisme réglementaire local. Voici des extraits issus d'un PLU pour t'aider à répondre.

RÈGLES STRICTES :
1) Tu réponds UNIQUEMENT à partir des extraits PLU fournis.
2) Si une information est absente, dis explicitement que le PLU ne la précise pas.
3) Cite les articles du PLU (ex : "article UX-11", "article UA11") et/ou les références présentes dans les extraits.
4) Reste clair, structuré, opérationnel.
""".strip()),
            ("human", """
Question :
{question}

Extraits PLU :
{context}

Réponds de manière juridique, précise et citée.
""".strip())
        ])

        return prompt | self.llm | StrOutputParser()

    # --------------------------------------------------
    # VECTOR SEARCH (Supabase RPC)
    # --------------------------------------------------

    def _vector_search(
        self,
        query_embedding: List[float],
        insee: str,
        top_k: int,
        document: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Appelle la fonction RPC Supabase `match_plu_embeddings`.
        - Filtre INSEE appliqué (obligatoire côté métier)
        - Filtre document optionnel (None => tous docs)
        """

        params = {
            "query_embedding": query_embedding,
            "match_count": int(top_k),
            "filter_insee": insee,
            "filter_document": document  # None => pas de filtre côté SQL
        }

        result = self.supabase.rpc(self.RPC_MATCH_FN, params).execute()

        # Supabase python renvoie parfois data=None
        return result.data or []

    # --------------------------------------------------
    # MAIN API
    # --------------------------------------------------

    def ask(
        self,
        question: str,
        insee: str,
        document: Optional[str] = None,
        top_k: int = 10,
        min_similarity: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Pose une question au RAG PLU.
        - insee obligatoire
        - document optionnel (None => tous docs)
        - min_similarity optionnel: filtre post-RPC si souhaité (désactivé par défaut)
        """

        start = time.time()

        q = (question or "").strip()
        if not q:
            raise ValueError("Question vide")

        if not insee or not str(insee).strip():
            raise ValueError("INSEE requis pour interroger un PLU")

        insee = str(insee).strip()
        document = (document.strip() if isinstance(document, str) and document.strip() else None)

        if self.verbose:
            print(f"\n🔍 PLU RAG | INSEE={insee} | document={document or 'ALL'} | top_k={top_k}")
            print(f"   Question : {q}")

        # 1) Embedding
        t0 = time.time()
        query_embedding = self.embeddings.embed_query(q)
        if self.verbose:
            print(f"   📊 Embedding généré en {time.time() - t0:.2f}s")

        # 2) Vector search (RPC)
        t1 = time.time()
        rows = self._vector_search(
            query_embedding=query_embedding,
            insee=insee,
            top_k=top_k,
            document=document
        )
        if self.verbose:
            print(f"   🔎 RPC match_plu_embeddings: {len(rows)} lignes en {time.time() - t1:.2f}s")

        if not rows:
            return {
                "question": q,
                "response": "Le PLU ne contient pas d'information permettant de répondre à cette question.",
                "sources": []
            }

        # 2bis) Optionnel : filtre par similarité (attention, dépend de tes distributions)
        # Note: ta RPC trie par distance croissante et renvoie similarity = 1 - distance (cohérent).
        if min_similarity is not None:
            try:
                rows = [r for r in rows if float(r.get("similarity", 0.0)) >= float(min_similarity)]
            except Exception:
                # si champ absent ou conversion impossible, on ne filtre pas
                pass

            if self.verbose:
                print(f"   🧪 Filtre min_similarity={min_similarity} => {len(rows)} lignes restantes")

            if not rows:
                return {
                    "question": q,
                    "response": "Le PLU ne contient pas d'information suffisamment proche pour répondre à cette question.",
                    "sources": []
                }

        # 3) Construire contexte + sources
        context_blocks: List[str] = []
        sources: List[Dict[str, Any]] = []

        # Log similaire : afficher quelques scores (utilise similarity tel que renvoyé par la RPC)
        if self.verbose:
            sims = []
            for r in rows[: min(5, len(rows))]:
                try:
                    sims.append(round(float(r.get("similarity", 0.0)), 3))
                except Exception:
                    pass
            if sims:
                print(f"   📈 Top similarity (≈): {', '.join(map(str, sims))}")

        for idx, r in enumerate(rows, 1):
            zonage = r.get("zonage") or "?"
            article = r.get("article") or "?"
            txt = (r.get("text") or "").strip()

            context_blocks.append(
                f"[{idx}] Commune {r.get('nom_commune', '')} ({r.get('insee', insee)})"
                f" – Document {r.get('document', 'PLU')}"
                f" – Zone {zonage} – Article {article}\n{txt}"
            )

            sources.append({
                "article_uid": r.get("article_uid"),
                "insee": r.get("insee"),
                "nom_commune": r.get("nom_commune"),
                "document": r.get("document"),
                "zonage": r.get("zonage"),
                "article": r.get("article"),
                "resume": r.get("resume"),
                "keywords": r.get("keywords"),
                "keyrules": r.get("keyrules"),
                "similarity": r.get("similarity")
            })

        context = "\n\n---\n\n".join(context_blocks)

        # 4) Génération réponse
        t2 = time.time()
        response = self.rag_chain.invoke({
            "question": q,
            "context": context
        })
        if self.verbose:
            print(f"✅ Réponse générée en {time.time() - t2:.2f}s (total {time.time() - start:.2f}s)")

        return {
            "question": q,
            "response": response,
            "sources": sources
        }
