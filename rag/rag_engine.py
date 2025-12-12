# moteur_rag_simple.py
# Version simplifiée du moteur RAG sans gestion d'historique
# Focus uniquement sur la recherche et génération de réponses

import os
import time
from typing import List, Dict, Optional
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()


class SimpleLegalRAG:
    """
    Moteur RAG simplifié pour codes juridiques.
    
    Features:
    - Recherche vectorielle d'articles
    - Génération de réponses avec citations
    - Multi-codes (urbanisme, construction, environnement)
    - Logs détaillés
    """
    
    TABLES = {
        "urbanisme": "urbanisme_articles_embeddings",
        "construction": "construction_articles_embeddings",
        "environnement": "environnement_articles_embeddings"
    }
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        
        if self.verbose:
            print("🔧 Initialisation du moteur RAG...")
            start = time.time()
        
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SERVICE_KEY")
        )
        
        if self.verbose:
            print(f"   ✅ Connexion Supabase établie")
        
        self.embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        if self.verbose:
            print(f"   ✅ Embeddings model: text-embedding-3-small")
        
        self.llm = ChatOpenAI(
            model="gpt-4.1-nano",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        if self.verbose:
            print(f"   ✅ LLM model: gpt-4.1-nano")
        
        # Chaîne RAG principale
        self.rag_chain = self._build_rag_chain()
        
        if self.verbose:
            elapsed = time.time() - start
            print(f"✅ Moteur RAG initialisé en {elapsed:.2f}s\n")
    
    def _build_rag_chain(self):
        """Chaîne RAG avec citations"""
        rag_prompt = ChatPromptTemplate.from_messages([
            ("system", """Tu es un assistant juridique expert en droit français de l'urbanisme, construction et environnement.

RÈGLES STRICTES:
1. Base ta réponse UNIQUEMENT sur les articles fournis dans le contexte
2. Cite systématiquement avec [numéro] après chaque affirmation juridique
3. Si l'information n'est pas dans les articles fournis, dis "Je n'ai pas trouvé cette information dans les articles disponibles"
4. Utilise un langage juridique précis mais accessible
5. Structure ta réponse clairement

Format attendu:
- Réponse directe et synthétique
- Citations [1][2] après chaque référence
- Si pertinent, mentionne la procédure ou les implications"""),
            ("human", """Question: {input}

Articles juridiques de référence:
{context}

Réponds en citant précisément les articles [numéro].""")
        ])
        
        return rag_prompt | self.llm | StrOutputParser()
    
    def _reformulate_query(self, query: str) -> str:
        """LLM reformule la query pour matcher le format embeddings"""
        
        reformulation_prompt = ChatPromptTemplate.from_messages([
            ("system", """Tu es un expert en recherche juridique française.

Ton rôle : transformer une question en mots-clés optimisés pour recherche vectorielle.

Format des articles indexés :
- Texte intégral de l'article
- Résumé explicatif  
- 3 questions types auxquelles l'article répond

Reformulation :
1. Extraire concepts juridiques clés
2. Ajouter termes techniques précis (ex: "ICPE" → "installation classée nomenclature autorisation")
3. Inclure numéros d'articles si pertinents (L511, R421, etc.)
4. Mentionner procédures associées
5. Max 50 mots

Exemples :
- "Qu'est-ce qu'une ICPE ?" → "installation classée protection environnement ICPE nomenclature soumises autorisation enregistrement déclaration dangers inconvénients L511"
- "Quand permis pour extension ?" → "permis construire extension surface 20m² déclaration préalable R421 travaux modification"

Réponds UNIQUEMENT avec les mots-clés."""),
            ("human", "{query}")
        ])
        
        chain = reformulation_prompt | self.llm | StrOutputParser()
        reformulated = chain.invoke({"query": query})
        
        if self.verbose:
            print(f"🔄 Reformulation LLM:")
            print(f"   Original : {query}")
            print(f"   Optimisée: {reformulated}\n")
        
        return reformulated
    
    def _search_articles(
        self,
        query: str,
        codes: Optional[List[str]] = None,
        top_k: int = 5
    ) -> tuple[List[Dict], str]:
        """
        Recherche articles et retourne (articles, context_formatted)
        """
        if self.verbose:
            print(f"🔍 Recherche d'articles...")
            print(f"   Query: {query[:100]}{'...' if len(query) > 100 else ''}")
            search_start = time.time()
        
        # Reformulation de la query via LLM
        optimized_query = self._reformulate_query(query)
        
        # Génération embedding avec query optimisée
        if self.verbose:
            print(f"   📊 Génération embedding...")
            embed_start = time.time()
        
        query_embedding = self.embeddings.embed_query(optimized_query)
        
        if self.verbose:
            embed_time = time.time() - embed_start
            print(f"   ✅ Embedding généré ({len(query_embedding)} dims) en {embed_time:.2f}s")
        
        target_codes = codes or list(self.TABLES.keys())
        
        if self.verbose:
            print(f"   📚 Codes à interroger: {', '.join(target_codes)}")
        
        all_articles = []
        
        for code in target_codes:
            table = self.TABLES[code]
            
            if self.verbose:
                print(f"   🔎 Recherche dans {code} ({table})...")
                code_start = time.time()
            
            try:
                result = self.supabase.rpc(
                    'match_articles',
                    {
                        'query_embedding': query_embedding,
                        'match_threshold': 0.5,
                        'match_count': top_k,
                        'table_name': table
                    }
                ).execute()
                
                code_articles = []
                for row in result.data:
                    article = {
                        "article_id": row['article_id'],
                        "code": code,
                        "title": row['title'],
                        "path_title": row['path_title'],
                        "text_clean": row['text_clean'],
                        "resume": row['resume'],
                        "similarity": 1 / (1 + row['similarity'])
                    }
                    all_articles.append(article)
                    code_articles.append(article)
                
                if self.verbose:
                    code_time = time.time() - code_start
                    print(f"      ✅ {len(code_articles)} articles trouvés en {code_time:.2f}s")
                    if code_articles:
                        scores = [f"{a['similarity']:.3f}" for a in code_articles[:3]]
                        print(f"      📊 Top scores: {', '.join(scores)}")
            except Exception as e:
                if self.verbose:
                    print(f"      ❌ Erreur lors de la recherche dans {code}: {e}")
        
        # Tri par pertinence
        all_articles.sort(key=lambda x: x['similarity'], reverse=True)
        top_articles = all_articles[:top_k]
        
        if self.verbose:
            search_time = time.time() - search_start
            print(f"   ✅ {len(top_articles)} articles sélectionnés sur {len(all_articles)} trouvés (en {search_time:.2f}s)")
            scores_list = [f"{a['similarity']:.3f}" for a in top_articles[:5]]
            print(f"   📊 Scores finaux: {', '.join(scores_list)}")
        
        # Format contexte pour LLM
        context_parts = []
        for idx, article in enumerate(top_articles, 1):
            context_parts.append(
                f"[{idx}] {article['title']} (Code {article['code']})\n"
                f"Chemin: {article['path_title']}\n"
                f"Résumé: {article['resume']}\n"
                f"Texte: {article['text_clean'][:600]}...\n"
            )
        
        context = "\n---\n".join(context_parts)
        
        if self.verbose:
            context_size = len(context)
            print(f"   📝 Contexte formaté: {context_size} caractères ({len(top_articles)} articles)\n")
        
        return top_articles, context
    
    def ask(
        self,
        question: str,
        codes: Optional[List[str]] = None,
        top_k: int = 5
    ) -> Dict[str, any]:
        """
        Pose une question et obtient une réponse via RAG.
        
        Args:
            question: Question à poser
            codes: Codes à interroger (None = tous)
            top_k: Nombre d'articles à récupérer
        
        Returns:
            {
                "response": "Réponse avec citations",
                "sources": [{"id": 1, "article": {...}}],
                "question": "Question posée"
            }
        """
        total_start = time.time()
        
        if self.verbose:
            print("=" * 80)
            print(f"💬 NOUVELLE QUESTION")
            print(f"   Question: {question}")
            print("=" * 80)
            print()
        
        # 1. Recherche RAG
        articles, context = self._search_articles(
            question,
            codes=codes,
            top_k=top_k
        )
        
        # 2. Génération réponse
        if self.verbose:
            print("🤖 Génération de la réponse...")
            gen_start = time.time()
        
        response = self.rag_chain.invoke({
            "input": question,
            "context": context
        })
        
        if self.verbose:
            gen_time = time.time() - gen_start
            response_len = len(response)
            print(f"   ✅ Réponse générée ({response_len} caractères) en {gen_time:.2f}s")
            print(f"   📝 Réponse: {response[:150]}{'...' if len(response) > 150 else ''}")
            print()
        
        # 3. Format sources
        sources = []
        for idx, article in enumerate(articles, 1):
            sources.append({
                "id": idx,
                "article_id": article["article_id"],
                "code": article["code"],
                "title": article["title"],
                "path": article["path_title"],
                "resume": article["resume"],
                "score": round(article["similarity"], 3)
            })
        
        if self.verbose:
            total_time = time.time() - total_start
            print(f"📊 RÉSUMÉ")
            print(f"   ⏱️  Temps total: {total_time:.2f}s")
            print(f"   📚 Sources: {len(sources)} articles")
            print(f"   📝 Réponse: {len(response)} caractères")
            print("=" * 80)
            print()
        
        return {
            "response": response,
            "sources": sources,
            "question": question
        }
    
    def search_only(
        self,
        query: str,
        codes: Optional[List[str]] = None,
        top_k: int = 10
    ) -> List[Dict]:
        """Recherche pure sans génération (pour explorer)"""
        if self.verbose:
            print("🔍 RECHERCHE UNIQUEMENT (sans génération)")
            print(f"   Query: {query}")
            print()
        
        articles, _ = self._search_articles(query, codes, top_k)
        
        if self.verbose:
            print(f"✅ {len(articles)} articles retournés\n")
        
        return articles


# ============================================================
# USAGE EXEMPLES
# ============================================================

if __name__ == "__main__":
    rag = SimpleLegalRAG(verbose=True)
    
    # Question 1
    print("\n" + "="*80)
    print("=== QUESTION 1 ===")
    print("="*80 + "\n")
    
    result1 = rag.ask(
        "Quand faut-il un permis de construire pour une extension ?",
        codes=["construction"]
    )
    
    print("\n📋 RÉSULTAT:")
    print(f"   Question: {result1['question']}")
    print(f"   Réponse: {result1['response']}")
    print(f"\n📚 Sources ({len(result1['sources'])} articles):")
    for src in result1['sources']:
        print(f"   [{src['id']}] {src['title']} (Code: {src['code']}, score: {src['score']:.3f})")
    print()
    
    # Question 2 - Tous les codes
    print("\n" + "="*80)
    print("=== QUESTION 2 ===")
    print("="*80 + "\n")
    
    result2 = rag.ask(
        "Quelles sont les règles concernant les installations classées pour la protection de l'environnement ?"
    )
    
    print("\n📋 RÉSULTAT:")
    print(f"   Question: {result2['question']}")
    print(f"   Réponse: {result2['response'][:300]}...")
    print(f"\n📚 Sources ({len(result2['sources'])} articles):")
    for src in result2['sources']:
        print(f"   [{src['id']}] {src['title']} (Code: {src['code']}, score: {src['score']:.3f})")
    print()
    
    # Recherche seule (sans génération)
    print("\n" + "="*80)
    print("=== RECHERCHE SEULE ===")
    print("="*80 + "\n")
    
    articles = rag.search_only(
        "permis de construire extension",
        codes=["construction"],
        top_k=5
    )
    
    print("\n📚 Articles trouvés:")
    for i, article in enumerate(articles, 1):
        print(f"\n[{i}] {article['title']}")
        print(f"    Code: {article['code']}")
        print(f"    Score: {article['similarity']:.3f}")
        print(f"    Chemin: {article['path_title']}")
        print(f"    Résumé: {article['resume'][:100]}...")

