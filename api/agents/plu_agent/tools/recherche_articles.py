#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tools recherche d'articles — Code de l'urbanisme (vector + full-text).

Deux portes d'entrée dans le corpus (2.5k articles) :
    search_articles_urbanisme   → recherche HYBRIDE (sémantique + lexicale, RRF)
    get_article_urbanisme_by_num→ lookup EXACT par numéro d'article

Aligné sur le pattern de zonage.py : psycopg2, _query/RealDictCursor,
retours {..., "error": ...}, déclarations DECL_*.
"""

import os
import logging
from typing import List, Dict, Any

import numpy as np
import psycopg2
import psycopg2.extras
from google import genai
from google.genai import types

logger = logging.getLogger("plu_tools")

TABLE = "public.urbanisme_articles_embeddings"
EMBED_MODEL = "gemini-embedding-001"
EMBED_DIM = 768
RRF_K = 60          # constante RRF standard (amortit l'effet des tops)
CANDIDATES = 30     # candidats récupérés par branche avant fusion


# ── DB helpers (identiques à zonage.py) ─────────────────────
def _db_connect(db_config: dict):
    return psycopg2.connect(**db_config)


def _query(db_config: dict, sql: str, params: tuple) -> List[dict]:
    conn = _db_connect(db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ── Embedding de la requête ─────────────────────────────────
_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def _embed_query(text: str) -> List[float]:
    """Embedde la requête utilisateur (task_type=RETRIEVAL_QUERY) + normalise."""
    resp = _client.models.embed_content(
        model=EMBED_MODEL,
        contents=text,
        config=types.EmbedContentConfig(
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=EMBED_DIM,
        ),
    )
    arr = np.asarray(resp.embeddings[0].values, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm).tolist() if norm else arr.tolist()


# ── Projection commune (colonnes renvoyées au LLM) ──────────
# Pas d'embedding ni de fts : inutile au LLM, coûteux en tokens.
_COLS = "article_id, num, title, path_title, resume, text_clean"


# ============================================================
# TOOL 1 — Recherche hybride (RRF)
# ============================================================
def search_articles_urbanisme(
    db_config: dict,
    query: str,
    top_k: int = 5,
) -> dict:
    """
    Recherche hybride : fusionne une branche sémantique (vecteur, cosinus)
    et une branche lexicale (full-text français, ts_rank) via Reciprocal
    Rank Fusion. Retourne les top_k articles les plus pertinents.
    """
    if not query or not query.strip():
        return {"articles": [], "count": 0, "error": "Requête vide."}

    try:
        qvec = _embed_query(query)
    except Exception as e:
        logger.error("search_articles_urbanisme — embedding requête échoué : %s", e)
        return {"articles": [], "count": 0, "error": f"Embedding requête : {e}"}

    # — Branche sémantique : distance cosinus (<=>), plus petit = plus proche —
    sql_vec = f"""
        SELECT {_COLS}, (embedding <=> %s::vector) AS dist
        FROM {TABLE}
        WHERE embedding IS NOT NULL
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    # — Branche lexicale : full-text FR, websearch_to_tsquery tolère le langage naturel —
    sql_fts = f"""
        SELECT {_COLS}, ts_rank(fts, websearch_to_tsquery('french', %s)) AS rank
        FROM {TABLE}
        WHERE fts @@ websearch_to_tsquery('french', %s)
        ORDER BY rank DESC
        LIMIT %s
    """

    try:
        vec_rows = _query(db_config, sql_vec, (qvec, qvec, CANDIDATES))
        fts_rows = _query(db_config, sql_fts, (query, query, CANDIDATES))
    except Exception as e:
        logger.error("search_articles_urbanisme — SQL échoué : %s", e)
        return {"articles": [], "count": 0, "error": str(e)}

    # — Reciprocal Rank Fusion —
    #   score(doc) = Σ_branches 1 / (RRF_K + rang_dans_la_branche)
    scores: Dict[str, float] = {}
    store: Dict[str, dict] = {}

    for rank, row in enumerate(vec_rows, start=1):
        aid = row["article_id"]
        scores[aid] = scores.get(aid, 0.0) + 1.0 / (RRF_K + rank)
        store.setdefault(aid, row)

    for rank, row in enumerate(fts_rows, start=1):
        aid = row["article_id"]
        scores[aid] = scores.get(aid, 0.0) + 1.0 / (RRF_K + rank)
        store.setdefault(aid, row)

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top_k]

    articles = []
    for aid, score in ranked:
        row = store[aid]
        row.pop("dist", None)
        row.pop("rank", None)
        row["rrf_score"] = round(score, 5)
        articles.append(row)

    return {"articles": articles, "count": len(articles), "error": None}


# ============================================================
# TOOL 2 — Lookup exact par numéro
# ============================================================
def get_article_urbanisme_by_num(db_config: dict, num: str) -> dict:
    """
    Récupère un (ou plusieurs) article(s) par leur numéro exact.
    Tolère les variantes d'écriture : 'L123-35', 'L. 123-35', 'l123 35'.
    """
    if not num or not num.strip():
        return {"articles": [], "count": 0, "error": "Numéro vide."}

    # Normalisation : maj, suppression espaces et points → 'L. 123-35' == 'L123-35'
    norm = num.upper().replace(" ", "").replace(".", "")

    sql = f"""
        SELECT {_COLS}
        FROM {TABLE}
        WHERE upper(replace(replace(num, ' ', ''), '.', '')) = %s
        ORDER BY num
    """
    try:
        rows = _query(db_config, sql, (norm,))
        return {"articles": rows, "count": len(rows), "error": None}
    except Exception as e:
        logger.error("get_article_urbanisme_by_num — SQL échoué : %s", e)
        return {"articles": [], "count": 0, "error": str(e)}


# ============================================================
# DÉCLARATIONS LLM (pattern DECL_* de zonage.py)
# ============================================================
DECL_SEARCH_ARTICLES = types.FunctionDeclaration(
    name="search_articles_urbanisme",
    description=(
        "Recherche dans le Code de l'urbanisme (≈2500 articles) les articles "
        "les plus pertinents pour une question thématique en langage naturel. "
        "Combine recherche sémantique et lexicale (hybride). "
        "À utiliser pour toute question de fond sur la réglementation d'urbanisme "
        "qui n'est PAS liée à une parcelle précise : définitions, procédures, "
        "règles générales, droits et obligations, notions juridiques. "
        "Ex: « Quelles sont les règles sur les emplacements réservés ? », "
        "« Comment fonctionne le sursis à statuer ? ». "
        "Ne PAS utiliser pour citer un numéro d'article précis (→ get_article_urbanisme_by_num)."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "query": types.Schema(
                type=types.Type.STRING,
                description="Question ou thématique d'urbanisme en langage naturel.",
            ),
            "top_k": types.Schema(
                type=types.Type.INTEGER,
                description="Nombre d'articles à retourner (défaut 5, max conseillé 10).",
            ),
        },
        required=["query"],
    ),
)

DECL_GET_ARTICLE_BY_NUM = types.FunctionDeclaration(
    name="get_article_urbanisme_by_num",
    description=(
        "Récupère le texte exact d'un article du Code de l'urbanisme par son numéro. "
        "Tolère les variantes d'écriture (L123-35, L. 123-35). "
        "À utiliser dès qu'un numéro d'article précis est mentionné dans la question "
        "ou doit être vérifié mot pour mot."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "num": types.Schema(
                type=types.Type.STRING,
                description="Numéro d'article (ex: 'L123-35', 'R151-1').",
            ),
        },
        required=["num"],
    ),
)