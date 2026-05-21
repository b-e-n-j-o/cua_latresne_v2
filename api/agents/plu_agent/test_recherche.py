#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_recherche.py — valide les 2 tools de recherche HORS agent.

But : isoler "le tool fonctionne" (embedding requête + RRF + SQL) de
"Gemini route bien". À lancer depuis la racine plu_agent/ :

    python -m test_recherche
    (ou : python test_recherche.py si les imports relatifs passent)

Vérifie :
  - connexion DB OK
  - embedding de requête Gemini OK (768d normalisé)
  - recherche hybride RRF renvoie du pertinent
  - lookup par numéro tolère les variantes d'écriture (L421-6, L. 421-6...)
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Réutilise EXACTEMENT la config DB de l'API
try:
    from _env import DB_CONFIG
except ImportError:
    DB_CONFIG = {
        "host": os.environ["SUPABASE_HOST"],
        "port": int(os.environ.get("SUPABASE_PORT", 5432)),
        "dbname": os.environ["SUPABASE_DB"],
        "user": os.environ["SUPABASE_USER"],
        "password": os.environ["SUPABASE_PASSWORD"],
        "sslmode": "require",
        "connect_timeout": 15,
    }

from tools.recherche_articles import (
    search_articles_urbanisme,
    get_article_urbanisme_by_num,
)

SEP = "=" * 78


def _short(txt: str, n: int = 90) -> str:
    txt = (txt or "").replace("\n", " ").strip()
    return txt[:n] + ("…" if len(txt) > n else "")


def test_hybride():
    print(f"\n{SEP}\nTEST 1 — Recherche hybride (search_articles_urbanisme)\n{SEP}")
    requetes = [
        "Qu'est-ce qu'un emplacement réservé et qui peut en demander la levée ?",
        "Comment fonctionne le sursis à statuer ?",
        "Règles de mixité sociale dans les zones urbaines",
        "Procédure d'évaluation environnementale d'un PLU",
        "Droit de préemption urbain sur les fonds de commerce",
    ]
    for q in requetes:
        print(f"\n▸ « {q} »")
        res = search_articles_urbanisme(DB_CONFIG, query=q, top_k=3)
        if res["error"]:
            print(f"  ❌ ERREUR : {res['error']}")
            continue
        if res["count"] == 0:
            print("  ⚠️  Aucun résultat")
            continue
        for a in res["articles"]:
            print(f"  • {a.get('num', '?'):<12} (rrf={a.get('rrf_score')}) — {_short(a.get('resume') or a.get('text_clean'))}")


def test_lookup():
    print(f"\n{SEP}\nTEST 2 — Lookup par numéro (variantes d'écriture)\n{SEP}")
    # Même article écrit de 4 façons → doit renvoyer le MÊME résultat
    variantes = ["L421-6", "L. 421-6", "l 421 6", "L.421-6"]
    print("\n▸ Variantes d'écriture du même numéro :")
    for v in variantes:
        res = get_article_urbanisme_by_num(DB_CONFIG, num=v)
        if res["error"]:
            print(f"  {v:<12} → ❌ {res['error']}")
        elif res["count"] == 0:
            print(f"  {v:<12} → ⚠️  introuvable")
        else:
            a = res["articles"][0]
            print(f"  {v:<12} → ✓ {a.get('num')} : {_short(a.get('text_clean'), 60)}")

    # Un numéro qui existe à coup sûr dans ton corpus (vu dans tes données)
    print("\n▸ Article connu (L123-35, vu dans l'échantillon) :")
    res = get_article_urbanisme_by_num(DB_CONFIG, num="L123-35")
    if res["count"]:
        a = res["articles"][0]
        print(f"  ✓ {a.get('num')} — {_short(a.get('text_clean'), 120)}")
    else:
        print(f"  ⚠️  introuvable (error={res['error']})")


def test_chainage():
    print(f"\n{SEP}\nTEST 3 — Simulation chaînage (renvoi inter-articles)\n{SEP}")
    print("\n▸ search → on récupère un article qui en cite un autre → lookup du cité")
    res = search_articles_urbanisme(DB_CONFIG, query="programme d'action gestion forestière", top_k=1)
    if res["count"]:
        a = res["articles"][0]
        print(f"  search a trouvé : {a.get('num')}")
        txt = a.get("text_clean") or ""
        # Cherche une référence type 'L. 123-33' dans le texte
        import re
        refs = re.findall(r"[LR]\.?\s?\d+-\d+", txt)
        if refs:
            cible = refs[0]
            print(f"  référence détectée dans le texte : {cible}")
            r2 = get_article_urbanisme_by_num(DB_CONFIG, num=cible)
            if r2["count"]:
                print(f"  ✓ lookup du cité OK : {r2['articles'][0].get('num')}")
            else:
                print(f"  ⚠️  cité introuvable (peut être hors corpus urbanisme)")
        else:
            print("  (pas de référence inter-article dans ce résultat)")


if __name__ == "__main__":
    print(f"DB host : {DB_CONFIG.get('host')}  |  GEMINI_API_KEY : {'✓' if os.getenv('GEMINI_API_KEY') else '✗ MANQUANT'}")
    test_hybride()
    test_lookup()
    test_chainage()
    print(f"\n{SEP}\nFin des tests.\n{SEP}")