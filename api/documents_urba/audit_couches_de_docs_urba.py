#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
`audit_doc_urba_v2.py` — Audit analytique des couches documentaires GPU.

Répond à deux questions précises :
  Q1. Pourquoi une commune remonte-t-elle N lignes dans doc_urba_com ?
      (hypothèse : `partition` = lot de livraison, pas une autre commune)
  Q2. Combien de documents DISTINCTS (idurba) pour la commune, et lequel
      est le document courant applicable ?

Méthode :
  1. doc_urba_com filtré insee -> agrégation par idurba (compte les partitions)
  2. vérifie que insee est constant (sinon l'hypothèse "communes voisines" tiendrait)
  3. doc_urba par idurba -> dédoublonne et montre typedoc / datappro / etat
  4. synthèse : tableau des documents distincts triés par date d'approbation

Usage :
  python3 audit_doc_urba_v2.py --insee 66008
  python3 audit_doc_urba_v2.py --insee 66008 --partitions   # détail partitions
"""

import sys
import json
import argparse
from collections import defaultdict
from io import BytesIO

import requests

WFS_URL = "https://data.geopf.fr/wfs/ows"

# Décodage CNIG (codes officiels standard PLU/SUP GPU)
ETAT_CNIG = {
    "01": "En projet / élaboration",
    "02": "Arrêté",
    "03": "Applicable / approuvé",
    "04": "Abrogé / annulé",
    "05": "Caduc",
    "06": "Remplacé",
}
TYPEDOC_CNIG = {
    "PLU": "Plan Local d'Urbanisme",
    "PLUI": "Plan Local d'Urbanisme intercommunal",
    "POS": "Plan d'Occupation des Sols",
    "CC": "Carte Communale",
    "PSMV": "Plan de Sauvegarde et de Mise en Valeur",
}


def wfs_json(typename: str, cql: str | None = None, count: int = 5000, timeout: int = 90):
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": typename, "outputFormat": "application/json", "count": count,
    }
    if cql:
        params["CQL_FILTER"] = cql
    try:
        r = requests.get(WFS_URL, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json().get("features", [])
    except Exception as e:
        print(f"  ❌ Erreur fetch {typename}: {str(e)[:160]}")
        return None


def props_of(features):
    return [f.get("properties", {}) for f in features]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--insee", required=True)
    ap.add_argument("--partitions", action="store_true", help="Détailler les partitions par idurba")
    args = ap.parse_args()
    insee = args.insee.strip()

    print("=" * 84)
    print(f"AUDIT ANALYTIQUE DOCUMENTS D'URBANISME — INSEE {insee}")
    print("=" * 84)

    # ── Étape 1 : doc_urba_com ────────────────────────────────────────────
    print(f"\n1. doc_urba_com (filtre insee='{insee}')")
    print("─" * 84)
    feats = wfs_json("wfs_du:doc_urba_com", cql=f"insee='{insee}'")
    if not feats:
        print("  ⚠️ Aucune entité (commune en RNU ou sans document dématérialisé)")
        return
    rows = props_of(feats)
    print(f"  {len(rows)} ligne(s) brute(s)")

    # Q1 : insee est-il constant ? (si oui -> pas des communes voisines)
    insee_vals = set(str(r.get("insee")) for r in rows)
    print(f"\n  Q1 — Valeurs distinctes de 'insee' dans ces lignes : {insee_vals}")
    if insee_vals == {insee}:
        print(f"       → insee TOUJOURS = {insee}. Donc ce ne sont PAS des communes voisines.")
        print(f"       → La multiplicité vient d'ailleurs (partition / versions).")
    else:
        print(f"       → ⚠️ insee varie ! Hypothèse communes voisines à reconsidérer.")

    # Agrégation par idurba : combien de partitions par document ?
    by_idurba = defaultdict(list)
    for r in rows:
        by_idurba[str(r.get("idurba"))].append(r)

    print(f"\n  Agrégation par idurba ({len(by_idurba)} document(s) distinct(s)) :")
    for idu, group in by_idurba.items():
        partitions = sorted(set(str(g.get("partition")) for g in group))
        print(f"    • {idu:<24} : {len(group)} ligne(s), {len(partitions)} partition(s)")
        if args.partitions:
            print(f"        partitions: {', '.join(partitions)}")

    print(f"\n  Q1 (suite) — 'partition' = identifiant du LOT DE LIVRAISON GPU,")
    print(f"       pas un code commune. Un même document (idurba) peut apparaître")
    print(f"       dans plusieurs lots (dépôts/corrections/migrations successifs).")

    # ── Étape 2 : doc_urba par idurba ─────────────────────────────────────
    print(f"\n2. doc_urba (détail sémantique par idurba)")
    print("─" * 84)
    docs_summary = []
    for idu in by_idurba:
        feats_doc = wfs_json("wfs_du:doc_urba", cql=f"idurba='{idu}'")
        if not feats_doc:
            print(f"  ▸ {idu}: aucune entité dans doc_urba")
            continue
        doc_rows = props_of(feats_doc)

        # Dédoublonnage : les champs sémantiques (typedoc/datappro/etat) sont
        # identiques sur toutes les lignes d'un même idurba -> on prend la 1ère
        first = doc_rows[0]
        etat = str(first.get("etat") or "")
        typedoc = str(first.get("typedoc") or "")
        docs_summary.append({
            "idurba": idu,
            "typedoc": typedoc,
            "typedoc_libelle": TYPEDOC_CNIG.get(typedoc.upper(), typedoc),
            "datappro": str(first.get("datappro") or ""),
            "datefin": str(first.get("datefin") or ""),
            "etat": etat,
            "etat_libelle": ETAT_CNIG.get(etat, f"code {etat}"),
            "nomreg": first.get("nomreg"),
            "urlreg": first.get("urlreg"),
            "siteweb": first.get("siteweb"),
            "n_lignes": len(doc_rows),
        })
        print(f"  ▸ {idu}")
        print(f"      typedoc={typedoc} ({TYPEDOC_CNIG.get(typedoc.upper(), '?')})")
        print(f"      datappro={first.get('datappro')} | etat={etat} ({ETAT_CNIG.get(etat, '?')})")
        print(f"      nomreg={first.get('nomreg')}")
        urlreg = first.get("urlreg")
        print(f"      urlreg={'(vide)' if not urlreg else urlreg[:60] + '...'}")
        print(f"      {len(doc_rows)} ligne(s) dans doc_urba (identiques sur les champs sémantiques)")

    # ── Synthèse ──────────────────────────────────────────────────────────
    print(f"\n3. SYNTHÈSE — documents distincts triés par date d'approbation")
    print("─" * 84)
    docs_summary.sort(key=lambda d: d["datappro"], reverse=True)
    print(f"  {'IDURBA':<24} {'TYPE':<6} {'APPRO':<10} {'ÉTAT':<24} {'RÈGLEMENT URL'}")
    print("  " + "─" * 80)
    for d in docs_summary:
        has_url = "oui" if d["urlreg"] else "non"
        print(f"  {d['idurba']:<24} {d['typedoc']:<6} {d['datappro']:<10} "
              f"{d['etat_libelle']:<24} {has_url}")

    # Q2 : quel est le document courant ?
    applicables = [d for d in docs_summary if d["etat"] == "03"]
    print(f"\n  Q2 — Documents en état 'Applicable' (etat=03) : {len(applicables)}")
    if len(applicables) == 1:
        d = applicables[0]
        print(f"       → Un seul document courant : {d['idurba']} ({d['datappro']})")
    elif len(applicables) > 1:
        most_recent = max(applicables, key=lambda d: d["datappro"])
        print(f"       → ⚠️ {len(applicables)} documents 'applicables' simultanément.")
        print(f"          Le plus récent par datappro : {most_recent['idurba']} ({most_recent['datappro']})")
        print(f"          Probable décalage GPU : l'ancien doc n'a pas encore été basculé")
        print(f"          en état 'remplacé/abrogé'. Régle pratique : prendre le datappro max.")
    else:
        print(f"       → Aucun document explicitement applicable (vérifier les états).")

    print("\n" + "=" * 84)
    print("🏁 FIN")


if __name__ == "__main__":
    main()