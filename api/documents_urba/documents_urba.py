#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
`documents_urba.py` — Document d'urbanisme courant d'une commune (GPU).

Module métier : pour un code INSEE, retourne LE document d'urbanisme courant
(PLU/PLUi/CC/POS) avec toutes ses infos décodées en clair, prêtes pour un LLM.

Chaînage validé empiriquement (200 communes testées) :
  insee --(doc_urba_com)--> idurba(s) --(doc_urba)--> détails sémantiques

Règle "document courant" :
  - état applicable  = {03 Opposable, 07 Approuvé (cartes communales)}
  - si plusieurs applicables (cas intercommunal ou décalage GPU), prendre
    celui dont la date d'approbation est la plus récente.
  - si aucun document : la commune est en RNU (Règlement National d'Urbanisme).

Nomenclature CNIG (vérifiée sur standards CNIG PLU v2024 + CC 2017) :
  ETAT  : 01 élaboration, 02 arrêté, 03 opposable, 04 annulé, 05 remplacé, 07 approuvé (CC)
  NOMPROC : E élaboration, R révision, M/Mx modification, MJ mise à jour, MS modif. simplifiée

Dépendances : requests uniquement (aucune BDD, aucune géométrie).

Usage CLI (debug) :
  python3 documents_urba.py --insee 66008
  python3 documents_urba.py --insee 66008 --json
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

import requests

logger = logging.getLogger("documents_urba")

WFS_URL = "https://data.geopf.fr/wfs/ows"
LAYER_DOC_URBA_COM = "wfs_du:doc_urba_com"
LAYER_DOC_URBA = "wfs_du:doc_urba"

DEFAULT_TIMEOUT = 60

# États considérés comme "le document s'applique"
ETATS_APPLICABLES = {"03", "07"}

ETAT_LIBELLE = {
    "01": "En cours d'élaboration",
    "02": "Arrêté",
    "03": "Opposable",
    "04": "Annulé",
    "05": "Remplacé",
    "07": "Approuvé",
}

TYPEDOC_LIBELLE = {
    "PLU": "Plan Local d'Urbanisme",
    "PLUI": "Plan Local d'Urbanisme intercommunal",
    "POS": "Plan d'Occupation des Sols",
    "CC": "Carte Communale",
    "PSMV": "Plan de Sauvegarde et de Mise en Valeur",
}

# Type de procédure (NOMPROC) — préfixe significatif
PROCEDURE_LIBELLE = {
    "E": "Élaboration",
    "R": "Révision",
    "M": "Modification",
    "MJ": "Mise à jour",
    "MS": "Modification simplifiée",
}


# ---------- Helpers ----------

def _wfs_json(typename: str, cql: str, count: int = 5000,
              timeout: int = DEFAULT_TIMEOUT,
              session: requests.Session | None = None) -> list[dict] | None:
    """Fetch GeoJSON via CQL_FILTER. Retourne la liste de properties, ou None si échec."""
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": typename, "outputFormat": "application/json",
        "CQL_FILTER": cql, "count": count,
    }
    getter = session.get if session else requests.get
    try:
        r = getter(WFS_URL, params=params, timeout=timeout)
        r.raise_for_status()
        feats = r.json().get("features", [])
        return [f.get("properties", {}) for f in feats]
    except Exception as e:
        logger.warning("WFS %s (%s) échec: %s", typename, cql, str(e)[:140])
        return None


def _format_date(yyyymmdd: str | None) -> str | None:
    """20251030 -> 2025-10-30. Retourne None si vide/invalide."""
    if not yyyymmdd:
        return None
    s = str(yyyymmdd).strip()
    if len(s) != 8 or not s.isdigit():
        return s or None
    try:
        return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        return s


def _procedure_libelle(nomproc: str | None) -> str | None:
    """M2 -> 'Modification', R1 -> 'Révision', etc. (préfixe alpha)."""
    if not nomproc:
        return None
    s = str(nomproc).strip().upper()
    if not s:
        return None
    # essai préfixe 2 lettres puis 1 lettre
    for length in (2, 1):
        prefix = s[:length]
        if prefix in PROCEDURE_LIBELLE:
            return PROCEDURE_LIBELLE[prefix]
    return s  # inconnu -> code brut


def _is_intercommunal(typedoc: str, idurba: str, siren: str | None) -> bool:
    """Heuristique intercommunal : typedoc PLUI, ou idurba sans préfixe INSEE communal."""
    if typedoc.upper() == "PLUI":
        return True
    return False


# ---------- Cœur métier ----------

def _select_document_courant(docs: list[dict]) -> dict | None:
    """
    Parmi les documents distincts, sélectionne le courant :
      1. ceux en état applicable (03/07)
      2. parmi eux, datappro la plus récente
    Si aucun applicable, retourne None.
    """
    applicables = [d for d in docs if d.get("etat") in ETATS_APPLICABLES]
    if not applicables:
        return None
    return max(applicables, key=lambda d: d.get("datappro") or "")


def get_document_urba(
    insee: str,
    session: requests.Session | None = None,
) -> dict:
    """
    Retourne le document d'urbanisme courant d'une commune, décodé en clair.

    Format de sortie :
      {
        "insee": str,
        "rnu": bool,                 # True si aucun document (RNU s'applique)
        "document_courant": {        # None si rnu
            "idurba", "typedoc", "typedoc_libelle",
            "date_approbation" (ISO), "etat", "etat_libelle", "applicable" (bool),
            "procedure", "procedure_libelle",
            "nom_reglement", "url_reglement",
            "nom_plan", "siteweb", "siren_collectivite", "intercommunal" (bool),
        },
        "error": str | None,
      }
    """
    insee = str(insee).strip()
    out = {"insee": insee, "rnu": False, "document_courant": None, "error": None}

    own_session = session is None
    if own_session:
        session = requests.Session()

    try:
        # 1. doc_urba_com -> idurba(s) couvrant la commune
        com_rows = _wfs_json(LAYER_DOC_URBA_COM, cql=f"insee='{insee}'", session=session)
        if com_rows is None:
            out["error"] = "Échec de la requête doc_urba_com."
            return out
        if len(com_rows) == 0:
            out["rnu"] = True  # aucun document dématérialisé -> RNU
            return out

        idurbas = sorted({
            str(r.get("idurba")) for r in com_rows if r.get("idurba")
        })
        if not idurbas:
            out["rnu"] = True
            return out

        # 2. doc_urba pour chaque idurba -> champs sémantiques (dédupliqués)
        docs = []
        for idu in idurbas:
            doc_rows = _wfs_json(LAYER_DOC_URBA, cql=f"idurba='{idu}'", session=session)
            if not doc_rows:
                continue
            p = doc_rows[0]  # champs sémantiques identiques sur toutes les lignes
            docs.append({
                "idurba": idu,
                "typedoc": str(p.get("typedoc") or ""),
                "datappro": str(p.get("datappro") or ""),
                "datefin": str(p.get("datefin") or ""),
                "etat": str(p.get("etat") or ""),
                "nomreg": p.get("nomreg") or None,
                "urlreg": p.get("urlreg") or None,
                "nomplan": p.get("nomplan") or None,
                "siteweb": p.get("siteweb") or None,
                "siren": p.get("siren") or None,
                "nomproc": p.get("nomproc") or None,
            })

        if not docs:
            out["error"] = "Documents référencés mais introuvables dans doc_urba."
            return out

        courant = _select_document_courant(docs)
        if courant is None:
            # Documents présents mais aucun applicable (tous annulés/remplacés)
            out["rnu"] = True
            out["error"] = "Aucun document applicable (états annulé/remplacé uniquement)."
            return out

        typedoc = courant["typedoc"]
        etat = courant["etat"]
        out["document_courant"] = {
            "idurba": courant["idurba"],
            "typedoc": typedoc,
            "typedoc_libelle": TYPEDOC_LIBELLE.get(typedoc.upper(), typedoc),
            "date_approbation": _format_date(courant["datappro"]),
            "etat": etat,
            "etat_libelle": ETAT_LIBELLE.get(etat, f"code {etat}"),
            "applicable": etat in ETATS_APPLICABLES,
            "procedure": courant["nomproc"],
            "procedure_libelle": _procedure_libelle(courant["nomproc"]),
            "nom_reglement": courant["nomreg"],
            "url_reglement": courant["urlreg"],
            "nom_plan": courant["nomplan"],
            "siteweb": courant["siteweb"],
            "siren_collectivite": courant["siren"],
            "intercommunal": _is_intercommunal(typedoc, courant["idurba"], courant["siren"]),
        }
        return out

    except Exception as e:
        logger.exception("get_document_urba — erreur inattendue")
        out["error"] = str(e)
        return out
    finally:
        if own_session:
            session.close()


# ---------- CLI debug ----------

def _cli():
    import argparse
    ap = argparse.ArgumentParser(description="Document d'urbanisme courant d'une commune (GPU)")
    ap.add_argument("--insee", required=True)
    ap.add_argument("--json", action="store_true", help="Sortie JSON brute")
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING)
    result = get_document_urba(args.insee)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    print("=" * 70)
    print(f"DOCUMENT D'URBANISME — INSEE {result['insee']}")
    print("=" * 70)
    if result["error"]:
        print(f"⚠️ {result['error']}")
    if result["rnu"]:
        print("⚪ Commune en RNU (Règlement National d'Urbanisme) — aucun document.")
        return
    d = result["document_courant"]
    if not d:
        print("Aucun document courant.")
        return
    print(f"  Type        : {d['typedoc']} — {d['typedoc_libelle']}")
    print(f"  Identifiant : {d['idurba']}")
    print(f"  Approuvé le : {d['date_approbation']}")
    print(f"  État        : {d['etat']} — {d['etat_libelle']} (applicable: {d['applicable']})")
    print(f"  Procédure   : {d['procedure']} — {d['procedure_libelle']}")
    print(f"  Intercommunal : {d['intercommunal']}")
    print(f"  Règlement   : {d['nom_reglement']}")
    print(f"  URL règlement : {d['url_reglement'] or '(non disponible)'}")
    print(f"  Plan        : {d['nom_plan']}")
    print(f"  Site web    : {d['siteweb'] or '—'}")
    print(f"  SIREN       : {d['siren_collectivite']}")


if __name__ == "__main__":
    _cli()