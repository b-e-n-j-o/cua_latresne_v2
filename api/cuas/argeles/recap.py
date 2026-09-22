# -*- coding: utf-8 -*-
"""
recap.py — Synthèse des contraintes en tête du CUA (Argelès).

compute_recap(rapport) -> dict : pur, sans python-docx (réutilisable côté API,
carte d'identité, front). Le rendu DOCX reste dans builder.section_recap.

Principe : aucun texte réglementaire ici. Uniquement des libellés, des parts de
surface (base = surface SIG de l'UF) et un renvoi vers la section détaillée.

Chaque rubrique a trois états :
  - concerne      → liste des éléments
  - non_concerne  → affiché seulement pour les rubriques « toujours » (DPU, SUP, PPR…)
  - non_verifie   → couche/module en erreur ou table absente : toujours affiché
"""
from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

CONCERNE = "concerne"
NON_CONCERNE = "non_concerne"
NON_VERIFIE = "non_verifie"
STATUTS_KO = frozenset({"erreur", "table_absente"})
MAX_LEN_ITEM = 140

# Titres des barres du builder (colonne « Voir »)
R_ZONAGE = "Zonage du PLU"
R_HAUTEURS = "Hauteurs"
R_CES = "Emprise au sol (CES)"
NOTE_CES = (
    "Information issue du PLU uniquement. Elle peut être recoupée avec d'autres "
    "réglementations (PPR, prescriptions, etc.). Indication à titre informatif : "
    "il ne s'agit pas, pour le moment, d'une réglementation établie."
)
R_DPU = "Droit de préemption"
R_SUP = "Servitudes"
R_RISQUES = "Risques"
R_PRESC = "Prescriptions et informations"
R_NATURA = "Natura 2000 / Prairies"
R_ENEDIS = "Réseaux électriques"
R_TAXES = "Taxes"


# ============================================================
# HELPERS
# ============================================================
def _s(value) -> str:
    if value is None:
        return ""
    txt = str(value).strip()
    return "" if txt == "\\N" else txt


def _pct(obj: dict) -> float:
    try:
        return float(obj.get("pct_sig") or 0)
    except (TypeError, ValueError):
        return 0.0


def _num(value) -> str:
    """9.0 -> '9' ; 9.5 -> '9,5'."""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return _s(value)
    return f"{f:.0f}" if f.is_integer() else f"{f:.1f}".replace(".", ",")


def _val(value):
    """float ou None."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_pct(p: float) -> str:
    p = min(max(p, 0.0), 100.0)
    txt = f"{p:.0f}" if p >= 10 else f"{p:.1f}".replace(".", ",")
    return f"{txt}\u00a0%"


def fmt_num(v: float, dec: int = 0) -> str:
    return f"{v:,.{dec}f}".replace(",", "\u202f").replace(".", ",")


def _court(txt: str, n: int = MAX_LEN_ITEM) -> str:
    txt = " ".join(txt.split())
    return txt if len(txt) <= n else txt[: n - 1].rstrip() + "…"


def _cles(*keys: str) -> Callable[[dict], str]:
    def _f(obj: dict) -> str:
        for k in keys:
            v = _s(obj.get(k))
            if v:
                return v
        return ""
    return _f


def _distinct(values: Iterable) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        v = _s(v)
        if v and v.casefold() not in seen:
            seen.add(v.casefold())
            out.append(v)
    return out


def _par_libelle(objets: list, libelle: Callable[[dict], str]) -> list[str]:
    """Regroupe par libellé, somme des parts UF (plafonnée à 100 %), tri décroissant."""
    acc: dict[str, float] = {}
    for obj in objets or []:
        lab = _court(libelle(obj))
        if lab:
            acc[lab] = acc.get(lab, 0.0) + _pct(obj)
    ordre = sorted(acc.items(), key=lambda kv: -kv[1])
    return [f"{lab} — {fmt_pct(p)}" if p > 0 else lab for lab, p in ordre]


def _layer(inter: dict, key: str) -> dict:
    return inter.get(key) or {}


def _objets(inter: dict, key: str) -> list:
    return _layer(inter, key).get("objets") or []


def _is_dpu(obj: dict) -> bool:
    lib = _s(obj.get("libelle")).lower()
    return "préemption" in lib or "preemption" in lib


# ============================================================
# EXTRACTEURS (inter -> liste de libellés courts)
# ============================================================
def _ex_zonage(inter):
    return _par_libelle(_objets(inter, "zonage_plu"), _cles("libelle", "zonage_reglement"))


def _ex_ces(inter):
    """Une ligne par règlement de zone présent sur l'UF, avec sa règle de CES.
    Champs ces_* attendus sur chaque objet zonage_plu (jointure amont sur plu_ces).
    On n'affiche jamais un % en aveugle quand la règle est 'partielle' ou
    'voir_reglementation' : renvoi au détail.
    Chaque item porte aussi la citation PLU d'où vient la valeur."""
    acc: dict[str, tuple[str, str, float]] = {}
    for o in _objets(inter, "zonage_plu"):
        code = _s(o.get("zonage_reglement")) or _s(o.get("libelle"))
        if not code:
            continue
        tr = _s(o.get("ces_type_regle"))
        appli = _s(o.get("ces_applicabilite"))
        pct = _val(o.get("ces_max_pct"))
        m2 = _val(o.get("emprise_max_m2"))
        citation = " ".join(_s(o.get("ces_citation")).split())

        if not tr:
            regle = "non déterminé"
        elif tr == "non_reglemente":
            regle = "non réglementé"
        elif tr == "renvoi_graphique":
            regle = "voir document graphique"
        elif appli == "partielle" or tr == "voir_reglementation":
            base = f"{_num(pct)} %" if pct is not None else (
                f"{_num(m2)} m²" if m2 is not None else "")
            regle = (f"variable selon secteur — {base} selon les cas (voir détail)"
                     if base else "variable selon secteur (voir détail)")
        elif tr == "ratio" and pct is not None:
            regle = f"max {_num(pct)} %"
        elif tr == "absolu" and m2 is not None:
            regle = f"max {_num(m2)} m²"
        else:
            regle = "voir détail"

        if _s(o.get("ces_statut")) not in ("", "valide"):
            regle += " (à valider)"

        label = _court(f"{code} — CES {regle}")
        _lab, _cit, prev = acc.get(code, (label, citation, 0.0))
        acc[code] = (label, citation or _cit, prev + _pct(o))

    ordre = sorted(acc.values(), key=lambda t: -t[2])
    multi = len(acc) > 1
    items = []
    for lab, citation, p in ordre:
        libelle = f"{lab} — {fmt_pct(p)} de l'UF" if multi and p > 0 else lab
        item = {"libelle": libelle, "citation": citation}
        items.append(item)
    return items


def recap_item_libelle(item) -> str:
    if isinstance(item, dict):
        return _s(item.get("libelle")) or _s(item.get("texte"))
    return _s(item)


def recap_item_citation(item) -> str:
    return _s(item.get("citation")) if isinstance(item, dict) else ""


def _ex_hauteurs(inter):
    def lab(o):
        secteur = _s(o.get("libelong"))
        h = _num(o.get("hauteur")) if _s(o.get("hauteur")) else ""
        if secteur and h:
            return f"{secteur} (H max {h} m)"
        return secteur or (f"H max {h} m" if h else "")
    return _par_libelle(_objets(inter, "hauteurs"), lab)


def _ex_dpu(inter):
    dpu = [o for o in _objets(inter, "infos_surf") if _is_dpu(o)]
    return _par_libelle(dpu, _cles("libelle"))


def _ex_infos_plu(inter):
    infos = [o for o in _objets(inter, "infos_surf") if not _is_dpu(o)]
    return _par_libelle(infos, _cles("libelle"))


def _libelle_prescription(obj: dict) -> str:
    """Libellé GPU ; pour une OAP (typepsc 18) on ajoute le nom de l'orientation."""
    libelle = _s(obj.get("libelle")) or _s(obj.get("texte"))
    nom = _s(obj.get("nom"))
    if _s(obj.get("typepsc")) == "18" and nom:
        if libelle and nom.casefold() not in libelle.casefold():
            return f"{libelle} — {nom}"
        return libelle or nom
    return libelle


def _ex_prescriptions_plu(inter):
    module = _layer(inter, "prescriptions_plu")
    out: dict[str, float] = {}
    for couche in module.get("couches") or []:
        for it in couche.get("items") or []:
            lab = _court(_libelle_prescription(it))
            if lab:
                out[lab] = min(out.get(lab, 0.0) + _pct(it), 100.0)
    if not out:  # repli sur les couches brutes si le module n'a rien produit
        brut = []
        for k in ("prescriptions_surf", "prescriptions_lineaires", "prescriptions_ponctuelles"):
            brut.extend(_objets(inter, k))
        return _par_libelle(brut, _libelle_prescription)
    return [f"{lab} — {fmt_pct(p)}" if p > 0 else lab
            for lab, p in sorted(out.items(), key=lambda kv: -kv[1])]


def _ex_sup(inter):
    layer = _layer(inter, "servitudes") or _layer(inter, "servitudes_reglementees")
    out, seen = [], set()
    for s in layer.get("servitudes") or []:
        code = _s(s.get("suptype")).upper()
        nom = _s(s.get("libelle")) or _s(s.get("nomsuplitt")) or _s(s.get("nom_sup"))
        cle = code or nom.casefold()
        if not cle or cle in seen:
            continue
        seen.add(cle)
        if code and nom and not nom.upper().startswith(code):
            lab = f"{code} — {nom}"
        else:
            lab = nom or code
        monuments = _distinct(m.get("nom") for m in s.get("monuments") or [])
        if monuments:
            lab += " : " + ", ".join(monuments)
        out.append(_court(lab))
    return out


def _zones_risque(inter, brut_key: str, module_part: str):
    """PPR / PPRIF : libellés depuis les blocs du module, parts depuis la couche brute."""
    brut = _objets(inter, brut_key)
    lab_fn = _cles("label", "zone")
    pcts: dict[str, float] = {}
    for o in brut:
        lab = lab_fn(o)
        if lab:
            pcts[lab] = pcts.get(lab, 0.0) + _pct(o)
    blocs = (_layer(inter, "ppr_et_pprif").get(module_part) or {}).get("blocs") or []
    lignes: dict[str, float] = {}
    for b in blocs or brut:
        lab = lab_fn(b)
        if not lab:
            continue
        precis = " / ".join(x for x in (_s(b.get("risque")), _s(b.get("degre"))) if x)
        txt = _court(f"{lab} ({precis})" if precis else lab)
        lignes.setdefault(txt, pcts.get(lab, 0.0))
    return [f"{t} — {fmt_pct(p)}" if p > 0 else t
            for t, p in sorted(lignes.items(), key=lambda kv: -kv[1])]


def _ex_ppr(inter):
    return _zones_risque(inter, "ppr", "ppr")


def _ex_pprif(inter):
    return _zones_risque(inter, "pprif", "pprif")


def _ex_alea_feu(inter):
    blocs = _layer(inter, "alea_feu").get("blocs") or []
    return _par_libelle(blocs or _objets(inter, "alea_feu"), _cles("libelle"))


def _ex_rga(inter):
    def lab(o):
        alea, niveau = _s(o.get("alea")), _s(o.get("niveau"))
        if alea and niveau and alea.casefold() != niveau.casefold():
            return f"Aléa {alea} (niveau {niveau})"
        return f"Aléa {alea or niveau}" if (alea or niveau) else ""
    return _par_libelle(_objets(inter, "retrait_gonflement_argiles_2026"), lab)


def _ex_old(inter):
    return _par_libelle(_objets(inter, "old"), _cles("zonage")) or (
        ["Zone soumise aux OLD"] if _objets(inter, "old") else []
    )


def _ex_natura(inter):
    def lab(o):
        nom, code = _s(o.get("n_site")), _s(o.get("c_site"))
        return f"{nom} ({code})" if nom and code else (nom or code)
    items = _par_libelle(_objets(inter, "natura_2000"), lab)
    if not items and _layer(inter, "prairies_et_natura_2000").get("has_natura"):
        items = ["Site Natura 2000 (voir détail)"]
    return items


def _ex_znieff(inter):
    def lab(o):
        nom, t = _s(o.get("nom_site")), _s(o.get("znieff_type"))
        if not t:
            return nom
        t = f"type {t}" if t.isdigit() or t.upper() in {"I", "II"} else t
        return f"{nom} ({t})" if nom else f"ZNIEFF {t}"
    return _par_libelle(_objets(inter, "znieffs"), lab)


def _ex_prairies(inter):
    objets = _objets(inter, "prairies_sensibles")
    if objets:
        total = sum(_pct(o) for o in objets)
        return [f"Prairie permanente — {fmt_pct(total)}" if total else "Prairie permanente"]
    if _layer(inter, "prairies_et_natura_2000").get("has_prairie"):
        return ["Prairie permanente (voir détail)"]
    return []


def _ex_haies(inter):
    objets = _objets(inter, "haies_bocages")
    if not objets:
        return []
    longueur = sum(float(o.get("longueur_inter_m") or 0) for o in objets)
    txt = f"{len(objets)} linéaire(s) de haie"
    return [f"{txt} — {fmt_num(longueur)} m" if longueur else txt]


def _ex_aoc(inter):
    return _par_libelle(_objets(inter, "aoc"), _cles("denom"))


def _ex_zaer(inter):
    def lab(o):
        nom, fil = _s(o.get("nom")), _s(o.get("filiere"))
        return f"{nom} ({fil})" if nom and fil else (nom or fil)
    return _par_libelle(_objets(inter, "zaer"), lab)


def _ex_batiments(inter):
    objets = _objets(inter, "batiments")
    if not objets:
        return []
    emprise = sum(float(o.get("surface_inter_m2") or 0) for o in objets)
    txt = f"{len(objets)} bâtiment(s) cadastré(s)"
    if emprise:
        txt += f" — emprise ≈ {fmt_num(emprise)} m²"
    types = _distinct(o.get("type") for o in objets)
    if types:
        txt += f" ({', '.join(types)})"
    return [_court(txt)]


def _ex_enedis(inter):
    analyses = _layer(inter, "reseaux_enedis_lineaires").get("analyses") or []
    diags = _distinct(a.get("diagnostic_expert_raccordement") for a in analyses)
    return [_court(diags[0])] if diags else []


def _ex_taxes(inter):
    t = _layer(inter, "taxes")
    taux = _s(t.get("taux_communale_libelle"))
    if not taux:
        return []
    lib = _s(t.get("libelle"))
    return [f"Part communale : {taux}" + (f" ({lib})" if lib else "")]


# ============================================================
# REGISTRE DES RUBRIQUES (ordre = ordre d'affichage)
# ============================================================
@dataclass(frozen=True)
class Rubrique:
    theme: str
    titre: str
    sources: tuple            # clés de rapport["intersections"] dont dépend la rubrique
    extract: Callable[[dict], list]
    renvoi: str
    toujours: bool = False    # afficher même si non concernée
    si_absent: str = "Non concernée"
    note: str = ""            # mention sous les items (encart CES, etc.)


T_URBA = "Document d'urbanisme"
T_SUP = "Servitudes d'utilité publique"
T_RISQ = "Risques"
T_ENV = "Environnement et agriculture"
T_DIV = "Réseaux, bâti et fiscalité"

# Rubriques dont l'absence silencieuse se lit comme un « non concerné » juridique.
COUCHES_CRITIQUES = frozenset({
    "Zonage PLU",
    "Droit de préemption urbain",
    "Servitudes",
    "PPRN (zones)",
    "PPRIF (zones)",
})

RUBRIQUES: tuple[Rubrique, ...] = (
    Rubrique(T_URBA, "Zonage PLU", ("zonage_plu",), _ex_zonage, R_ZONAGE,
             toujours=True, si_absent="Zonage non déterminé"),
    Rubrique(T_URBA, "Emprise au sol (CES)", ("zonage_plu",), _ex_ces, R_CES,
             toujours=True, si_absent="Zonage non déterminé", note=NOTE_CES),
    Rubrique(T_URBA, "Hauteurs maximales", ("hauteurs",), _ex_hauteurs, R_HAUTEURS),
    Rubrique(T_URBA, "Prescriptions PLU",
             ("prescriptions_plu", "prescriptions_surf",
              "prescriptions_lineaires", "prescriptions_ponctuelles"),
             _ex_prescriptions_plu, R_PRESC),
    Rubrique(T_URBA, "Informations PLU", ("infos_surf",), _ex_infos_plu, R_PRESC),
    Rubrique(T_URBA, "Droit de préemption urbain", ("infos_surf",), _ex_dpu, R_DPU,
             toujours=True, si_absent="Non soumise au DPU"),

    Rubrique(T_SUP, "Servitudes", ("servitudes", "servitudes_reglementees"), _ex_sup, R_SUP,
             toujours=True, si_absent="Aucune servitude recensée"),

    Rubrique(T_RISQ, "PPRN (zones)", ("ppr", "ppr_et_pprif"), _ex_ppr, R_RISQUES,
             toujours=True, si_absent="Hors zone PPRN"),
    Rubrique(T_RISQ, "PPRIF (zones)", ("pprif", "ppr_et_pprif"), _ex_pprif, R_RISQUES,
             toujours=True, si_absent="Hors zone PPRIF"),
    Rubrique(T_RISQ, "Aléa feu de forêt (PAC)", ("alea_feu",), _ex_alea_feu, R_RISQUES),
    Rubrique(T_RISQ, "Retrait-gonflement des argiles",
             ("retrait_gonflement_argiles_2026",), _ex_rga, R_RISQUES),
    Rubrique(T_RISQ, "Obligations légales de débroussaillement", ("old",), _ex_old, R_RISQUES),

    Rubrique(T_ENV, "Natura 2000", ("natura_2000", "prairies_et_natura_2000"),
             _ex_natura, R_NATURA, toujours=True, si_absent="Hors site Natura 2000"),
    Rubrique(T_ENV, "ZNIEFF", ("znieffs",), _ex_znieff, R_PRESC),
    Rubrique(T_ENV, "Prairies sensibles", ("prairies_sensibles", "prairies_et_natura_2000"),
             _ex_prairies, R_NATURA),
    Rubrique(T_ENV, "Haies et bocages", ("haies_bocages",), _ex_haies, R_PRESC),
    Rubrique(T_ENV, "AOC viticole", ("aoc",), _ex_aoc, R_PRESC),
    Rubrique(T_ENV, "ZAER", ("zaer",), _ex_zaer, R_PRESC),

    Rubrique(T_DIV, "Bâti existant", ("batiments",), _ex_batiments, R_PRESC),
    Rubrique(T_DIV, "Réseau électrique BT", ("reseaux_enedis_lineaires",), _ex_enedis, R_ENEDIS),
    Rubrique(T_DIV, "Taxe d'aménagement", ("taxes",), _ex_taxes, R_TAXES),
)


# ============================================================
# POINT D'ENTRÉE
# ============================================================
def compute_recap(rapport: dict, rubriques: Iterable[Rubrique] = RUBRIQUES) -> dict:
    inter = rapport.get("intersections") or {}
    lignes: list[dict] = []
    alertes: list[str] = []

    for r in rubriques:
        presentes = [k for k in r.sources if k in inter]
        if not presentes:
            continue  # couche hors catalogue pour cette commune

        items: list[str] = []
        couche_ko = any(_layer(inter, k).get("status") in STATUTS_KO for k in presentes)
        if couche_ko:
            statut = NON_VERIFIE
        else:
            try:
                items = r.extract(inter) or []
                statut = CONCERNE if items else NON_CONCERNE
            except Exception as exc:  # la synthèse ne doit jamais casser le CUA
                log = logger.error if r.titre in COUCHES_CRITIQUES else logger.warning
                log("recap %s : %s", r.titre, exc)
                statut = NON_VERIFIE

        if statut == NON_VERIFIE:
            alertes.append(r.titre)
            if couche_ko:
                log = logger.error if r.titre in COUCHES_CRITIQUES else logger.warning
                log("recap %s : donnée indisponible (couche en erreur ou table absente)", r.titre)
        if statut == NON_CONCERNE and not r.toujours:
            continue

        lignes.append({
            "theme": r.theme,
            "titre": r.titre,
            "statut": statut,
            "items": items,
            "renvoi": r.renvoi,
            "si_absent": r.si_absent,
            "note": r.note,
        })

    n = rapport.get("n_parcelles") or len(rapport.get("parcelles") or [])
    entete = f"Unité foncière : {n} parcelle{'s' if n > 1 else ''}"
    try:
        surf = float(rapport.get("surface_m2") or 0)
    except (TypeError, ValueError):
        surf = 0.0
    if surf:
        entete += f" — {fmt_num(surf)} m² (surface SIG, base des pourcentages)"

    return {"entete": entete, "lignes": lignes, "alertes": alertes}


if __name__ == "__main__":
    # python recap.py rapport_intersections_XXX.json
    data = json.loads(open(sys.argv[1], encoding="utf-8").read())
    recap = compute_recap(data)
    print(recap["entete"])
    for l in recap["lignes"]:
        print(f"[{l['theme']}] {l['titre']} ({l['statut']})")
        if l["statut"] == NON_VERIFIE:
            print("    - Donnée indisponible — à vérifier manuellement")
        else:
            for it in l["items"] or [l["si_absent"]]:
                print(f"    - {recap_item_libelle(it)}")
                citation = recap_item_citation(it)
                if citation:
                    print(f"      {citation}")
        if l.get("note"):
            print(f"      {l['note']}")
    if recap["alertes"]:
        print("⚠ non vérifié :", ", ".join(recap["alertes"]))