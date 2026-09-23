# -*- coding: utf-8 -*-
"""
cas_speciaux.py — Règles spécifiques de traitement avant génération du CUA
-----------------------------------------------------------------------
Chaque fonction adapte une couche spécifique avant injection dans le DOCX :
- découpe, regroupement ou fusion d'entités selon leur logique propre.
"""

from typing import Dict, Any, List, Tuple
import re


# ==============================================================
# 🟩 Cas 1 : Patrimoine naturel (ZNIEFF vs autres)
# ==============================================================

def split_patrimoine_znieff(intersections: Dict[str, Any], layers_by_article: Dict[str, list]):
    """
    Cas particulier : la couche patrimoine_naturel_latresne contient à la fois :
    - des entités ZNIEFF (type I ou II) à placer dans l'article 7 (Informations utiles)
    - des entités environnementales (autres) à garder dans l'article 5
    """

    key = "patrimoine_naturel_latresne"
    if key not in intersections:
        return

    layer = intersections[key]
    objs = layer.get("objets") or []

    znieff_objs = []
    other_objs = []

    for o in objs:
        tpat = (o.get("type_patrimoine") or "").strip().lower()
        if "znieff" in tpat:
            znieff_objs.append(o)
        else:
            other_objs.append(o)

    # Créer une sous-couche ZNIEFF vers article 7
    if znieff_objs:
        znieff_layer = layer.copy()
        znieff_layer["objets"] = znieff_objs
        znieff_layer["nom"] = f"{layer.get('nom', 'Patrimoine naturel')} (ZNIEFF)"
        znieff_layer["source_article"] = 5
        layers_by_article.setdefault("7", []).append(znieff_layer)

    # Garder les autres entités dans la couche originale (article 5)
    if other_objs:
        layer["objets"] = other_objs
        intersections[key] = layer
    else:
        # Si toutes les entités étaient ZNIEFF → on supprime la couche du bloc principal
        del intersections[key]


# ==============================================================
# 🟧 Cas 2 : Zones de bruit (ZBRS Gironde)
# ==============================================================

def merge_zbrs_legendes(intersections: Dict[str, Any], layers_by_article: Dict[str, list]):
    """
    Cas particulier : fusion des entités ZBRS selon leurs légendes.
    Objectif : créer une seule réglementation par période (jour/nuit) en prenant :
    - la fourchette min → max de 'legende'
    - la réglementation descriptive correspondant à la valeur max
    """

    key = "zbrs_gironde"
    if key not in intersections:
        return

    layer = intersections[key]
    objs = layer.get("objets") or []
    if not objs:
        return

    # --- Table des correspondances entre niveau max et phrase descriptive ---
    REGLES_BRUIT = [
        (55, "un bureau calme ou une machine à laver la vaisselle"),
        (60, "le bruit d'une conversation ou d'une machine à laver"),
        (65, "le bruit d'un restaurant animé ou d'une douche"),
        (70, "le bruit d'un aspirateur ou d'un sèche-cheveux"),
        (75, "le bruit d'une tondeuse électrique ou d'une scie sauteuse"),
        (999, "le bruit d'une tondeuse thermique ou le chant du coq"),
    ]

    def parse_db(s: str):
        if not s:
            return None
        m = re.search(r"(\d+(?:\.\d+)?)", str(s))
        return float(m.group(1)) if m else None

    def phrase_bruit(max_db: float, periode: str, min_db: float = None) -> str:
        """Construit la phrase finale de réglementation selon la fourchette."""
        # Trouver la phrase descriptive la plus adaptée
        phrase = next((txt for seuil, txt in REGLES_BRUIT if max_db <= seuil), REGLES_BRUIT[-1][1])

        # Si une seule valeur, créer une fourchette artificielle de 5 dB
        if min_db is None or min_db == max_db:
            fourchette = f"entre {int(max_db - 5)} et {int(max_db)} dB"
        else:
            fourchette = f"entre {int(min_db)} et {int(max_db)} dB"

        return (
            f"Le terrain est exposé à un fond sonore {periode} oscillant {fourchette}. "
            f"Cette intensité sonore correspond à {phrase}."
        )

    # --- Regrouper les entités par période jour/nuit ---
    grouped = {"jour": [], "nuit": []}
    for o in objs:
        periode = (o.get("jour_nuit") or "jour").strip().lower()
        if periode not in grouped:
            periode = "jour"
        grouped[periode].append(o)

    merged_objs = []

    for periode, entites in grouped.items():
        entites_valides = [e for e in entites if parse_db(e.get("legende")) is not None]
        if not entites_valides:
            continue

        entites_valides.sort(key=lambda e: parse_db(e.get("legende")))
        min_db = parse_db(entites_valides[0]["legende"])
        max_db = parse_db(entites_valides[-1]["legende"])

        regle_synthetique = phrase_bruit(max_db, periode, min_db)

        merged_objs.append({
            "periode": periode,
            "legende_min": min_db,
            "legende_max": max_db,
            "reglementation": regle_synthetique,
            "source": f"ZBRS ({periode})"
        })

    if merged_objs:
        layer["objets"] = merged_objs
        layer["nom"] = "Zones de bruit – synthèse par période (ZBRS)"
        intersections[key] = layer


# ==============================================================
# 🟦 Cas 3 : Fossés Latresne (déduplication des réglementations)
# ==============================================================

def deduplicate_fosses_reglementations(intersections: Dict[str, Any], layers_by_article: Dict[str, list]):
    """
    Cas particulier : pour la couche troncons_et_fosses_latresne,
    regroupe les objets par réglementation unique pour éviter les répétitions.
    
    Objectif : ne garder qu'une seule occurrence de chaque réglementation distincte.
    """
    
    key = "troncons_et_fosses_latresne"
    if key not in intersections:
        return
    
    layer = intersections[key]
    objs = layer.get("objets") or []
    if not objs:
        return
    
    def normalize_reglementation(regl: str) -> str:
        """Normalise une réglementation pour la comparaison."""
        if not regl:
            return ""
        # Normaliser : enlever espaces multiples, retours à la ligne multiples, tabulations
        # Remplacer tous les espaces multiples (y compris \n, \t, \r) par un seul espace
        normalized = re.sub(r'\s+', ' ', str(regl).strip())
        # Enlever les espaces en début/fin de chaque ligne virtuelle
        normalized = normalized.strip()
        return normalized
    
    # Regrouper les objets par réglementation normalisée
    reglementations_map = {}  # {réglementation_normalisée: premier_objet}
    
    for obj in objs:
        regl = obj.get("reglementation") or ""
        regl_normalized = normalize_reglementation(regl)
        
        # Si réglementation vide, garder quand même l'objet
        if not regl_normalized:
            # Pour les objets sans réglementation, on les garde tels quels
            # On crée une clé unique pour éviter de les fusionner
            if "_sans_reglementation" not in reglementations_map:
                reglementations_map["_sans_reglementation"] = []
            reglementations_map["_sans_reglementation"].append(obj)
            continue
        
        # Si cette réglementation n'existe pas encore, on l'ajoute
        if regl_normalized not in reglementations_map:
            # Créer un nouvel objet avec cette réglementation unique
            unique_obj = obj.copy()
            reglementations_map[regl_normalized] = unique_obj
        # Sinon, on ignore cet objet (déjà une réglementation identique)
    
    # Reconstruire la liste des objets uniques
    unique_objs = []
    
    # D'abord, ajouter toutes les réglementations uniques
    for regl_norm, obj in reglementations_map.items():
        if regl_norm != "_sans_reglementation":
            unique_objs.append(obj)
    
    # Ensuite, ajouter les objets sans réglementation (si il y en a)
    if "_sans_reglementation" in reglementations_map:
        unique_objs.extend(reglementations_map["_sans_reglementation"])
    
    # Mettre à jour la couche avec les objets dédupliqués
    if unique_objs:
        layer["objets"] = unique_objs
        layer["nom"] = layer.get("nom", "Fossés Latresne")  # Garder le nom original
        intersections[key] = layer
    else:
        # Si plus aucun objet après déduplication → supprimer la couche
        del intersections[key]


# ==============================================================
# 🧩 Entrée principale : appliquer toutes les règles spéciales
# ==============================================================

def appliquer_cas_speciaux(intersections: Dict[str, Any], layers_by_article: Dict[str, list]):
    """
    Point d'entrée : applique toutes les règles spécifiques.
    """
    split_patrimoine_znieff(intersections, layers_by_article)
    merge_zbrs_legendes(intersections, layers_by_article)
    deduplicate_fosses_reglementations(intersections, layers_by_article)
