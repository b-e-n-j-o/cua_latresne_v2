"""
Catalogue des couches Géoportail de l'Urbanisme (GPU) pour le fetch live.

Vérité unique : pour chaque couche, on déclare le typename WFS, le groupe de
sortie, le mode d'intersection (strict_parcel / buffer), et la liste BLANCHE
d'attributs à conserver (validée par l'expérience métier — cf. catalogue commune).

Ce module ne fait AUCUN appel réseau ni calcul : c'est de la config pure.
"""

from __future__ import annotations

# Namespaces WFS de la Géoplateforme
NS_DU = "wfs_du"      # documents d'urbanisme (PLU/PLUi/CC/POS)
NS_SUP = "wfs_sup"    # servitudes d'utilité publique

# CRS
SRS_FETCH = "EPSG:4326"    # le WFS GPU sert en 4326 (mais ne déclare pas le CRS -> à forcer)
SRS_METRIC = "EPSG:2154"   # Lambert 93 pour tous les calculs métriques

# Seuils du filtre strict (identiques à l'ancien code BDD)
MIN_PARCEL_INTERSECTION_M2 = 1.0   # aire ou longueur minimale
POINT_BUFFER_TOLERANCE_M = -0.05   # rétraction parcelle pour test point (5 cm)

# Buffer par défaut du zonage (catalogue: strict_parcel=false, buffer_m=100)
ZONAGE_BUFFER_M = 100.0


# Définition des couches. L'ordre est l'ordre d'affichage logique.
# - layer        : typeName WFS complet
# - group        : groupe de sortie dans le payload (zonage / prescriptions / servitudes / informations)
# - subgroup     : surfaciques / lineaires / ponctuelles (pour prescriptions et infos)
# - geom_type    : type géométrique attendu (pour normalisation Multi*)
# - strict_parcel: True = filtre strict 1m² ; False = buffer simple
# - buffer_m     : buffer appliqué si strict_parcel=False
# - attributes   : liste BLANCHE des attributs WFS à garder (le reste est ignoré)
# - optional     : si True, une erreur de fetch n'interrompt pas (couche tolérée absente)
LAYERS = {
    "zonage": {
        "layer": f"{NS_DU}:zone_urba",
        "group": "zonage",
        "subgroup": None,
        "geom_type": "MultiPolygon",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["libelle", "libelong", "typezone"],
        "optional": False,
    },
    "prescriptions_surf": {
        "layer": f"{NS_DU}:prescription_surf",
        "group": "prescriptions",
        "subgroup": "surfaciques",
        "geom_type": "MultiPolygon",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typepsc", "stypepsc"],
        "optional": False,
    },
    "prescriptions_lin": {
        "layer": f"{NS_DU}:prescription_lin",
        "group": "prescriptions",
        "subgroup": "lineaires",
        "geom_type": "MultiLineString",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typepsc", "stypepsc"],
        "optional": False,
    },
    "prescriptions_pct": {
        "layer": f"{NS_DU}:prescription_pct",
        "group": "prescriptions",
        "subgroup": "ponctuelles",
        "geom_type": "Point",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typepsc", "stypepsc"],
        "optional": False,
    },
    "servitudes": {
        "layer": f"{NS_SUP}:assiette_sup_s",
        "group": "servitudes",
        "subgroup": None,
        "geom_type": "MultiPolygon",
        "strict_parcel": True,
        "buffer_m": 0.0,
        # nom_servitude n'existe pas dans le WFS -> calculé par fallback (cf. mapping)
        "attributes": ["gid", "idass", "nomass", "suptype", "typeass", "nomsuplitt", "nomreg"],
        "optional": False,
    },
    "infos_surf": {
        "layer": f"{NS_DU}:info_surf",
        "group": "informations",
        "subgroup": "surfaciques",
        "geom_type": "MultiPolygon",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typeinf", "stypeinf"],
        "optional": True,
    },
    "infos_lin": {
        "layer": f"{NS_DU}:info_lin",
        "group": "informations",
        "subgroup": "lineaires",
        "geom_type": "MultiLineString",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typeinf", "stypeinf"],
        "optional": True,
    },
    "infos_pct": {
        "layer": f"{NS_DU}:info_pct",
        "group": "informations",
        "subgroup": "ponctuelles",
        "geom_type": "Point",
        "strict_parcel": True,
        "buffer_m": 0.0,
        "attributes": ["gml_id", "libelle", "txt", "typeinf", "stypeinf"],
        "optional": True,
    },
}


def servitude_label(props: dict) -> str:
    """
    Libellé servitude : nom_servitude n'existant pas dans le WFS, on applique
    le fallback identique à l'ancien _nom_servitude_label() :
    suptype -> typeass -> nomsuplitt -> nomass -> 'Servitude'.
    """
    for key in ("suptype", "typeass", "nomsuplitt", "nomass"):
        val = (props.get(key) or "").strip()
        if val:
            return val
    return "Servitude"