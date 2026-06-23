# -*- coding: utf-8 -*-
"""Profils RAA par slug communal (schéma SQL + prompts Gemini)."""

from __future__ import annotations

from dataclasses import dataclass

# Nature thématique de chaque arrêté (clé JSON `nature` dans raa_analyse.arretes[])
ARRETE_NATURES = frozenset({"URBANISME", "ENVIRONNEMENT", "EVENEMENT", "AUTRE"})

_NATURE_ALIASES = {
    "EVENNEMENT": "EVENEMENT",
    "ÉVÉNEMENT": "EVENEMENT",
    "EVENT": "EVENEMENT",
    "ENV": "ENVIRONNEMENT",
    "URBA": "URBANISME",
}


def normalise_arrete_nature(value: object | None) -> str:
    if value is None:
        return "AUTRE"
    v = str(value).strip().upper()
    v = _NATURE_ALIASES.get(v, v)
    return v if v in ARRETE_NATURES else "AUTRE"


_NATURE_CLASSIFICATION = """
4. Classe la nature thématique de l'arrêté (`nature`, une seule valeur) :
- URBANISME : PLU/POS, permis et autorisations d'urbanisme, ZAC, servitudes d'urbanisme, voirie/aménagement, construction, démolition, division parcellaire, loi littoral côté aménagement, etc.
- ENVIRONNEMENT : ICPE, eaux et milieux, forêts, Natura 2000, réserves, captages, PPRI/PPRIF et risques naturels, pollution, défrichement, protection du patrimoine naturel, etc.
- EVENEMENT : mesures temporaires liées à un événement (circulation, sécurité, occupation domaine public, réglementation provisoire de manifestation, feux d'artifice, etc.)
- AUTRE : nominations, RH, attributions, contentieux sans lien direct, ou acte ne rentrant pas clairement dans les trois catégories ci-dessus
"""

_JSON_ARRETE_SCHEMA = """
Réponds en JSON avec cette structure exacte :
{{
  "arretes": [
    {{
      "titre": "titre ou objet complet de l'arrêté",
      "reference": "numéro de référence si visible",
      "pertinence": "DIRECTE" | "INDIRECTE" | "POSSIBLE" | "NON_PERTINENT",
      "nature": "URBANISME" | "ENVIRONNEMENT" | "EVENEMENT" | "AUTRE",
      "raison": "explication en 1-2 phrases",
      "resume": "résumé en 2-4 phrases",
      "pages": "numéro(s) de page"
    }}
  ],
  "nb_arretes_total": 0,
  "nb_arretes_pertinents": 0,
  "commune_mentionnee": true | false,
  "niveau_alerte": "ROUGE" | "ORANGE" | "VERT",
  "resume_global": "résumé global en 2-3 phrases"
}}

Règles pour niveau_alerte :
- ROUGE  : au moins un arrêté DIRECTE
- ORANGE : au moins un arrêté INDIRECTE ou POSSIBLE, aucun DIRECTE
- VERT   : uniquement des arrêtés NON_PERTINENT
"""


@dataclass(frozen=True)
class RaaCommuneConfig:
    slug: str
    schema: str
    commune_label: str
    departement_label: str
    insee: str
    system_prompt: str
    analyse_prompt: str


def _prompts_argeles() -> tuple[str, str]:
    commune = "Argelès-sur-Mer"
    insee = "66008"
    system = f"""Tu es un expert en droit administratif français spécialisé dans l'analyse des Recueils des Actes Administratifs (RAA) préfectoraux.
Tu analyses des arrêtés préfectoraux des Pyrénées-Orientales (66) pour le compte d'une commune : {commune} ({insee}).

Tu dois établir si un arrêté est pertinent en lien avec l'urbanisme et l'aménagement : servitudes d'utilité publique, risques naturels (inondation/PPRI, feux de forêt/PPRIF, submersion marine, mouvements de terrain, retrait-gonflement des argiles), loi Littoral, eaux et zones humides, forêts (notamment le massif des Albères), routes et voies, télécommunications, monuments historiques, sites classés/inscrits, sites patrimoniaux remarquables, réserves naturelles, Natura 2000, périmètres de protection de captage, drainage, défrichement, ICPE, etc.
On cherche à identifier : de nouvelles servitudes, des modifications/suppressions de servitudes, de nouvelles réglementations urbanistiques ou environnementales, ou toute mesure affectant le territoire communal.

Contexte géographique : {commune} est une commune littorale méditerranéenne des Pyrénées-Orientales, sur la Côte Vermeille, membre de la Communauté de communes Albères-Côte Vermeille-Illibéris (CC ACVI), arrondissement de Céret. Elle est concernée par la loi Littoral, des risques inondation/submersion et feux de forêt, et des espaces naturels protégés.
"""
    analyse = f"""Analyse ce Recueil des Actes Administratifs (RAA) de la Préfecture des Pyrénées-Orientales.

IMPORTANT : ce RAA peut contenir plusieurs arrêtés distincts. Analyse CHAQUE arrêté séparément.

Pour CHAQUE arrêté présent dans le document :
1. Identifie son titre/objet précis
2. Détermine s'il est pertinent pour la commune de {commune} ({insee})
3. Rédige un résumé de son contenu
{_NATURE_CLASSIFICATION}
Critères de pertinence :
- DIRECTE : l'arrêté mentionne explicitement "{commune}" (ou "Argelès sur Mer")
- INDIRECTE : l'arrêté concerne la CC Albères-Côte Vermeille-Illibéris, l'arrondissement de Céret, ou une zone géographique incluant clairement {commune}
- POSSIBLE : l'arrêté s'applique à l'ensemble des Pyrénées-Orientales ou à un large périmètre pouvant inclure {commune}
- NON_PERTINENT : concerne une autre commune/zone précise, ou purement administratif/RH

{_JSON_ARRETE_SCHEMA}

`commune_mentionnee` = true uniquement si "{commune}" est explicitement nommée.
Réponds UNIQUEMENT en JSON valide, sans markdown, sans backticks, sans commentaires.
"""
    return system, analyse


def _prompts_latresne() -> tuple[str, str]:
    commune = "Latresne"
    insee = "33234"
    system = f"""Tu es un expert en droit administratif français spécialisé dans l'analyse des Recueils des Actes Administratifs (RAA) préfectoraux.
Tu analyses des arrêtés préfectoraux de la Gironde pour le compte d'une commune : {commune} ({insee}).
Tu dois établir si un arrêté est pertinent en lien avec l'urbanisme, l'environnement, les risques naturels, les eaux, les forêts, les routes, les voies ferrées, les télécommunications, les monuments historiques, les sites classés, les réserves naturelles, les sites patrimoniaux remarquables, les périmètres de protection des captages, etc.
On cherche à identifier de nouvelles servitudes, des modifications ou suppressions de servitudes, de nouvelles réglementations urbanistiques ou environnementales.
Pour info, {commune} est une commune de la Gironde située sur la rive droite de la Garonne, dans la métropole bordelaise, mais pas membre de Bordeaux Métropole.
"""
    analyse = f"""Analyse ce Recueil des Actes Administratifs (RAA) de la Préfecture de la Gironde.

IMPORTANT : ce RAA peut contenir plusieurs arrêtés distincts. Analyse CHAQUE arrêté séparément.

Pour CHAQUE arrêté présent dans le document :
1. Identifie son titre/objet précis
2. Détermine s'il est pertinent pour la commune de {commune} ({insee})
3. Rédige un résumé de son contenu
{_NATURE_CLASSIFICATION}
Critères de pertinence :
- DIRECTE : l'arrêté mentionne explicitement "{commune}"
- INDIRECTE : l'arrêté concerne la CC des Coteaux Bordelais ou une zone géographique incluant clairement {commune}
- POSSIBLE : l'arrêté s'applique à l'ensemble de la Gironde ou un large périmètre pouvant inclure {commune}
- NON_PERTINENT : concerne une autre commune/zone précise, ou purement administratif/RH

{_JSON_ARRETE_SCHEMA}

`commune_mentionnee` = true uniquement si "{commune}" est explicitement nommée.
Réponds UNIQUEMENT en JSON valide, sans markdown, sans backticks, sans commentaires.
"""
    return system, analyse


_argeles_sys, _argeles_analyse = _prompts_argeles()
_latresne_sys, _latresne_analyse = _prompts_latresne()

RAA_COMMUNES: dict[str, RaaCommuneConfig] = {
    "argeles": RaaCommuneConfig(
        slug="argeles",
        schema="argeles",
        commune_label="Argelès-sur-Mer",
        departement_label="Pyrénées-Orientales",
        insee="66008",
        system_prompt=_argeles_sys,
        analyse_prompt=_argeles_analyse,
    ),
    "latresne": RaaCommuneConfig(
        slug="latresne",
        schema="latresne",
        commune_label="Latresne",
        departement_label="Gironde",
        insee="33234",
        system_prompt=_latresne_sys,
        analyse_prompt=_latresne_analyse,
    ),
}


def get_raa_config(slug: str) -> RaaCommuneConfig | None:
    return RAA_COMMUNES.get(slug)
