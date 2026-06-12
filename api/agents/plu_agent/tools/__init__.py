"""
Package tools — agent PLU Argelès-sur-Mer.

Structure :
  - tools/*.py          → tools LLM (DECL_* + Gemini) : parcelle, contexte_parcelle, recherche
  - tools/utils/*.py    → couches spatiales (SQL + get_* + build_map_*)
  - cartography/carto.py → catalogue GeoJSON GET /map (hors LLM)

Ajouter un tool LLM :
  1. Créer tools/mon_tool.py (fonction + DECL_MON_TOOL)
  2. Importer ici et ajouter dans TOOL_DECLARATIONS + build_dispatch
"""

import functools

from google.genai import types

from .contexte_parcelle import DECL_CONTEXTE_PARCELLE, get_contexte_parcelle
from .geoportail_contexte_live import (
    DECL_GEOPORTAIL_CONTEXTE_LIVE,
    get_geoportail_contexte_live,
)
from .parcelle import DECL_PARCELLE, get_parcelle
from .resolve_commune_insee import DECL_RESOLVE_COMMUNE_INSEE, resolve_commune_insee
from .reglement_ppr import DECL_REGLEMENT_PPR, get_ppr_reglement
from .reglement_pprif import DECL_REGLEMENT_PPRIF, get_pprif_reglement
from .reglement_ppri import DECL_REGLEMENT_PPRI, get_reglement_ppri
from .reglement_pprmvt import DECL_REGLEMENT_PPRMVT, get_reglement_pprmvt
from .reglement_zone import DECL_REGLEMENT_ZONE, get_reglement_zone
from .recherche_articles import (
    DECL_SEARCH_ARTICLES,
    DECL_GET_ARTICLE_BY_NUM,
    search_articles_urbanisme,
    get_article_urbanisme_by_num,
)

TOOL_DECLARATIONS_BY_NAME: dict[str, types.FunctionDeclaration] = {
    "get_parcelle": DECL_PARCELLE,
    "get_contexte_parcelle": DECL_CONTEXTE_PARCELLE,
    "get_geoportail_contexte_live": DECL_GEOPORTAIL_CONTEXTE_LIVE,
    "resolve_commune_insee": DECL_RESOLVE_COMMUNE_INSEE,
    "search_articles_urbanisme": DECL_SEARCH_ARTICLES,
    "get_article_urbanisme_by_num": DECL_GET_ARTICLE_BY_NUM,
    "get_reglement_zone": DECL_REGLEMENT_ZONE,
    "get_reglement_pprmvt": DECL_REGLEMENT_PPRMVT,
    "get_reglement_ppri": DECL_REGLEMENT_PPRI,
    "get_ppr_reglement": DECL_REGLEMENT_PPR,
    "get_pprif_reglement": DECL_REGLEMENT_PPRIF,
}

DEFAULT_LLM_TOOL_NAMES = tuple(TOOL_DECLARATIONS_BY_NAME.keys())

TOOL_DECLARATIONS = types.Tool(
    function_declarations=[
        TOOL_DECLARATIONS_BY_NAME[n] for n in DEFAULT_LLM_TOOL_NAMES
    ]
)

TOOL_FUNCTIONS = {
    "get_parcelle": get_parcelle,
    "get_contexte_parcelle": get_contexte_parcelle,
    "get_geoportail_contexte_live": get_geoportail_contexte_live,
    "resolve_commune_insee": resolve_commune_insee,
    "search_articles_urbanisme": search_articles_urbanisme,
    "get_article_urbanisme_by_num": get_article_urbanisme_by_num,
    "get_reglement_zone": get_reglement_zone,
    "get_reglement_pprmvt": get_reglement_pprmvt,
    "get_reglement_ppri": get_reglement_ppri,
    "get_ppr_reglement": get_ppr_reglement,
    "get_pprif_reglement": get_pprif_reglement,
}

TOOL_RESPONSE_SHAPES = {
    "get_parcelle": {
        "parcelle": "object | null — une parcelle si seule",
        "parcelles": "array — détail par feuille cadastrale",
        "unite_fonciere": "object — geojson union, superficie_m2, nb_parcelles",
        "error": "string | null",
    },
    "get_contexte_parcelle": {
        "zones": (
            "array — code_zone, libelle, superficie_intersection_m2, "
            "pct_parcelle_couverte, reglementation, …"
        ),
        "zones_count": "integer",
        "surfaciques": "array — gml_id, libelle, txt, typepsc, stypepsc",
        "lineaires": "array — idem",
        "ponctuelles": "array — idem",
        "prescriptions_count": "integer — total",
        "servitudes": (
            "array — gid, nom_servitude (libellé principal), suptype, typeass, nomsuplitt, idass, nomass"
        ),
        "servitudes_count": "integer",
        "informations": (
            "object — surfaciques, lineaires, ponctuelles (libelle, typeinf, stypeinf), counts"
        ),
        "informations_count": "integer — total informations",
        "parcelles": "array — feuilles cadastrales de l'unité foncière",
        "nb_parcelles": "integer",
        "superficie_unite_m2": "number",
        "error": "string | null",
    },
    "get_geoportail_contexte_live": {
        "zones": "array — code_zone, libelle, superficie_intersection_m2, pct_parcelle_couverte",
        "zones_count": "integer",
        "surfaciques": "array — prescriptions surfaciques",
        "lineaires": "array — prescriptions lineaires",
        "ponctuelles": "array — prescriptions ponctuelles",
        "prescriptions_count": "integer — total prescriptions",
        "servitudes": "array — servitudes intersectantes",
        "servitudes_count": "integer",
        "informations": "object — surfaciques/lineaires/ponctuelles + counts",
        "informations_count": "integer — total informations",
        "parcelles": "array — feuilles cadastrales de l'unité foncière",
        "nb_parcelles": "integer",
        "superficie_unite_m2": "number",
        "llm_context_metrics": "object — payload_chars, payload_bytes_utf8, payload_tokens_tiktoken",
        "error": "string | null",
    },
    "resolve_commune_insee": {
        "query": "string — nom saisi",
        "insee": "string | null — code INSEE si résolution unique",
        "commune": "string | null — libellé commune retenue",
        "departement": "string | null",
        "status": "string — ok | ambiguous | not_found",
        "matches": "array — candidats (insee, libelle, departement, match_type, score)",
        "count": "integer",
        "error": "string | null",
    },
    "search_articles_urbanisme": {
        "articles": (
            "array — article_id, num, title, path_title, resume, text_clean, rrf_score"
        ),
        "count": "integer",
        "error": "string | null",
    },
    "get_article_urbanisme_by_num": {
        "articles": "array — article_id, num, title, path_title, resume, text_clean",
        "count": "integer",
        "error": "string | null",
    },
    "get_reglement_zone": {
        "code_zone": "string — code tel qu'en base",
        "reglementation": "string | null — texte intégral du règlement",
        "found": "boolean",
        "error": "string | null",
    },
    "get_reglement_pprmvt": {
        "dispositions_generales": (
            "array — DG1/DG2/DG3 : code_zone, type=dispositions_generales, libelle, "
            "reglementation, found, error"
        ),
        "zones": (
            "array — par code demandé : type=zone, libelle, reglementation, found, error"
        ),
        "dispositions_generales_found": "integer — 0 à 3",
        "zones_found": "integer",
        "zones_requested": "array — codes zones demandés (hors DG)",
        "error": "string | null — erreur globale SQL",
    },
    "get_ppr_reglement": {
        "zone_codes_requested": "array — DG, I, II, III chargées",
        "zone_codes_fetched": "array — zones trouvées en base",
        "sous_zone_labels": "array — labels PPR (ex. I-b2) pour cibler le texte",
        "dispositions_generales": "object | null — bloc DG",
        "zones": "array — blocs I, II, III avec reglementation markdown",
        "zones_found": "integer",
        "hors_zonage_ppr": "boolean — true si zone III seule (hors degré 1/2)",
        "guidance": "string | null — consigne pour le modèle",
        "error": "string | null",
    },
    "get_pprif_reglement": {
        "zone_codes_requested": "array — DG + R, B1, B2, B3, B4 chargées",
        "zone_codes_fetched": "array — zones trouvées en base",
        "dispositions_generales": "object | null — bloc DG",
        "zones": "array — blocs R/B1/B2/B3/B4 avec reglementation markdown et couleur",
        "zones_found": "integer",
        "zones_pprif_reference": "object — correspondance zone_code → couleur (R rouge, B1–B3 bleues, B4 blanche)",
        "guidance": "string | null — consigne pour le modèle (ex. parcelle multi-zones)",
        "error": "string | null",
    },
    "get_reglement_ppri": {
        "dispositions_generales": (
            "array — dispositions générales fusionnées (zone_code DG) : "
            "type=dispositions_generales, libelle, reglementation, blocs[], found"
        ),
        "dispositions_communes": "array — alias de dispositions_generales (rétrocompat)",
        "zones": (
            "array — par zone PPRI : zone_code, libelle, reglementation, blocs[], found"
        ),
        "dispositions_generales_found": "integer — 0 ou 1 (ligne DG)",
        "dispositions_communes_found": "integer — alias de dispositions_generales_found",
        "zones_found": "integer",
        "zones_requested": "array",
        "zones_available_in_db": "array — codes zone présents en base (aide debug)",
        "zones_ppri_reference": (
            "object — liste de référence : dispositions_generales=DG, zones_couleur=[...]"
        ),
        "code_insee": "string",
        "error": "string | null",
    },
}


def build_tool_declarations(
    tool_names: tuple[str, ...] | list[str] | None = None,
) -> types.Tool:
    """Sous-ensemble des déclarations Gemini (filtré par profil commune)."""
    names = tuple(tool_names) if tool_names is not None else DEFAULT_LLM_TOOL_NAMES
    unknown = [n for n in names if n not in TOOL_DECLARATIONS_BY_NAME]
    if unknown:
        raise ValueError(f"Tools LLM inconnus : {unknown}")
    return types.Tool(
        function_declarations=[TOOL_DECLARATIONS_BY_NAME[n] for n in names]
    )


def build_dispatch(
    db_config: dict,
    tool_names: tuple[str, ...] | list[str] | None = None,
) -> dict:
    names = tuple(tool_names) if tool_names is not None else DEFAULT_LLM_TOOL_NAMES
    return {
        name: functools.partial(TOOL_FUNCTIONS[name], db_config)
        for name in names
        if name in TOOL_FUNCTIONS
    }


def _schema_type_label(schema: types.Schema | None) -> str:
    if schema is None or schema.type is None:
        return "any"
    name = schema.type.name if hasattr(schema.type, "name") else str(schema.type)
    if name == "ARRAY" and schema.items is not None:
        return f"array<{_schema_type_label(schema.items)}>"
    return name.lower()


def _format_llm_parameters(schema: types.Schema | None) -> list[str]:
    if schema is None or not schema.properties:
        return ["  (aucun paramètre déclaré)"]
    required = set(schema.required or [])
    lines = []
    for prop_name, prop_schema in schema.properties.items():
        req = "requis" if prop_name in required else "optionnel"
        typ = _schema_type_label(prop_schema)
        desc = (prop_schema.description or "").strip()
        line = f"  - {prop_name} ({typ}, {req})"
        if desc:
            line += f"\n      {desc}"
        lines.append(line)
    return lines


def print_tools_mapping() -> None:
    declarations = TOOL_DECLARATIONS.function_declarations
    llm_names = [fd.name for fd in declarations]

    print("=" * 72)
    print("Tools PLU Argelès — déclarations LLM + impl Python")
    print("=" * 72)

    for fd in declarations:
        name = fd.name
        print(f"\n▸ {name}")
        print("-" * 72)
        print(f"  {(fd.description or '').strip()}")
        print("\nParamètres :")
        for line in _format_llm_parameters(fd.parameters):
            print(line)
        response = TOOL_RESPONSE_SHAPES.get(name)
        if response:
            print("\nRéponse JSON :")
            for key, desc in response.items():
                print(f"  - {key}: {desc}")
        impl = TOOL_FUNCTIONS.get(name)
        if impl:
            print(f"\nImpl : {impl.__module__}.{impl.__qualname__}")

    print("\n" + "=" * 72)
    for name in llm_names:
        if name in TOOL_FUNCTIONS:
            print(f"  {name} → {TOOL_FUNCTIONS[name].__qualname__}")


__all__ = [
    "TOOL_DECLARATIONS",
    "TOOL_DECLARATIONS_BY_NAME",
    "DEFAULT_LLM_TOOL_NAMES",
    "TOOL_FUNCTIONS",
    "TOOL_RESPONSE_SHAPES",
    "build_tool_declarations",
    "build_dispatch",
    "get_parcelle",
    "get_contexte_parcelle",
    "get_geoportail_contexte_live",
    "resolve_commune_insee",
    "get_reglement_zone",
    "get_reglement_pprmvt",
    "get_reglement_ppri",
    "get_ppr_reglement",
    "get_pprif_reglement",
    "search_articles_urbanisme",
    "get_article_urbanisme_by_num",
    "print_tools_mapping",
]
