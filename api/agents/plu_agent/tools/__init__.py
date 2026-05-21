"""
Package tools — agent PLU Argelès-sur-Mer.

Structure :
  - tools/*.py          → tools LLM (DECL_* + Gemini) : parcelle, contexte_parcelle, recherche
  - tools/utils/*.py    → couches spatiales (SQL + get_* + build_map_*)
  - tools/carto.py      → catalogue GeoJSON GET /map (hors LLM)

Ajouter un tool LLM :
  1. Créer tools/mon_tool.py (fonction + DECL_MON_TOOL)
  2. Importer ici et ajouter dans TOOL_DECLARATIONS + build_dispatch
"""

import functools

from google.genai import types

from .contexte_parcelle import DECL_CONTEXTE_PARCELLE, get_contexte_parcelle
from .parcelle import DECL_PARCELLE, get_parcelle
from .recherche_articles import (
    DECL_SEARCH_ARTICLES,
    DECL_GET_ARTICLE_BY_NUM,
    search_articles_urbanisme,
    get_article_urbanisme_by_num,
)

TOOL_DECLARATIONS = types.Tool(
    function_declarations=[
        DECL_PARCELLE,
        DECL_CONTEXTE_PARCELLE,
        DECL_SEARCH_ARTICLES,
        DECL_GET_ARTICLE_BY_NUM,
    ]
)

TOOL_FUNCTIONS = {
    "get_parcelle": get_parcelle,
    "get_contexte_parcelle": get_contexte_parcelle,
    "search_articles_urbanisme": search_articles_urbanisme,
    "get_article_urbanisme_by_num": get_article_urbanisme_by_num,
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
            "pct_parcelle_couverte, nom_zone, reglementation, …"
        ),
        "zones_count": "integer",
        "surfaciques": "array — gml_id, libelle, txt, typepsc, stypepsc",
        "lineaires": "array — idem",
        "ponctuelles": "array — idem",
        "prescriptions_count": "integer — total",
        "servitudes": (
            "array — gid, suptype (type principal), typeass, nomsuplitt, idass, nomass"
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
}


def build_dispatch(db_config: dict) -> dict:
    return {
        name: functools.partial(func, db_config)
        for name, func in TOOL_FUNCTIONS.items()
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
    "TOOL_FUNCTIONS",
    "TOOL_RESPONSE_SHAPES",
    "build_dispatch",
    "get_parcelle",
    "get_contexte_parcelle",
    "search_articles_urbanisme",
    "get_article_urbanisme_by_num",
    "print_tools_mapping",
]
