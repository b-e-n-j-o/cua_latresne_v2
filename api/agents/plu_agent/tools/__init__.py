"""
Package tools — agent PLU Argelès-sur-Mer.

Ajouter un tool :
  1. Créer tools/mon_tool.py (fonction + DECL_MON_TOOL)
  2. Importer ici et ajouter dans TOOL_DECLARATIONS + build_dispatch
"""

import functools

from google.genai import types

from .map_data import DECL_MAP_DATA, get_map_data
from .parcelle import DECL_PARCELLE, get_parcelle
from .zonage import (
    DECL_ZONES_GEOM,
    DECL_ZONAGE,
    get_zonage_et_reglements,
    get_zones_for_geometry,
)

TOOL_DECLARATIONS = types.Tool(
    function_declarations=[
        DECL_PARCELLE,
        DECL_ZONAGE,
        DECL_MAP_DATA,
        DECL_ZONES_GEOM,
    ]
)

TOOL_FUNCTIONS = {
    "get_parcelle": get_parcelle,
    "get_zonage_et_reglements": get_zonage_et_reglements,
    "get_map_data": get_map_data,
    "get_zones_for_geometry": get_zones_for_geometry,
}

TOOL_RESPONSE_SHAPES = {
    "get_parcelle": {
        "parcelle": "object — idu, section, numero, contenance, geojson_wgs84, superficie_m2",
        "error": "string | null",
    },
    "get_zonage_et_reglements": {
        "zones": (
            "array — code_zone, libelle, superficie_intersection_m2, "
            "pct_parcelle_couverte, nom_zone, reglementation, …"
        ),
        "count": "integer",
        "error": "string | null",
    },
    "get_map_data": {
        "map_ready": "bool — carte affichée côté UI",
        "parcelle": "object — idu, section, numero (sans géométrie côté LLM)",
        "zones": "array — code_zone, pct, libelle, color",
        "error": "string | null",
    },
    "get_zones_for_geometry": {
        "zones": "array — code_zone, libelle, typezone, destdomi",
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
    "get_zonage_et_reglements",
    "get_map_data",
    "get_zones_for_geometry",
    "print_tools_mapping",
]
