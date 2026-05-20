"""
tools.py
--------
Définition de tous les tools disponibles pour l'agent PLU Argelès-sur-Mer.

Structure de chaque tool :
  - Fonction Python      : implémentation réelle (SQL + logique)
  - FunctionDeclaration  : schema exposé au LLM (nom, description, paramètres)

Ajouter un tool :
  1. Écrire la fonction Python
  2. Ajouter sa FunctionDeclaration dans TOOL_DECLARATIONS
  3. L'enregistrer dans TOOL_FUNCTIONS (+ TOOL_RESPONSE_SHAPES si besoin)
"""

import functools
import json
import logging
import psycopg2
import psycopg2.extras
from google.genai import types

logger = logging.getLogger("plu_tools")


# ---------------------------------------------------------------------------
# Helpers DB
# ---------------------------------------------------------------------------

def _db_connect(db_config: dict):
    return psycopg2.connect(**db_config)


def _query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    conn = _db_connect(db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ---------------------------------------------------------------------------
# TOOL 1 — Zonage PLU + réglementation (en une seule passe SQL)
# ---------------------------------------------------------------------------

def get_zonage_et_reglements(db_config: dict, geojson: str = None, idu: str = None, section: str = None, numero: str = None) -> dict:
    """
    Intersecte une géométrie (GeoJSON ou parcelle cadastrale) avec le zonage PLU
    d'Argelès-sur-Mer et retourne les zones concernées avec leurs textes réglementaires,
    superficies d'intersection et pourcentages de couverture.
    """
    try:
        # Étape 1 : résoudre la géométrie cible en WKB (EPSG:2154).
        # Deux requêtes séparées pour éviter les %s imbriqués dans le SQL principal.
        if geojson:
            geom_param = geojson if isinstance(geojson, str) else json.dumps(geojson)
            sql_geom   = """
                SELECT ST_AsEWKB(
                    ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 2154)
                ) AS geom_wkb
            """
            rows_geom = _query(db_config, sql_geom, (geom_param,))

        elif idu:
            sql_geom  = "SELECT ST_AsEWKB(geom_2154) AS geom_wkb FROM argeles.parcelles WHERE idu = %s LIMIT 1"
            rows_geom = _query(db_config, sql_geom, (idu,))

        elif section and numero:
            section_norm = section.upper().strip()
            numero_norm  = str(numero).strip()
            sql_geom  = """
                SELECT ST_AsEWKB(geom_2154) AS geom_wkb, idu, numero
                FROM argeles.parcelles
                WHERE section = %s
                  AND lpad(numero, 4, '0') = lpad(%s, 4, '0')
                LIMIT 1
            """
            logger.info(
                "get_zonage_et_reglements — recherche parcelle section=%r numero=%r (match lpad 4)",
                section_norm, numero_norm,
            )
            rows_geom = _query(db_config, sql_geom, (section_norm, numero_norm))

        else:
            return {"zones": [], "count": 0, "error": "Fournir geojson, idu, ou section+numero."}

        if not rows_geom or rows_geom[0]["geom_wkb"] is None:
            logger.warning(
                "get_zonage_et_reglements — géométrie introuvable "
                "(geojson=%s, idu=%r, section=%r, numero=%r)",
                bool(geojson), idu, section, numero,
            )
            return {
                "zones": [],
                "count": 0,
                "error": (
                    f"Géométrie introuvable pour les paramètres fournis "
                    f"(section={section!r}, numero={numero!r}, idu={idu!r})."
                ),
            }

        if section and numero and rows_geom[0].get("idu"):
            logger.info(
                "get_zonage_et_reglements — parcelle trouvée idu=%s numero_db=%r",
                rows_geom[0]["idu"], rows_geom[0].get("numero"),
            )

        geom_wkb = rows_geom[0]["geom_wkb"]

        # Étape 2 : intersection avec le zonage PLU + jointure réglementation.
        sql = """
            WITH cible AS (
                SELECT %s::geometry AS geom
            )
            SELECT
                z.zonage_reglement                                            AS code_zone,
                z.libelle,
                z.libelong,
                z.typezone,
                z.destdomi,
                ROUND(ST_Area(ST_Intersection(z.geom_2154, c.geom))::numeric, 1)
                                                                              AS superficie_intersection_m2,
                ROUND((ST_Area(ST_Intersection(z.geom_2154, c.geom))
                       / NULLIF(ST_Area(c.geom), 0) * 100)::numeric, 1)      AS pct_parcelle_couverte,
                r.nom_zone,
                r.resume_zone,
                r.reglementation
            FROM argeles.zonage_plu z
            CROSS JOIN cible c
            LEFT JOIN argeles.plu_reglement r ON r.code_zone = z.zonage_reglement
            WHERE ST_Intersects(z.geom_2154, c.geom)
            ORDER BY superficie_intersection_m2 DESC;
        """
        rows = _query(db_config, sql, (geom_wkb,))
        return {"zones": rows, "count": len(rows), "error": None}

    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# TOOL 2 — Intersection géométrique générique (GeoJSON uniquement)
#           Utile pour des géométries ad hoc non référencées en base
# ---------------------------------------------------------------------------

def get_zones_for_geometry(db_config: dict, geojson: str) -> dict:
    """
    Identifie les zones PLU intersectant une géométrie GeoJSON (WGS84).
    Version légère sans texte réglementaire — utile pour un premier diagnostic rapide.
    """
    try:
        geom_dict   = json.loads(geojson) if isinstance(geojson, str) else geojson
        geojson_str = json.dumps(geom_dict)
    except (json.JSONDecodeError, TypeError) as e:
        return {"zones": [], "count": 0, "error": f"GeoJSON invalide : {e}"}

    sql = """
        SELECT DISTINCT
            z.zonage_reglement AS code_zone,
            z.libelle,
            z.libelong,
            z.typezone,
            z.destdomi
        FROM argeles.zonage_plu z
        WHERE ST_Intersects(
            z.geom_2154,
            ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 2154)
        )
        ORDER BY z.zonage_reglement;
    """
    try:
        rows = _query(db_config, sql, (geojson_str,))
        return {"zones": rows, "count": len(rows), "error": None}
    except Exception as e:
        return {"zones": [], "count": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# Déclarations LLM (FunctionDeclaration)
# C'est ce que le LLM lit pour décider quel tool appeler et avec quels args.
# La description est l'élément le plus important.
# ---------------------------------------------------------------------------

TOOL_DECLARATIONS = types.Tool(
    function_declarations=[

        types.FunctionDeclaration(
            name="get_zonage_et_reglements",
            description=(
                "Intersecte une parcelle ou une géométrie avec le zonage PLU d'Argelès-sur-Mer "
                "et retourne pour chaque zone : le code de zone, le libellé, la superficie "
                "d'intersection en m², le pourcentage de couverture de la parcelle, "
                "et le texte réglementaire complet. "
                "Accepte soit un GeoJSON (WGS84), soit une référence cadastrale (section+numero ou idu). "
                "À utiliser pour toute question sur la constructibilité, les usages autorisés, "
                "les règles d'implantation, de hauteur, ou de destination d'une parcelle."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "geojson": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Géométrie GeoJSON en WGS84 (Point, Polygon, MultiPolygon). "
                            "À utiliser si la géométrie est connue directement."
                        ),
                    ),
                    "section": types.Schema(
                        type=types.Type.STRING,
                        description="Section cadastrale (ex: 'AC'). Alternative au geojson.",
                    ),
                    "numero": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Numéro de parcelle (ex: '45', '8770', '0042'). "
                            "La recherche tolère les zéros en tête (45 = 0045)."
                        ),
                    ),
                    "idu": types.Schema(
                        type=types.Type.STRING,
                        description="IDU de la parcelle. Alternative à section+numero.",
                    ),
                },
            ),
        ),

        types.FunctionDeclaration(
            name="get_zones_for_geometry",
            description=(
                "Identifie rapidement les zones PLU intersectant une géométrie GeoJSON (WGS84), "
                "sans récupérer les textes réglementaires. "
                "Utile pour un diagnostic rapide ou quand seule la liste des zones est nécessaire. "
                "Préférer get_zonage_et_reglements pour une analyse réglementaire complète."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "geojson": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Géométrie GeoJSON en WGS84 (Point, Polygon ou MultiPolygon). "
                            'Exemple : {"type":"Point","coordinates":[3.0267,42.5467]}'
                        ),
                    ),
                },
                required=["geojson"],
            ),
        ),

    ]
)

# ---------------------------------------------------------------------------
# Dispatch : nom du tool → fonction Python
# Toute nouvelle fonction doit être enregistrée ici et dans TOOL_DECLARATIONS.
# ---------------------------------------------------------------------------

TOOL_FUNCTIONS = {
    "get_zonage_et_reglements": get_zonage_et_reglements,
    "get_zones_for_geometry": get_zones_for_geometry,
}

# Forme des réponses JSON renvoyées à Gemini (non déclarée dans FunctionDeclaration).
TOOL_RESPONSE_SHAPES = {
    "get_zonage_et_reglements": {
        "zones": (
            "array — chaque élément : code_zone, libelle, libelong, typezone, destdomi, "
            "superficie_intersection_m2, pct_parcelle_couverte, nom_zone, resume_zone, reglementation"
        ),
        "count": "integer",
        "error": "string | null",
    },
    "get_zones_for_geometry": {
        "zones": (
            "array — chaque élément : code_zone, libelle, libelong, typezone, destdomi"
        ),
        "count": "integer",
        "error": "string | null",
    },
}


def _schema_type_label(schema: types.Schema | None) -> str:
    if schema is None or schema.type is None:
        return "any"
    name = schema.type.name if hasattr(schema.type, "name") else str(schema.type)
    if name == "ARRAY" and schema.items is not None:
        return f"array<{_schema_type_label(schema.items)}>"
    return name.lower()


def _format_llm_parameters(schema: types.Schema | None) -> list[str]:
    """Lignes décrivant les paramètres tels que déclarés au LLM."""
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


def build_dispatch(db_config: dict) -> dict:
    """
    Retourne le mapping nom_tool → callable, avec db_config injecté via closure.
    Comme ça les fonctions tools n'ont pas besoin d'accéder à une variable globale.
    """
    return {
        name: functools.partial(func, db_config)
        for name, func in TOOL_FUNCTIONS.items()
    }


def print_tools_mapping() -> None:
    """Affiche ce que le LLM voit (TOOL_DECLARATIONS) + impl Python (build_dispatch)."""
    declarations = TOOL_DECLARATIONS.function_declarations
    llm_names = [fd.name for fd in declarations]

    print("=" * 72)
    print("Ce que le LLM voit pour choisir et appeler les tools")
    print("(TOOL_DECLARATIONS — descriptions et paramètres d'entrée)")
    print("=" * 72)

    for fd in declarations:
        name = fd.name
        print(f"\n▸ {name}")
        print("-" * 72)
        print("Description (FunctionDeclaration.description) :")
        print(f"  {(fd.description or '').strip()}")

        print("\nParamètres d'entrée (FunctionDeclaration.parameters) :")
        for line in _format_llm_parameters(fd.parameters):
            print(line)

        print(
            "\nParamètres de sortie côté API Gemini :"
            "\n  (non déclarés — le schéma de réponse n'est pas exposé au modèle)"
        )
        response = TOOL_RESPONSE_SHAPES.get(name)
        if response:
            print("Réponse JSON effective renvoyée par l'implémentation :")
            for key, desc in response.items():
                print(f"  - {key}: {desc}")
        else:
            print("  (non documenté dans TOOL_RESPONSE_SHAPES)")

        impl = TOOL_FUNCTIONS.get(name)
        if impl is None:
            print("\nImplémentation Python : (manquante dans TOOL_FUNCTIONS)")
        else:
            mod = impl.__module__
            if mod == "__main__":
                mod = "tools"
            print(f"\nImplémentation Python (build_dispatch) : {mod}.{impl.__qualname__}")

    print("\n" + "=" * 72)
    print("Récapitulatif build_dispatch")
    print("=" * 72)
    for name in llm_names:
        if name in TOOL_FUNCTIONS:
            print(f"  {name} → {TOOL_FUNCTIONS[name].__qualname__}")

    llm_set = set(llm_names)
    only_impl = set(TOOL_FUNCTIONS) - llm_set
    if only_impl:
        print("\n⚠️  Dans TOOL_FUNCTIONS mais absent de TOOL_DECLARATIONS :")
        for name in sorted(only_impl):
            print(f"  - {name}")


if __name__ == "__main__":
    print_tools_mapping()