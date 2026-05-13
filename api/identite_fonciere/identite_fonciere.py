"""
identite_parcelle.py
Service métier pour l'analyse d'identité parcellaire
"""
import os
import re
import json
import requests
import io
import logging
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Iterator
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from ..ssl_utils import ssl_verify_for_requests

try:
    import geopandas as gpd
except ImportError:
    gpd = None

load_dotenv()

logger = logging.getLogger(__name__)


def _debug_identite_fonciere() -> bool:
    """Logs détaillés pour la phase de tests : IDENTITE_FONCIERE_DEBUG=1 ou true."""
    return os.getenv("IDENTITE_FONCIERE_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")


# ------------------------------------------------------------
# Config
# ------------------------------------------------------------

SUPABASE_HOST = os.getenv('SUPABASE_HOST')
SUPABASE_DB = os.getenv('SUPABASE_DB')
SUPABASE_USER = os.getenv('SUPABASE_USER')
SUPABASE_PASSWORD = os.getenv('SUPABASE_PASSWORD')
SUPABASE_PORT = os.getenv('SUPABASE_PORT')

DATABASE_URL = f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

IGN_WFS_ENDPOINT = "https://data.geopf.fr/wfs/ows"
IGN_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

# Chargement catalogues identité foncière
# — Par défaut : schéma effectif `latresne` → catalogue étendu (données locales + GPU) ;
#   tout autre schéma (ex. argeles) → catalogue réduit Géoportail / GPU.
# — Surcharge : IDENTITE_FONCIERE_CATALOGUE_PATH = un seul .json imposé pour tous les schémas.
_CATALOG_DIR = Path(__file__).resolve().parent / "catalogues"
_CATALOG_LATRESNE_PATH = _CATALOG_DIR / "catalogue_identite_fonciere_latresne.json"
_CATALOG_GEOPORTAIL_PATH = _CATALOG_DIR / "catalogue_identite_fonciere_geoportail.json"

_CATALOGUE_OVERRIDE: Optional[Dict[str, Any]] = None
CATALOGUE_LATRESNE: Dict[str, Any]
CATALOGUE_GEOPORTAIL: Dict[str, Any]

_ov_raw = (os.getenv("IDENTITE_FONCIERE_CATALOGUE_PATH") or "").strip()
if _ov_raw:
    _ov_path = Path(_ov_raw)
    if not _ov_path.is_file():
        raise FileNotFoundError(
            f"IDENTITE_FONCIERE_CATALOGUE_PATH introuvable ou non fichier: {_ov_path}"
        )
    with open(_ov_path.resolve(), "r", encoding="utf-8") as f:
        _CATALOGUE_OVERRIDE = json.load(f)
    CATALOGUE_LATRESNE = _CATALOGUE_OVERRIDE
    CATALOGUE_GEOPORTAIL = _CATALOGUE_OVERRIDE
    CATALOGUE_PATH = str(_ov_path.resolve())
else:
    if not _CATALOG_LATRESNE_PATH.is_file():
        raise FileNotFoundError(f"Catalogue Latresne absent: {_CATALOG_LATRESNE_PATH}")
    if not _CATALOG_GEOPORTAIL_PATH.is_file():
        raise FileNotFoundError(f"Catalogue Géoportail absent: {_CATALOG_GEOPORTAIL_PATH}")
    with open(_CATALOG_LATRESNE_PATH, "r", encoding="utf-8") as f:
        CATALOGUE_LATRESNE = json.load(f)
    with open(_CATALOG_GEOPORTAIL_PATH, "r", encoding="utf-8") as f:
        CATALOGUE_GEOPORTAIL = json.load(f)
    CATALOGUE_PATH = f"{_CATALOG_LATRESNE_PATH} | {_CATALOG_GEOPORTAIL_PATH} (choix selon schéma)"


# Compat : import `CATALOGUE` = catalogue Latresne (scripts hors contexte requête).
CATALOGUE = CATALOGUE_LATRESNE

# Schéma PostgreSQL des couches cartographiques (défaut déploiement ; surcharge requête possible)
IDENTITE_DB_SCHEMA = os.getenv("IDENTITE_FONCIERE_DB_SCHEMA", "latresne").strip()

_identite_schema_override: ContextVar[Optional[str]] = ContextVar(
    "identite_fonciere_db_schema_override", default=None
)


def get_identite_db_schema() -> str:
    """Schéma effectif : `db_schema` du corps de requête si fourni, sinon IDENTITE_FONCIERE_DB_SCHEMA."""
    o = _identite_schema_override.get()
    return o if o is not None else IDENTITE_DB_SCHEMA


@contextmanager
def identite_fonciere_request_context(db_schema: Optional[str] = None):
    """
    Fixe le schéma PostGIS pour la durée d'une requête API (multi-communes / multi-schémas).
    Le catalogue suit le schéma : `latresne` → catalogue étendu, sinon catalogue Géoportail,
    sauf si IDENTITE_FONCIERE_CATALOGUE_PATH impose un fichier unique.
    """
    if not db_schema or not str(db_schema).strip():
        yield
        return
    s = str(db_schema).strip()
    _sql_ident(s)
    tok = _identite_schema_override.set(s)
    try:
        yield
    finally:
        _identite_schema_override.reset(tok)


def get_catalogue() -> Dict[str, Any]:
    """
    Catalogue JSON actif : override env, sinon `latresne` → étendu, sinon GPU réduit.
    """
    if _CATALOGUE_OVERRIDE is not None:
        return _CATALOGUE_OVERRIDE
    if get_identite_db_schema() == "latresne":
        return CATALOGUE_LATRESNE
    return CATALOGUE_GEOPORTAIL


def _parcelles_table_for_schema(schema: str) -> str:
    """Table des parcelles cadastrales dans le schéma (convention Kerelia / legacy Latresne)."""
    if (schema or "").strip() == "latresne":
        return "parcelles_latresne"
    return "parcelles"


def fetch_parcelle_geometry_geojson_from_db(
    *,
    idu: Optional[str] = None,
    parcelle_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Lit `geom_2154` sur la parcelle et renvoie un GeoJSON géométrie seule en WGS84 (EPSG:4326).
    Nécessite que le schéma courant (`get_identite_db_schema`) soit déjà fixé (contexte requête ou env).
    """
    schema = get_identite_db_schema()
    _sql_ident(schema)
    tbl = _parcelles_table_for_schema(schema)
    _sql_ident(tbl)

    if parcelle_id is not None:
        q = text(
            f"""
            SELECT ST_AsGeoJSON(ST_Transform(geom_2154, 4326))::text AS gj
            FROM {_pg_quote_ident(schema)}.{_pg_quote_ident(tbl)}
            WHERE geom_2154 IS NOT NULL AND id = :pid
            LIMIT 1
            """
        )
        params: Dict[str, Any] = {"pid": int(parcelle_id)}
    elif idu and str(idu).strip():
        q = text(
            f"""
            SELECT ST_AsGeoJSON(ST_Transform(geom_2154, 4326))::text AS gj
            FROM {_pg_quote_ident(schema)}.{_pg_quote_ident(tbl)}
            WHERE geom_2154 IS NOT NULL AND idu = :idu
            LIMIT 1
            """
        )
        params = {"idu": str(idu).strip()}
    else:
        raise ValueError("idu ou parcelle_id requis pour charger la géométrie depuis la base.")

    with engine.connect() as conn:
        row = conn.execute(q, params).mappings().first()
    if not row or not row.get("gj"):
        raise ValueError(
            "Parcelle introuvable ou sans géométrie (geom_2154) pour ce schéma et cette référence."
        )
    geom = json.loads(row["gj"])
    if not isinstance(geom, dict) or "type" not in geom:
        raise ValueError("Géométrie lue en base invalide.")
    return geom


def resolve_identite_fonciere_geometry(
    geometry: Optional[Dict[str, Any]] = None,
    *,
    idu: Optional[str] = None,
    parcelle_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    GeoJSON UF : soit fourni dans `geometry`, soit chargé depuis `{schema}.parcelles` (ou parcelles_latresne).
    """
    if isinstance(geometry, dict) and geometry.get("type"):
        return geometry
    return fetch_parcelle_geometry_geojson_from_db(idu=idu, parcelle_id=parcelle_id)


# Textes longs (réglementation, laius Markdown…) : non exposés par défaut sauf présents dans `keep`.
_IDENTITE_LONG_TEXT_ATTRS = frozenset({"reglementation", "laius_reglement"})


def _sql_ident(name: str) -> str:
    if not re.match(r"^[a-z_][a-z0-9_]*$", name or ""):
        raise ValueError(f"Identifiant SQL invalide: {name!r}")
    return name


def _pg_quote_ident(name: str) -> str:
    """
    Identifiant PostgreSQL entre guillemets doubles (casse et caractères préservés).
    Sans cela, PG plie les noms non quotés en minuscules et les colonnes « Ap » échouent.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Identifiant SQL invalide: {name!r}")
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(f"Identifiant SQL invalide: {name!r}")
    return '"' + name.replace('"', '""') + '"'


try:
    _sql_ident(IDENTITE_DB_SCHEMA)
except ValueError as e:
    raise ValueError(
        f"Variable IDENTITE_FONCIERE_DB_SCHEMA invalide: {IDENTITE_DB_SCHEMA!r}"
    ) from e


@dataclass
class GeoJsonLayerAttempt:
    """Résultat d’intersection pour une couche du catalogue (GeoJSON → base)."""

    table: str
    display_name: str
    status: str  # skipped | not_intersected | intersected | error
    intersected: bool
    elements_count: int = 0
    intersection: Optional[Dict[str, Any]] = None
    skip_reason: Optional[str] = None
    error: Optional[str] = None


def _fingerprint_valeur_groupe(val: Any) -> str:
    """Clé stable pour dédupliquer les valeurs d’attribut (str ou liste)."""
    if val is None:
        return ""
    if isinstance(val, list):
        return ",".join(str(v) for v in val if v is not None)
    return str(val)


def _elements_display_count(elements: List[Dict[str, Any]], config: Dict[str, Any]) -> int:
    """
    Nombre affiché pour la couche (SSE, PDF synthèse) : si le catalogue définit
    `group_by`, on compte les valeurs distinctes de cet attribut ; sinon le nombre
    d’éléments retournés.
    """
    if not elements:
        return 0
    gb = config.get("group_by")
    keys: List[str] = []
    if isinstance(gb, str) and gb.strip():
        keys = [gb.strip()]
    elif isinstance(gb, list):
        keys = [str(x).strip() for x in gb if isinstance(x, str) and x.strip()]

    if not keys:
        return len(elements)

    chosen: Optional[str] = None
    for k in keys:
        if any(k in el for el in elements):
            chosen = k
            break
    if not chosen:
        return len(elements)

    distinct: set = set()
    for el in elements:
        if chosen not in el:
            continue
        distinct.add(_fingerprint_valeur_groupe(el.get(chosen)))
    return len(distinct) if distinct else len(elements)


# ------------------------------------------------------------
# Fonctions métier
# ------------------------------------------------------------

def fetch_parcelle_geometry_ign(section: str, numero: str, insee: str) -> str:
    """
    Récupère la géométrie WKT en EPSG:2154 depuis l'IGN WFS
    
    Args:
        section: Section cadastrale (ex: "AC")
        numero: Numéro de parcelle (ex: "0042")
        insee: Code INSEE commune (ex: "33522")
    
    Returns:
        str: Géométrie au format WKT en EPSG:2154
    
    Raises:
        ValueError: Si geopandas n'est pas disponible
        requests.RequestException: Si erreur réseau
        ValueError: Si parcelle non trouvée
    """
    logger.info(f"🔍 Récupération géométrie IGN pour {section} {numero} (INSEE: {insee})")
    
    if gpd is None:
        raise ValueError("geopandas non disponible")
    
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": IGN_LAYER,
        "srsName": "EPSG:2154",
        "outputFormat": "application/json",
        "CQL_FILTER": f"code_insee='{insee}' AND section='{section}' AND numero='{numero}'"
    }
    
    logger.info(f"📡 Appel IGN WFS...")
    r = requests.get(
        IGN_WFS_ENDPOINT,
        params=params,
        timeout=30,
        verify=ssl_verify_for_requests(),
    )
    r.raise_for_status()
    logger.info(f"✅ Réponse IGN reçue ({len(r.content)} bytes)")
    
    gdf = gpd.read_file(io.BytesIO(r.content))
    
    if gdf.empty:
        raise ValueError(f"Parcelle {section} {numero} non trouvée (INSEE: {insee})")
    
    logger.info(f"✅ Géométrie extraite : {len(gdf.iloc[0].geometry.wkt)} caractères")
    return gdf.iloc[0].geometry.wkt

def get_carto_tables() -> List[str]:
    """
    Liste des couches à tester : clés du catalogue JSON (tables dans le schéma courant, voir get_identite_db_schema()).
    
    Returns:
        List[str]: Liste des noms de tables actives
    """
    logger.info("📊 Récupération des tables depuis le catalogue JSON...")
    tables = list(get_catalogue().keys())
    logger.info(f"✅ {len(tables)} tables cataloguées")
    return tables


def _resolve_discriminant_attribute(config: Dict[str, Any]) -> Optional[str]:
    """
    Détermine l'attribut discriminant à utiliser pour l'identité parcellaire.
    Ordre de priorité:
    1) attribut_disc / attribut_discriminant
    2) group_by (str ou premier élément si list)
    3) premier attribut hors textes longs (`reglementation`, `laius_reglement`) dans keep
    """
    explicit_attr = config.get("attribut_disc") or config.get("attribut_discriminant")
    if explicit_attr:
        return explicit_attr

    group_by = config.get("group_by")
    if isinstance(group_by, str) and group_by.strip():
        return group_by
    if isinstance(group_by, list) and group_by:
        candidate = group_by[0]
        if isinstance(candidate, str) and candidate.strip():
            return candidate

    keep = config.get("keep", [])
    if isinstance(keep, list):
        for attr in keep:
            if (
                isinstance(attr, str)
                and attr.strip()
                and attr.lower() not in _IDENTITE_LONG_TEXT_ATTRS
            ):
                return attr

    return None


def _attrs_sans_reglementation(attrs: List[str]) -> List[str]:
    """N'expose pas les attributs de texte long (réglementation, laius Markdown, etc.)."""
    return [
        a
        for a in attrs
        if isinstance(a, str) and a.lower() not in _IDENTITE_LONG_TEXT_ATTRS
    ]


def _elements_intersection_geometrique_seule(n: int) -> List[Dict[str, str]]:
    """Réponse minimale quand seule la géométrie compte (pas d’attributs hors réglementation)."""
    if n < 1:
        return []
    if n == 1:
        return [{"intersection": "Oui"}]
    return [{"intersection": "Oui", "entités": str(n)}]


def _attempt_geometry_only_intersection(
    conn,
    *,
    table_name: str,
    geom_json: str,
    parcelle_geom_sql: str,
    geom_col: str,
    display_name: str,
    article: Any,
    attr_disc: Optional[str],
    db_schema: Optional[str] = None,
) -> GeoJsonLayerAttempt:
    """Compte les intersections sans lecture d’attributs (keep vide ou seulement réglementation)."""
    n = _count_broad_intersect(
        conn, geom_json, table_name, parcelle_geom_sql, geom_col, db_schema=db_schema
    )
    if n is None:
        return GeoJsonLayerAttempt(
            table=table_name,
            display_name=display_name,
            status="error",
            intersected=False,
            error="Échec du comptage géométrique",
        )
    if n == 0:
        return GeoJsonLayerAttempt(
            table=table_name,
            display_name=display_name,
            status="not_intersected",
            intersected=False,
            elements_count=0,
        )
    logger.info(
        "   ✅ %s: intersection géométrique seule (%s ligne(s))",
        table_name,
        n,
    )
    els = _elements_intersection_geometrique_seule(n)
    return GeoJsonLayerAttempt(
        table=table_name,
        display_name=display_name,
        status="intersected",
        intersected=True,
        elements_count=n,
        intersection={
            "table": table_name,
            "display_name": display_name,
            "article": article,
            "attribut_discriminant": attr_disc,
            "elements": els,
        },
    )


def calculate_intersections_detailed(parcelle_wkt: str, tables: List[str] = None):
    if tables is None:
        tables = get_carto_tables()
    
    if not tables:
        return []
    
    schema_snap = get_identite_db_schema()
    catalogue_snap = get_catalogue()
    logger.info(f"🧩 Test avec attributs sur {len(tables)} tables...")
    
    def test_table(table_name):
        try:
            config = catalogue_snap.get(table_name)
            if not config:
                return None
            
            display_name = config.get("nom_affiche") or config.get("nom") or table_name
            article = config.get("article")
            keep_attrs = config.get("keep", [])
            attr_disc = _resolve_discriminant_attribute(config)

            if not isinstance(keep_attrs, list):
                keep_attrs = []
            keep_attrs = [a for a in keep_attrs if isinstance(a, str) and a.strip()]
            if attr_disc and attr_disc not in keep_attrs:
                keep_attrs = [attr_disc, *keep_attrs]

            if not keep_attrs:
                return None
            
            with engine.connect() as conn:
                _sql_ident(table_name)
                geom_col = _find_geom_column(conn, table_name, schema_snap)
                if not geom_col:
                    return None

                existing_cols_query = text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                    AND table_name = :tbl
                """)
                existing_cols = {
                    row[0]
                    for row in conn.execute(
                        existing_cols_query,
                        {"tbl": table_name, "schema": schema_snap},
                    )
                }
                selected_attrs = [attr for attr in keep_attrs if attr in existing_cols]

                if not selected_attrs:
                    return None

                has_reg = any(
                    isinstance(a, str) and a.lower() in _IDENTITE_LONG_TEXT_ATTRS
                    for a in selected_attrs
                )
                # Par défaut on évite les textes longs (`reglementation`, `laius_reglement`, …),
                # mais s'ils sont dans `keep`, on les récupère (ex. PDF / annexe).
                output_attrs = (
                    selected_attrs if has_reg else _attrs_sans_reglementation(selected_attrs)
                )
                if not output_attrs:
                    n = _count_wkt_intersect(conn, table_name, parcelle_wkt, geom_col, db_schema=schema_snap)
                    if not n:
                        return None
                    logger.info(
                        "   ✅ %s: intersection géométrique seule (%s ligne(s))",
                        table_name,
                        n,
                    )
                    return {
                        "table": table_name,
                        "display_name": display_name,
                        "article": article,
                        "attribut_discriminant": attr_disc,
                        "elements": _elements_intersection_geometrique_seule(n),
                    }

                q = _pg_quote_ident
                selected_expr = ", ".join(
                    [f"{q(attr)} AS {q(attr)}" for attr in output_attrs]
                )
                query = text(f"""
                    SELECT DISTINCT {selected_expr}
                    FROM {schema_snap}.{table_name} t
                    WHERE t.{geom_col} && ST_Expand(ST_GeomFromText(:wkt, 2154), 1000)
                    AND ST_Intersects(t.{geom_col}, ST_GeomFromText(:wkt, 2154))
                """)

                result = conn.execute(query, {"wkt": parcelle_wkt})
                elements = []
                seen = set()
                for row in result.mappings():
                    obj = {}
                    for attr in output_attrs:
                        value = row.get(attr)
                        if value is None:
                            continue
                        if isinstance(value, list):
                            normalized = [str(v) for v in value if v is not None]
                            if normalized:
                                obj[attr] = normalized
                        else:
                            obj[attr] = str(value)

                    if not obj:
                        continue

                    signature = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                    if signature in seen:
                        continue
                    seen.add(signature)
                    elements.append(obj)

                if elements:
                    logger.info(f"   ✅ {table_name}: {len(elements)} élément(s)")
                    return {
                        "table": table_name,
                        "display_name": display_name,
                        "article": article,
                        "attribut_discriminant": attr_disc,
                        "elements": elements
                    }
        except Exception as e:
            logger.error(f"   ❌ {table_name}: {e}")
        return None
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = [r for r in executor.map(test_table, tables) if r]
    
    logger.info(f"🎯 {len(results)} couches intersectées")
    return results


def _first_xy_pair(coords: Any) -> Optional[Tuple[float, float]]:
    """Premier couple (x,y) numérique trouvé dans l'arbre coordinates GeoJSON."""
    if isinstance(coords, list):
        if (
            len(coords) >= 2
            and isinstance(coords[0], (int, float))
            and isinstance(coords[1], (int, float))
        ):
            return (float(coords[0]), float(coords[1]))
        for item in coords:
            got = _first_xy_pair(item)
            if got:
                return got
    return None


def _log_geojson_geometry_hints(geom: Dict[str, Any]) -> None:
    """Aide au diagnostic CRS : coordonnées typiques 4326 vs Web Mercator."""
    t = geom.get("type")
    pair = _first_xy_pair(geom.get("coordinates"))
    if pair:
        x, y = pair
        logger.info(
            "   [debug] indice coordonnées: type=%s, premier (x,y)≈(%s, %s) — "
            "WGS84 attendu env. lon∈[-180,180], lat∈[-90,90] ; si valeurs ~ millions, souvent EPSG:3857",
            t,
            x,
            y,
        )


def _diagnostic_parcelle_geojson(conn, geom_json: str, parcelle_geom_sql: str) -> None:
    """Validité, aire, enveloppe de l'UF en 2154 (selon parcelle_geom_sql)."""
    diag = text(f"""
        SELECT
            ST_IsValid(g) AS is_valid,
            ST_IsEmpty(g) AS is_empty,
            ROUND(ST_Area(g)::numeric, 2) AS area_m2,
            ST_AsText(ST_Envelope(g)) AS envelope_2154,
            LEFT(ST_AsText(g), 200) AS wkt_prefix
        FROM (
            SELECT {parcelle_geom_sql} AS g
        ) t
    """)
    row = conn.execute(diag, {"geom_json": geom_json}).mappings().first()
    if row:
        logger.info(
            "   [debug] parcelle 2154: valid=%s empty=%s area_m2=%s env=%s",
            row["is_valid"],
            row["is_empty"],
            row["area_m2"],
            row["envelope_2154"],
        )
        logger.info("   [debug] WKT (200 premiers car.): %s", row["wkt_prefix"])


def _count_broad_intersect(
    conn,
    geom_json: str,
    table_name: str,
    parcelle_geom_sql: str,
    geom_col: str,
    *,
    db_schema: Optional[str] = None,
) -> Optional[int]:
    """Compte les lignes qui intersectent (sans filtre attributs), pour le diagnostic spatial."""
    sch = db_schema if db_schema is not None else get_identite_db_schema()
    _sql_ident(table_name)
    _sql_ident(geom_col)
    _sql_ident(sch)
    q = text(f"""
        WITH parcelle AS (
            SELECT {parcelle_geom_sql} AS geom_2154
        )
        SELECT COUNT(*)::int AS n
        FROM {sch}.{table_name} t, parcelle p
        WHERE t.{geom_col} && ST_Expand(p.geom_2154, 1000)
        AND ST_Intersects(t.{geom_col}, p.geom_2154)
    """)
    try:
        return conn.execute(q, {"geom_json": geom_json}).scalar()
    except Exception as e:
        logger.warning("   [debug] count intersect %s: %s", table_name, e)
        return None


def _count_wkt_intersect(
    conn,
    table_name: str,
    parcelle_wkt: str,
    geom_col: str,
    *,
    db_schema: Optional[str] = None,
) -> Optional[int]:
    """Compte les lignes intersectant la parcelle (WKT 2154), sans filtre attributs."""
    sch = db_schema if db_schema is not None else get_identite_db_schema()
    _sql_ident(table_name)
    _sql_ident(geom_col)
    _sql_ident(sch)
    q = text(f"""
        SELECT COUNT(*)::int AS n
        FROM {sch}.{table_name} t
        WHERE t.{geom_col} && ST_Expand(ST_GeomFromText(:wkt, 2154), 1000)
        AND ST_Intersects(t.{geom_col}, ST_GeomFromText(:wkt, 2154))
    """)
    try:
        return conn.execute(q, {"wkt": parcelle_wkt}).scalar()
    except Exception as e:
        logger.warning("   [debug] count wkt intersect %s: %s", table_name, e)
        return None


def _detect_input_srid(parcelle_geometry: Dict[str, Any], explicit_srid: Optional[int] = None) -> int:
    """
    Détecte le SRID d'entrée.
    Priorité: valeur explicite API -> heuristique sur le 1er couple XY.
    """
    if explicit_srid in (4326, 2154, 3857):
        return explicit_srid

    pair = _first_xy_pair(parcelle_geometry.get("coordinates"))
    if not pair:
        return 4326

    x, y = pair
    if -180 <= x <= 180 and -90 <= y <= 90:
        return 4326
    # Web Mercator autour de Bordeaux: x ~ -60000, y ~ 5600000
    if abs(x) <= 20037508 and abs(y) <= 20037508:
        return 3857
    # Lambert-93 en France: x~0.2M..1.2M, y~6M..7.2M
    if 0 <= x <= 1300000 and 5800000 <= y <= 7300000:
        return 2154
    return 4326


def _build_parcelle_geom_sql(input_srid: int) -> str:
    if input_srid == 2154:
        return "ST_SetSRID(ST_GeomFromGeoJSON(:geom_json), 2154)"
    return f"ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(:geom_json), {input_srid}), 2154)"


def _find_geom_column(conn, table_name: str, schema: str) -> Optional[str]:
    """Colonne géométrie PostGIS (sans dépendre de geometry_columns, souvent absent en cloud)."""
    _sql_ident(table_name)
    _sql_ident(schema)
    preferred = ("geom_2154", "geom")
    cols_query = text("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = :schema
        AND table_name = :tbl
    """)
    rows = list(conn.execute(cols_query, {"tbl": table_name, "schema": schema}))
    by_name = {row[0]: row[1] for row in rows}
    for c in preferred:
        if c in by_name:
            return c
    for col, udt in by_name.items():
        if udt == "geometry":
            return col
    return None


def process_geojson_layer(
    table_name: str,
    geom_json: str,
    parcelle_geom_sql: str,
    *,
    debug: bool = False,
    db_schema: Optional[str] = None,
    catalogue: Optional[Dict[str, Any]] = None,
) -> GeoJsonLayerAttempt:
    """
    Intersection catalogue + GeoJSON pour une seule table.
    Utilisé par le calcul parallèle et le flux SSE (progression couche par couche).

    `db_schema` : schéma SQL explicite (ex. capturé dans le thread principal) ; requis pour
    ThreadPoolExecutor car les ContextVar ne sont pas propagés aux workers.

    `catalogue` : dict catalogue explicite (même motif : get_catalogue() dépend du schéma via ContextVar).
    """
    schema = db_schema if db_schema is not None else get_identite_db_schema()
    cat = catalogue if catalogue is not None else get_catalogue()
    _sql_ident(schema)
    try:
        config = cat.get(table_name)
        if not config:
            if debug:
                logger.info("   [debug] skip %s: absent du catalogue", table_name)
            return GeoJsonLayerAttempt(
                table=table_name,
                display_name=table_name,
                status="skipped",
                intersected=False,
                skip_reason="absent du catalogue",
            )

        display_name = config.get("nom_affiche") or config.get("nom") or table_name
        article = config.get("article")
        keep_attrs = config.get("keep", [])
        attr_disc = _resolve_discriminant_attribute(config)

        if not isinstance(keep_attrs, list):
            keep_attrs = []
        keep_attrs = [a for a in keep_attrs if isinstance(a, str) and a.strip()]
        if attr_disc and attr_disc not in keep_attrs:
            keep_attrs = [attr_disc, *keep_attrs]

        with engine.connect() as conn:
            _sql_ident(table_name)
            geom_col = _find_geom_column(conn, table_name, schema)
            if not geom_col:
                if debug:
                    logger.info(
                        "   [debug] skip %s: aucune colonne géométrique trouvée",
                        table_name,
                    )
                return GeoJsonLayerAttempt(
                    table=table_name,
                    display_name=display_name,
                    status="skipped",
                    intersected=False,
                    skip_reason="aucune colonne géométrique",
                )

            if not keep_attrs:
                if debug:
                    logger.info(
                        "   [debug] %s: keep vide → intersection géométrique seule",
                        table_name,
                    )
                return _attempt_geometry_only_intersection(
                    conn,
                    table_name=table_name,
                    geom_json=geom_json,
                    parcelle_geom_sql=parcelle_geom_sql,
                    geom_col=geom_col,
                    display_name=display_name,
                    article=article,
                    attr_disc=attr_disc,
                    db_schema=schema,
                )

            existing_cols_query = text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema
                AND table_name = :tbl
            """
            )
            existing_cols = {
                row[0]
                for row in conn.execute(
                    existing_cols_query,
                    {"tbl": table_name, "schema": schema},
                )
            }
            selected_attrs = [attr for attr in keep_attrs if attr in existing_cols]

            if not selected_attrs:
                if debug:
                    missing = [a for a in keep_attrs if a not in existing_cols]
                    logger.info(
                        "   [debug] skip %s: colonnes catalogue absentes en base → %s",
                        table_name,
                        missing,
                    )
                return GeoJsonLayerAttempt(
                    table=table_name,
                    display_name=display_name,
                    status="skipped",
                    intersected=False,
                    skip_reason="colonnes catalogue absentes en base",
                )

            has_reg = any(
                isinstance(a, str) and a.lower() in _IDENTITE_LONG_TEXT_ATTRS
                for a in selected_attrs
            )
            # Par défaut on évite les textes longs ; si dans `keep`, on les récupère (ex. PDF).
            output_attrs = (
                selected_attrs if has_reg else _attrs_sans_reglementation(selected_attrs)
            )
            if not output_attrs:
                return _attempt_geometry_only_intersection(
                    conn,
                    table_name=table_name,
                    geom_json=geom_json,
                    parcelle_geom_sql=parcelle_geom_sql,
                    geom_col=geom_col,
                    display_name=display_name,
                    article=article,
                    attr_disc=attr_disc,
                    db_schema=schema,
                )

            q = _pg_quote_ident
            selected_expr = ", ".join(
                [f"{q(attr)} AS {q(attr)}" for attr in output_attrs]
            )
            query = text(f"""
                WITH parcelle AS (
                    SELECT {parcelle_geom_sql} AS geom_2154
                )
                SELECT DISTINCT {selected_expr}
                FROM {schema}.{table_name} t, parcelle p
                WHERE t.{geom_col} && ST_Expand(p.geom_2154, 1000)
                AND ST_Intersects(t.{geom_col}, p.geom_2154)
            """)

            result = conn.execute(query, {"geom_json": geom_json})
            rows = list(result.mappings())
            if debug and not rows:
                n_raw = _count_broad_intersect(
                    conn, geom_json, table_name, parcelle_geom_sql, geom_col, db_schema=schema
                )
                logger.info(
                    "   [debug] %s: 0 ligne DISTINCT mais intersect géom=%s (si géom>0, attrs tous NULL ?)",
                    table_name,
                    n_raw,
                )

            elements = []
            seen = set()
            for row in rows:
                obj = {}
                for attr in output_attrs:
                    value = row.get(attr)
                    if value is None:
                        continue
                    if isinstance(value, list):
                        normalized = [str(v) for v in value if v is not None]
                        if normalized:
                            obj[attr] = normalized
                    else:
                        obj[attr] = str(value)

                if not obj:
                    continue

                signature = json.dumps(obj, sort_keys=True, ensure_ascii=False)
                if signature in seen:
                    continue
                seen.add(signature)
                elements.append(obj)

            if elements:
                n_display = _elements_display_count(elements, config)
                if n_display != len(elements):
                    logger.info(
                        "   ✅ %s: %s valeur(s) distincte(s) (group_by) sur %s ligne(s)",
                        table_name,
                        n_display,
                        len(elements),
                    )
                else:
                    logger.info(f"   ✅ {table_name}: {len(elements)} élément(s)")
                return GeoJsonLayerAttempt(
                    table=table_name,
                    display_name=display_name,
                    status="intersected",
                    intersected=True,
                    elements_count=n_display,
                    intersection={
                        "table": table_name,
                        "display_name": display_name,
                        "article": article,
                        "attribut_discriminant": attr_disc,
                        "elements": elements,
                    },
                )

            return GeoJsonLayerAttempt(
                table=table_name,
                display_name=display_name,
                status="not_intersected",
                intersected=False,
                elements_count=0,
            )
    except Exception as e:
        logger.error(f"   ❌ {table_name}: {e}")
        return GeoJsonLayerAttempt(
            table=table_name,
            display_name=table_name,
            status="error",
            intersected=False,
            error=str(e),
        )


def calculate_intersections_detailed_from_geojson(
    parcelle_geometry: Dict[str, Any],
    tables: List[str] = None,
    srid: Optional[int] = None,
):
    if tables is None:
        tables = get_carto_tables()

    if not tables:
        return []

    debug = _debug_identite_fonciere()
    schema_snap = get_identite_db_schema()
    catalogue_snap = get_catalogue()
    logger.info(f"🧩 Test avec attributs sur {len(tables)} tables (GeoJSON)...")
    geom_json = json.dumps(parcelle_geometry, ensure_ascii=False)
    input_srid = _detect_input_srid(parcelle_geometry, srid)
    parcelle_geom_sql = _build_parcelle_geom_sql(input_srid)
    logger.info(f"   → SRID entrée détecté: EPSG:{input_srid} (reprojection vers EPSG:2154)")
    logger.info(f"   → Schéma BDD des couches: {schema_snap}")

    if debug:
        logger.info(
            "   [debug] IDENTITE_FONCIERE_DEBUG actif | GeoJSON %d car., type=%s",
            len(geom_json),
            parcelle_geometry.get("type"),
        )
        _log_geojson_geometry_hints(parcelle_geometry)
        with engine.connect() as conn:
            _diagnostic_parcelle_geojson(conn, geom_json, parcelle_geom_sql)
            # Couche de référence souvent présente en base
            for ref in ("zonage_plu", "prescriptions_surf", "infos_surf", "plu_latresne", "prescriptions_surf_latresne"):
                if ref in catalogue_snap:
                    gcol = _find_geom_column(conn, ref, schema_snap)
                    if not gcol:
                        logger.info("   [debug] pas de colonne géom pour %s", ref)
                        break
                    n = _count_broad_intersect(
                        conn, geom_json, ref, parcelle_geom_sql, gcol, db_schema=schema_snap
                    )
                    logger.info("   [debug] intersect brut (sans attrs) %s → %s lignes", ref, n)
                    break

    def test_table(table_name):
        att = process_geojson_layer(
            table_name,
            geom_json,
            parcelle_geom_sql,
            debug=debug,
            db_schema=schema_snap,
            catalogue=catalogue_snap,
        )
        if att.status == "intersected" and att.intersection:
            return att.intersection
        return None

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = [r for r in executor.map(test_table, tables) if r]

    logger.info(f"🎯 {len(results)} couches intersectées")
    return results

# ------------------------------------------------------------
# Fonction principale (orchestration)
# ------------------------------------------------------------

def analyser_identite_parcelle(
    section: str,
    numero: str,
    insee: str,
    commune: str
) -> Dict:
    """
    Fonction principale : orchestre l'analyse complète d'identité parcellaire
    
    Args:
        section: Section cadastrale
        numero: Numéro de parcelle
        insee: Code INSEE
        commune: Nom de la commune
    
    Returns:
        Dict: Résultat complet de l'analyse
    
    Raises:
        Exception: En cas d'erreur métier
    """
    logger.info(f"🚀 Début analyse identité parcellaire : {section} {numero} ({commune}, INSEE: {insee})")
    
    # 1. Normalisation
    section = section.upper().strip()
    numero = numero.zfill(4)
    logger.info(f"   → Normalisé : {section} {numero}")
    
    # 2. Récupération géométrie
    parcelle_wkt = fetch_parcelle_geometry_ign(section, numero, insee)
    
    # 3. Calcul intersections avec attributs discriminants
    intersections = calculate_intersections_detailed(parcelle_wkt)
    
    # 4. Tri alphabétique
    intersections.sort(key=lambda x: x["display_name"])
    
    result = {
        "parcelle": f"{section} {numero}",
        "commune": commune,
        "insee": insee,
        "nb_intersections": len(intersections),
        "intersections": intersections
    }
    
    logger.info(f"✅ Analyse terminée : {len(intersections)} intersection(s) trouvée(s)")
    return result


def analyser_identite_fonciere(
    geometry: Dict[str, Any],
    commune: str,
    insee: str | None = None,
    srid: Optional[int] = None,
) -> Dict:
    """
    Analyse d'identité foncière à partir d'une géométrie GeoJSON (UF).
    """
    logger.info(f"🚀 Début analyse identité foncière (commune={commune}, insee={insee})")

    if not isinstance(geometry, dict) or "type" not in geometry:
        raise ValueError("La géométrie GeoJSON est invalide")

    intersections = calculate_intersections_detailed_from_geojson(geometry, srid=srid)
    intersections.sort(key=lambda x: x["display_name"])

    result = {
        "parcelle": "UNITE_FONCIERE",
        "commune": commune,
        "insee": insee or "",
        "nb_intersections": len(intersections),
        "intersections": intersections
    }

    logger.info(f"✅ Analyse foncière terminée : {len(intersections)} intersection(s) trouvée(s)")
    return result


def iter_identite_fonciere_sse_events(
    parcelle_geometry: Dict[str, Any],
    commune: str,
    insee: Optional[str],
    srid: Optional[int],
) -> Iterator[Dict[str, Any]]:
    """
    Flux d’événements JSON pour SSE : init (liste des couches), layer_done (résultat par couche),
    puis complete avec le même corps métier que `analyser_identite_fonciere` (tri par display_name).
    """
    if not isinstance(parcelle_geometry, dict) or "type" not in parcelle_geometry:
        raise ValueError("La géométrie GeoJSON est invalide")

    tables = get_carto_tables()
    catalogue_snap = get_catalogue()
    layers_meta = []
    for t in tables:
        cfg = catalogue_snap.get(t) or {}
        layers_meta.append(
            {
                "table": t,
                "display_name": cfg.get("nom_affiche") or cfg.get("nom") or t,
            }
        )

    yield {
        "type": "init",
        "commune": commune,
        "insee": insee or "",
        "total_layers": len(layers_meta),
        "layers": layers_meta,
    }

    schema_snap = get_identite_db_schema()
    geom_json = json.dumps(parcelle_geometry, ensure_ascii=False)
    input_srid = _detect_input_srid(parcelle_geometry, srid)
    parcelle_geom_sql = _build_parcelle_geom_sql(input_srid)
    debug = _debug_identite_fonciere()

    intersections_accum: List[Dict[str, Any]] = []
    for table_name in tables:
        att = process_geojson_layer(
            table_name,
            geom_json,
            parcelle_geom_sql,
            debug=debug,
            db_schema=schema_snap,
            catalogue=catalogue_snap,
        )
        yield {
            "type": "layer_done",
            "table": att.table,
            "display_name": att.display_name,
            "status": att.status,
            "intersected": att.intersected,
            "elements_count": att.elements_count,
            "intersection": att.intersection,
            "skip_reason": att.skip_reason,
            "error": att.error,
        }
        if att.status == "intersected" and att.intersection:
            intersections_accum.append(att.intersection)

    intersections_sorted = sorted(
        intersections_accum, key=lambda x: x["display_name"]
    )
    result = {
        "parcelle": "UNITE_FONCIERE",
        "commune": commune,
        "insee": insee or "",
        "nb_intersections": len(intersections_sorted),
        "intersections": intersections_sorted,
    }
    yield {"type": "complete", "success": True, **result}