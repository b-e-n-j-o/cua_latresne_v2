import json
import os

import psycopg2
from fastapi import APIRouter, HTTPException
from psycopg2 import sql

from api.cuas.argeles.sig_resume_layers import (
    LEGACY_RESUME_COLUMN,
    LAYER_COL_PREFIX,
    assemble_sig_resume,
    layer_key_from_column,
    sanitize_for_json,
)

router = APIRouter(prefix="/latresne")
communes_router = APIRouter(prefix="/communes")

SUPABASE_HOST = str(os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
SUPABASE_PORT = str(os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
if "pooler.supabase.com" in SUPABASE_HOST.lower() and SUPABASE_PORT == "5432":
    SUPABASE_PORT = "6543"

def _load_commune_table_mapping() -> dict[str, tuple[str, str]]:
    """
    Whitelist des communes autorisees.
    Format env optionnel CADASTRE_COMMUNES_TABLES:
    {"latresne":"latresne.parcelles","argeles":"argeles.parcelles"}
    """
    default_mapping = {
        "latresne": ("latresne", "parcelles"),
        "argeles": ("argeles", "parcelles"),
    }
    raw = (os.getenv("CADASTRE_COMMUNES_TABLES") or "").strip()
    if not raw:
        return default_mapping

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return default_mapping

    if not isinstance(parsed, dict):
        return default_mapping

    cleaned: dict[str, tuple[str, str]] = {}
    for slug, value in parsed.items():
        if not isinstance(slug, str):
            continue
        if not isinstance(value, str) or "." not in value:
            continue
        schema, table = value.split(".", 1)
        schema = schema.strip()
        table = table.strip()
        if not schema or not table:
            continue
        cleaned[slug.strip().lower()] = (schema, table)

    return cleaned or default_mapping


CADASTRE_TABLES = _load_commune_table_mapping()


def _resolve_table_for_commune(slug: str) -> tuple[str, str]:
    key = (slug or "").strip().lower()
    if key not in CADASTRE_TABLES:
        raise HTTPException(
            status_code=404,
            detail=f"Commune non supportee: {slug}",
        )
    return CADASTRE_TABLES[key]


def _slug_to_commune_label(slug: str) -> str:
    value = (slug or "").strip().replace("-", " ")
    if not value:
        return ""
    return value.title()


def _list_layer_columns(cur, schema_name: str, table_name: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name LIKE %s
          AND column_name <> %s
        ORDER BY column_name
        """,
        (schema_name, table_name, f"{LAYER_COL_PREFIX}%", LEGACY_RESUME_COLUMN),
    )
    return [row[0] for row in cur.fetchall()]


def _sql_sig_resume_expr(
    cur,
    schema_name: str,
    table_name: str,
    *,
    has_legacy: bool,
) -> sql.Composable | None:
    layer_cols = _list_layer_columns(cur, schema_name, table_name)
    if layer_cols:
        layer_pairs: list[sql.Composable] = []
        for col in layer_cols:
            key = layer_key_from_column(col)
            if not key:
                continue
            layer_pairs.append(
                sql.SQL("{}, {}").format(sql.Literal(key), sql.Identifier(col))
            )
        if layer_pairs:
            return sql.SQL(
                "jsonb_build_object('section', section, 'numero', numero, "
                "'idu', idu, 'contenance_m2', ROUND(contenance::numeric, 2), "
                "'layers', jsonb_strip_nulls(jsonb_build_object({})))"
            ).format(sql.SQL(", ").join(layer_pairs))
    if has_legacy:
        return sql.SQL("sig_resume")
    return None


def _table_has_column(cur, schema_name: str, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (schema_name, table_name, column_name),
    )
    return cur.fetchone() is not None


def _build_geojson_payload(schema_name: str, table_name: str, commune_label: str) -> dict:
    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )
    cur = None
    try:
        cur = conn.cursor()
        has_geom_3857 = _table_has_column(cur, schema_name, table_name, "geom_3857")

        if has_geom_3857:
            geom_expr = sql.SQL(
                """
                ST_AsGeoJSON(
                    CASE
                        WHEN geom_2154 IS NOT NULL THEN ST_Transform(geom_2154, 4326)
                        WHEN geom_3857 IS NOT NULL THEN ST_Transform(geom_3857, 4326)
                    END
                )::json
                """
            )
            where_clause = sql.SQL("geom_2154 IS NOT NULL OR geom_3857 IS NOT NULL")
        else:
            geom_expr = sql.SQL("ST_AsGeoJSON(ST_Transform(geom_2154, 4326))::json")
            where_clause = sql.SQL("geom_2154 IS NOT NULL")

        has_idu = _table_has_column(cur, schema_name, table_name, "idu")
        has_legacy_sig_resume = _table_has_column(
            cur, schema_name, table_name, LEGACY_RESUME_COLUMN
        )
        sig_resume_expr = _sql_sig_resume_expr(
            cur,
            schema_name,
            table_name,
            has_legacy=has_legacy_sig_resume,
        )

        prop_pairs: list[sql.Composable] = [
            sql.SQL("'section', section"),
            sql.SQL("'numero', numero"),
            sql.SQL("'commune', %s"),
            sql.SQL("'insee', code_insee"),
            sql.SQL("'contenance', contenance"),
        ]
        if has_idu:
            prop_pairs.append(sql.SQL("'idu', idu"))
        if sig_resume_expr is not None:
            prop_pairs.append(sql.SQL("'sig_resume', {}").format(sig_resume_expr))

        properties_expr = sql.SQL("json_build_object({})").format(
            sql.SQL(", ").join(prop_pairs)
        )

        query = sql.SQL(
            """
            SELECT json_build_object(
                'type', 'FeatureCollection',
                'features', COALESCE(
                    json_agg(
                        json_build_object(
                            'type', 'Feature',
                            'properties', {properties_expr},
                            'geometry', {geom_expr}
                        )
                    ),
                    '[]'::json
                )
            )
            FROM {schema}.{table}
            WHERE {where_clause}
            """
        ).format(
            properties_expr=properties_expr,
            geom_expr=geom_expr,
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            where_clause=where_clause,
        )
        cur.execute(query, (commune_label,))
        payload = cur.fetchone()[0]
        return payload or {"type": "FeatureCollection", "features": []}
    finally:
        if cur:
            cur.close()
        conn.close()


@router.get("/parcelles/geojson")
def get_all_parcelles():
    schema_name, table_name = _resolve_table_for_commune("latresne")
    return _build_geojson_payload(schema_name, table_name, "Latresne")


@communes_router.get("/{commune_slug}/parcelles/geojson")
def get_all_parcelles_by_commune(commune_slug: str):
    schema_name, table_name = _resolve_table_for_commune(commune_slug)
    commune_label = _slug_to_commune_label(commune_slug)
    return _build_geojson_payload(schema_name, table_name, commune_label)


def _normalize_section(value: str) -> str:
    return (value or "").strip().upper()


def _normalize_numero(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    return raw.zfill(4)


def _fetch_parcelles_resume(
    schema_name: str,
    table_name: str,
    refs: list[tuple[str, str]],
) -> list[dict]:
    """sig_resume à la demande (1–20 parcelles) — évite un GeoJSON monolithique."""
    if not refs:
        return []

    conn = psycopg2.connect(
        host=SUPABASE_HOST,
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=int(SUPABASE_PORT),
    )
    cur = None
    try:
        cur = conn.cursor()
        layer_cols = _list_layer_columns(cur, schema_name, table_name)
        has_legacy_sig_resume = _table_has_column(
            cur, schema_name, table_name, LEGACY_RESUME_COLUMN
        )

        select_cols = ["section", "numero", "idu", "contenance"]
        if layer_cols:
            select_cols.extend(layer_cols)
        if has_legacy_sig_resume:
            select_cols.append(LEGACY_RESUME_COLUMN)

        clauses = []
        params: list = []
        for section, numero in refs:
            clauses.append(
                "(upper(trim(section)) = %s AND lpad(trim(numero), 4, '0') = %s)"
            )
            params.extend([_normalize_section(section), _normalize_numero(numero)])

        query = f"""
            SELECT {", ".join(select_cols)}
            FROM {schema_name}.{table_name}
            WHERE {" OR ".join(clauses)}
        """
        cur.execute(query, params)
        rows = cur.fetchall()
        out: list[dict] = []
        for row in rows:
            section, numero, idu, contenance = row[:4]
            tail = row[4:]
            layers: dict = {}
            legacy = None
            idx = 0
            for col in layer_cols:
                value = tail[idx]
                idx += 1
                if value is not None:
                    key = layer_key_from_column(col)
                    if key:
                        layers[key] = value
            if has_legacy_sig_resume:
                legacy = tail[idx] if idx < len(tail) else None
            out.append(
                {
                    "section": section,
                    "numero": numero,
                    "idu": idu,
                    "contenance": contenance,
                    "sig_resume": assemble_sig_resume(
                        section=section,
                        numero=numero,
                        idu=idu,
                        contenance=contenance,
                        layers=layers,
                        legacy=legacy,
                    ),
                }
            )
        return out
    finally:
        if cur:
            cur.close()
        conn.close()


@communes_router.get("/{commune_slug}/parcelles/resume")
def get_parcelles_resume_by_commune(commune_slug: str, refs: str):
    """
    Résumé SIG pré-calculé pour une ou plusieurs parcelles.
    refs : « SECTION:NUMERO,SECTION:NUMERO » (max 20).
    """
    schema_name, table_name = _resolve_table_for_commune(commune_slug)
    parsed: list[tuple[str, str]] = []
    for tok in (refs or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        section, _, numero = tok.partition(":")
        if not numero:
            raise HTTPException(
                status_code=400,
                detail=f"Référence invalide {tok!r} (format SECTION:NUMERO).",
            )
        parsed.append((section.strip(), numero.strip()))

    if not parsed:
        raise HTTPException(status_code=400, detail="Paramètre refs requis.")
    if len(parsed) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 parcelles par requête.")

    items = _fetch_parcelles_resume(schema_name, table_name, parsed)
    return sanitize_for_json({"items": items})