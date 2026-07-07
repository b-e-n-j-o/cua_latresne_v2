"""
router_reglements.py
--------------------
Router FastAPI pour le back-office d'édition des règlements par commune.

Chaque commune dispose d'un catalogue JSON dans ``catalogues/{slug}.json``
(schema Postgres, tables, colonnes éditables). Le frontend lit
GET /communes/{slug}/reglements/sources pour se configurer.

Exemple Argelès : /communes/argeles/reglements/sources

Variables d'environnement :
    DATABASE_URL / SUPABASE_*   connexion Postgres
    ADMIN_API_TOKEN             jeton Bearer (optionnel en dev)
"""

from __future__ import annotations

import json
import os
import re
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Optional

import asyncpg
from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field

from services.auth.commune_access import assert_authorized_for_commune_slug
from services.auth.current_user import get_current_user_id

Kind = Literal["text", "longtext", "bool", "date", "int", "json"]
WriteRole = Literal["superadmin"]

_CATALOG_DIR = Path(__file__).resolve().parent / "catalogues"
_ADMIN_CATALOG_DIR = _CATALOG_DIR / "admin"
_CATALOG_FILES: dict[str, Path] = {
    "argeles": _CATALOG_DIR / "argeles.json",
    "latresne": _CATALOG_DIR / "latresne.json",
}


def _is_catalog_json_file(path: Path) -> bool:
    """Ignore fichiers cachés / AppleDouble (._*) sur volumes macOS."""
    name = path.name
    if not name.endswith(".json") or name.startswith(".") or name.startswith("._"):
        return False
    return True


def _discover_admin_catalog_files() -> dict[str, Path]:
    files: dict[str, Path] = {}
    if not _ADMIN_CATALOG_DIR.is_dir():
        return files
    for path in sorted(_ADMIN_CATALOG_DIR.glob("*.json")):
        if not _is_catalog_json_file(path):
            continue
        slug = path.stem.strip().lower()
        if not slug or not re.fullmatch(r"[a-z][a-z0-9_-]*", slug):
            continue
        files[slug] = path
    return files


_ADMIN_CATALOG_FILES: dict[str, Path] = _discover_admin_catalog_files()


class Col(BaseModel):
    name: str
    label: str
    kind: Kind = "text"
    editable: bool = True
    creatable: bool = True
    pk: bool = False


class Source(BaseModel):
    source: str
    label: str
    table: str
    schema: Optional[str] = None
    pk: str
    list_primary: str
    list_secondary: Optional[str] = None
    search_cols: list[str]
    columns: list[Col]
    aggregated: bool = False
    creatable: bool = True
    deletable: bool = True
    write_role: Optional[WriteRole] = None


def can_write_source(src: Source, user_id: str) -> bool:
    """Écriture autorisée si pas de write_role, ou si le rôle utilisateur correspond."""
    if not src.write_role:
        return True
    from services.auth.commune_access import is_superadmin

    if src.write_role == "superadmin":
        return is_superadmin(user_id)
    return False


def apply_source_permissions(src: Source, user_id: str) -> Source:
    """Masque creatable/editable/deletable pour les utilisateurs sans droit d'écriture."""
    if can_write_source(src, user_id):
        return src
    return src.model_copy(
        update={
            "creatable": False,
            "deletable": False,
            "columns": [
                c.model_copy(update={"editable": False, "creatable": False})
                for c in src.columns
            ],
        }
    )


def assert_can_write_source(src: Source, user_id: str) -> None:
    if not can_write_source(src, user_id):
        raise HTTPException(
            status_code=403,
            detail="Modification réservée aux superadministrateurs Kerelia.",
        )


class CommuneCatalog(BaseModel):
    commune_slug: str
    schema: str
    label: str
    source_order: list[str] = Field(default_factory=list)
    sources: list[Source]
    enabled: bool = True
    disabled_message: Optional[str] = None

    @property
    def registry(self) -> dict[str, Source]:
        return {s.source: s for s in self.sources}

    def ordered_sources(self) -> list[Source]:
        reg = self.registry
        if self.source_order:
            ordered = [reg[s] for s in self.source_order if s in reg]
            extras = [s for key, s in reg.items() if key not in self.source_order]
            return ordered + extras
        return list(reg.values())


def _load_catalog_from_path(path: Path) -> CommuneCatalog:
    data = json.loads(path.read_text(encoding="utf-8"))
    catalog = CommuneCatalog.model_validate(data)
    _validate_schema_name(catalog.schema, label="Schéma catalogue")
    return catalog


def _load_catalog(commune_slug: str) -> CommuneCatalog:
    slug = (commune_slug or "").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", slug):
        raise HTTPException(status_code=400, detail=f"Slug commune invalide : {commune_slug!r}")
    path = _CATALOG_FILES.get(slug)
    if not path or not path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Catalogue règlements introuvable pour la commune : {commune_slug}",
        )
    return _load_catalog_from_path(path)


def _load_admin_catalog(commune_slug: str) -> CommuneCatalog:
    slug = (commune_slug or "").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]*", slug):
        raise HTTPException(status_code=400, detail=f"Slug commune invalide : {commune_slug!r}")
    path = _ADMIN_CATALOG_FILES.get(slug) or (_ADMIN_CATALOG_DIR / f"{slug}.json")
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Catalogue admin introuvable pour la commune : {commune_slug}",
        )
    return _load_catalog_from_path(path)


def _list_admin_catalog_summaries() -> list[dict]:
    out: list[dict] = []
    for slug, path in sorted(_ADMIN_CATALOG_FILES.items()):
        if not path.is_file():
            continue
        catalog = _load_catalog_from_path(path)
        out.append(
            {
                "commune_slug": catalog.commune_slug,
                "label": catalog.label,
                "schema": catalog.schema,
                "enabled": catalog.enabled,
                "disabled_message": catalog.disabled_message,
                "source_count": len(catalog.sources),
            }
        )
    return out


_pool: Optional[asyncpg.Pool] = None


def _database_dsn() -> str:
    dsn = (os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn.replace("postgresql+psycopg2://", "postgresql://").replace(
            "postgresql+psycopg://", "postgresql://"
        )
    host = (os.getenv("SUPABASE_HOST") or "").strip().strip('"').strip("'")
    db = (os.getenv("SUPABASE_DB") or "").strip().strip('"').strip("'")
    user = (os.getenv("SUPABASE_USER") or "").strip().strip('"').strip("'")
    password = (os.getenv("SUPABASE_PASSWORD") or "").strip().strip('"').strip("'")
    port = (os.getenv("SUPABASE_PORT") or "5432").strip().strip('"').strip("'")
    if host and "pooler.supabase.com" in host and port == "5432":
        port = "6543"
    if not all([host, db, user, password]):
        raise RuntimeError("DATABASE_URL ou variables SUPABASE_* requises")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _uses_pgbouncer(dsn: str) -> bool:
    return "pooler.supabase.com" in dsn or ":6543" in dsn


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = _database_dsn()
        pool_kwargs: dict[str, Any] = {"min_size": 1, "max_size": 5}
        if _uses_pgbouncer(dsn):
            pool_kwargs["statement_cache_size"] = 0
        _pool = await asyncpg.create_pool(dsn, **pool_kwargs)
    return _pool


def require_editor(authorization: Optional[str] = Header(default=None)) -> None:
    expected = os.getenv("ADMIN_API_TOKEN")
    if not expected:
        return
    if authorization != f"Bearer {expected}":
        raise HTTPException(status_code=401, detail="Jeton admin invalide")


def require_superadmin(authorization: Optional[str] = Header(default=None)) -> str:
    """JWT Supabase superadmin, ou ADMIN_API_TOKEN en dev."""
    expected = os.getenv("ADMIN_API_TOKEN")
    if expected and authorization == f"Bearer {expected}":
        return "admin-token"

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authentification requise (Bearer token).")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token manquant.")

    from api.agents.plu_agent.routes.plu_auth import verify_supabase_access_token
    from services.auth.commune_access import assert_superadmin

    user_id = verify_supabase_access_token(token)
    assert_superadmin(user_id)
    return user_id


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _validate_schema_name(schema: str, *, label: str) -> str:
    s = schema.strip().lower()
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", s):
        raise HTTPException(status_code=500, detail=f"{label} invalide : {schema!r}")
    return s


def source_schema(catalog: CommuneCatalog, src: Source) -> str:
    """Schéma Postgres pour une source (override optionnel, ex. servitudes → public)."""
    if src.schema:
        return _validate_schema_name(src.schema, label=f"Schéma source {src.source}")
    return catalog.schema


def table_ref(schema: str, src: Source) -> str:
    return f"{qi(schema)}.{qi(src.table)}"


def get_src(registry: dict[str, Source], source: str) -> Source:
    src = registry.get(source)
    if not src:
        raise HTTPException(status_code=404, detail=f"Source inconnue : {source}")
    return src


_JSONB_COLS: dict[str, set[str]] = {
    "ppr_constantes": {"valeur"},
}


def _zonage_plu_ref(schema: str) -> str:
    return f"{qi(schema)}.{qi('zonage_plu')}"


def _ppr_ref(schema: str) -> str:
    return f"{qi(schema)}.{qi('ppr')}"


def _plu_reglement_ref(schema: str) -> str:
    return f"{qi(schema)}.{qi('plu_reglement')}"


def _zonage_libelong_agg_subquery(schema: str, alias: str = "z") -> str:
    ref = _zonage_plu_ref(schema)
    return f"""(
        SELECT libelle, string_agg(DISTINCT libelong, '/') AS libelong_agg
        FROM {ref}
        GROUP BY libelle
    ) {qi(alias)}"""


def _reglementation_missing_sql(expr: str) -> str:
    return f"({expr} IS NULL OR btrim({expr}::text) = '')"


def _has_reglementation_col(src: Source) -> bool:
    return any(c.name in ("reglementation", "reglementation_generale") for c in src.columns)


async def _fetch_plu_list(
    pool: asyncpg.Pool,
    schema: str,
    *,
    search: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
    plu = _plu_reglement_ref(schema)
    zonage = _zonage_libelong_agg_subquery(schema)
    sql = f"""
        SELECT
            p.{qi('code_zone')} AS code_zone,
            z.libelong_agg AS nom_zone,
            {_reglementation_missing_sql(f"p.{qi('reglementation')}")} AS reglementation_manquante
        FROM {plu} p
        LEFT JOIN {zonage} ON z.{qi('libelle')} = p.{qi('code_zone')}
    """
    args: list[Any] = []
    if search:
        sql += f"""
            WHERE p.{qi('code_zone')}::text ILIKE $1
               OR z.libelong_agg ILIKE $1
               OR p.{qi('resume_zone')}::text ILIKE $1
               OR p.{qi('reglementation')}::text ILIKE $1
        """
        args.append(f"%{search}%")
    sql += f" ORDER BY p.{qi('code_zone')} LIMIT {int(limit)} OFFSET {int(offset)}"
    rows = await pool.fetch(sql, *args)
    return [dict(r) for r in rows]


async def _fetch_plu_row(pool: asyncpg.Pool, schema: str, code_zone: str) -> Optional[dict]:
    plu = _plu_reglement_ref(schema)
    zonage = _zonage_libelong_agg_subquery(schema)
    sql = f"""
        SELECT
            p.{qi('code_zone')} AS code_zone,
            z.libelong_agg AS nom_zone,
            p.{qi('resume_zone')} AS resume_zone,
            p.{qi('reglementation')} AS reglementation
        FROM {plu} p
        LEFT JOIN {zonage} ON z.{qi('libelle')} = p.{qi('code_zone')}
        WHERE p.{qi('code_zone')}::text = $1
    """
    r = await pool.fetchrow(sql, str(code_zone))
    return dict(r) if r else None


async def _fetch_zonage_plu_agg(
    pool: asyncpg.Pool,
    schema: str,
    *,
    libelle: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
    list_only: bool = False,
) -> list[dict]:
    ref = _zonage_plu_ref(schema)
    if list_only:
        reg_col = (
            ", "
            + _reglementation_missing_sql("max(reglementation)")
            + " AS reglementation_manquante"
        )
    else:
        reg_col = ", max(reglementation) AS reglementation"
    sql = f"""
        SELECT
            libelle,
            string_agg(DISTINCT libelong, '/') AS libelong_agg,
            count(*)::int AS nb_entites
            {reg_col}
        FROM {ref}
    """
    args: list[Any] = []
    if libelle is not None:
        sql += " WHERE libelle = $1"
        args.append(libelle)
        sql += " GROUP BY libelle"
    else:
        sql += " GROUP BY libelle"
        if search:
            idx = len(args) + 1
            sql += f"""
                HAVING libelle ILIKE ${idx}
                    OR string_agg(DISTINCT libelong, '/') ILIKE ${idx}
                    OR max(reglementation) ILIKE ${idx}
            """
            args.append(f"%{search}%")
        sql += f" ORDER BY libelle LIMIT {int(limit)} OFFSET {int(offset)}"
    rows = await pool.fetch(sql, *args)
    return [dict(r) for r in rows]


async def _fetch_ppr_agg(
    pool: asyncpg.Pool,
    schema: str,
    *,
    label: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
    list_only: bool = False,
) -> list[dict]:
    ref = _ppr_ref(schema)
    if list_only:
        reg_col = (
            ", "
            + _reglementation_missing_sql("max(reglementation_generale)")
            + " AS reglementation_manquante"
        )
        detail_cols = ""
    else:
        reg_col = ", max(reglementation_generale) AS reglementation_generale"
        detail_cols = """
            , max(ces) AS ces
            , max(mise_hors_d_eau) AS mise_hors_d_eau
        """
    sql = f"""
        SELECT
            label,
            max(risque) AS risque,
            max(code_degre) AS code_degre,
            max(degre) AS degre,
            count(*)::int AS nb_entites
            {detail_cols}
            {reg_col}
        FROM {ref}
    """
    args: list[Any] = []
    if label is not None:
        sql += " WHERE label = $1"
        args.append(label)
        sql += " GROUP BY label"
    else:
        sql += " GROUP BY label"
        if search:
            idx = len(args) + 1
            sql += f"""
                HAVING label ILIKE ${idx}
                    OR max(risque) ILIKE ${idx}
                    OR max(code_degre) ILIKE ${idx}
                    OR max(degre) ILIKE ${idx}
                    OR max(ces) ILIKE ${idx}
                    OR max(mise_hors_d_eau) ILIKE ${idx}
                    OR max(reglementation_generale) ILIKE ${idx}
            """
            args.append(f"%{search}%")
        sql += f" ORDER BY label LIMIT {int(limit)} OFFSET {int(offset)}"
    rows = await pool.fetch(sql, *args)
    return [dict(r) for r in rows]


def _serialize_api_row(row: dict, src: Source) -> dict:
    out = dict(row)
    json_cols = _JSONB_COLS.get(src.source, set())
    for col in src.columns:
        v = out.get(col.name)
        if col.name in json_cols or col.kind == "json" or (
            col.kind == "longtext" and isinstance(v, (dict, list))
        ):
            out[col.name] = json.dumps(v, ensure_ascii=False, indent=2)
        elif col.kind == "text" and isinstance(v, (int, float, Decimal)):
            out[col.name] = str(v)
    return out


def coerce(col: Col, v: Any, *, source: str = "") -> Any:
    if v is None:
        return None
    if col.name in _JSONB_COLS.get(source, set()) or col.kind == "json":
        if isinstance(v, str):
            s = v.strip()
            if s == "":
                return None
            return json.loads(s)
        return v
    if col.kind == "bool":
        if isinstance(v, str):
            return v.strip().lower() in ("true", "t", "1", "oui", "yes", "on")
        return bool(v)
    if col.kind == "int":
        return int(v)
    if col.kind == "date":
        if v == "":
            return None
        return date.fromisoformat(v) if isinstance(v, str) else v
    if col.kind == "text" and v != "" and not isinstance(v, str):
        return str(v)
    return v


router = APIRouter(
    prefix="/communes",
    tags=["reglements-admin"],
)

admin_router = APIRouter(
    prefix="/admin/reglements",
    tags=["reglements-superadmin"],
    dependencies=[Depends(require_superadmin)],
)


async def _list_sources_for_catalog(
    catalog: CommuneCatalog,
    user_id: Optional[str] = None,
    *,
    apply_write_roles: bool = True,
) -> list[dict]:
    if not catalog.enabled:
        return []
    sources = catalog.ordered_sources()
    if apply_write_roles and user_id:
        sources = [apply_source_permissions(s, user_id) for s in sources]
    return [s.model_dump() for s in sources]


async def _list_rows_for_catalog(
    catalog: CommuneCatalog,
    source: str,
    pool: asyncpg.Pool,
    *,
    search: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> list[dict]:
    if not catalog.enabled:
        return []
    src = get_src(catalog.registry, source)
    schema = source_schema(catalog, src)
    if src.source == "plu":
        return await _fetch_plu_list(pool, catalog.schema, search=search, limit=limit, offset=offset)
    if src.aggregated and src.source == "zonage_plu":
        return await _fetch_zonage_plu_agg(
            pool, catalog.schema, search=search, limit=limit, offset=offset, list_only=True
        )
    if src.aggregated and src.source == "ppr":
        rows = await _fetch_ppr_agg(
            pool, catalog.schema, search=search, limit=limit, offset=offset, list_only=True
        )
        return [_serialize_api_row(r, src) for r in rows]
    cols: list[str] = []
    for c in (src.pk, src.list_primary, src.list_secondary):
        if c and c not in cols:
            cols.append(c)
    select_parts = [qi(c) for c in cols]
    if _has_reglementation_col(src):
        select_parts.append(
            f"{_reglementation_missing_sql(qi('reglementation'))} AS reglementation_manquante"
        )
    select = ", ".join(select_parts)
    sql = f"SELECT {select} FROM {table_ref(schema, src)}"
    args: list[Any] = []
    if search:
        ors = " OR ".join(f"{qi(c)}::text ILIKE $1" for c in src.search_cols)
        sql += f" WHERE {ors}"
        args.append(f"%{search}%")
    sql += f" ORDER BY {qi(src.list_primary)} LIMIT {int(limit)} OFFSET {int(offset)}"
    rows = await pool.fetch(sql, *args)
    if src.source in _JSONB_COLS:
        return [_serialize_api_row(dict(r), src) for r in rows]
    return [dict(r) for r in rows]


async def _get_row_for_catalog(
    catalog: CommuneCatalog,
    source: str,
    pk: str,
    pool: asyncpg.Pool,
) -> dict:
    if not catalog.enabled:
        raise HTTPException(status_code=404, detail="Catalogue indisponible")
    src = get_src(catalog.registry, source)
    schema = source_schema(catalog, src)
    if src.source == "plu":
        row = await _fetch_plu_row(pool, catalog.schema, pk)
        if not row:
            raise HTTPException(status_code=404, detail="Entrée introuvable")
        return row
    if src.aggregated and src.source == "zonage_plu":
        rows = await _fetch_zonage_plu_agg(pool, catalog.schema, libelle=pk)
        if not rows:
            raise HTTPException(status_code=404, detail="Entrée introuvable")
        return rows[0]
    if src.aggregated and src.source == "ppr":
        rows = await _fetch_ppr_agg(pool, catalog.schema, label=pk)
        if not rows:
            raise HTTPException(status_code=404, detail="Entrée introuvable")
        return _serialize_api_row(rows[0], src)
    sql = f"SELECT * FROM {table_ref(schema, src)} WHERE {qi(src.pk)}::text = $1"
    r = await pool.fetchrow(sql, str(pk))
    if not r:
        raise HTTPException(status_code=404, detail="Entrée introuvable")
    row = dict(r)
    if src.source in _JSONB_COLS:
        return _serialize_api_row(row, src)
    return row


async def _create_row_for_catalog(
    catalog: CommuneCatalog,
    source: str,
    body: dict[str, Any],
    pool: asyncpg.Pool,
    *,
    user_id: Optional[str] = None,
    enforce_write_role: bool = True,
) -> dict:
    if not catalog.enabled:
        raise HTTPException(status_code=403, detail=catalog.disabled_message or "Catalogue indisponible")
    src = get_src(catalog.registry, source)
    if enforce_write_role and user_id:
        assert_can_write_source(src, user_id)
    schema = source_schema(catalog, src)
    if not src.creatable:
        raise HTTPException(status_code=405, detail="Création non autorisée pour cette source")
    creatable = {c.name: c for c in src.columns if c.creatable}
    data = {k: coerce(creatable[k], v, source=src.source) for k, v in body.items() if k in creatable}
    if not data:
        raise HTTPException(status_code=400, detail="Aucun champ valide à insérer")
    cols = list(data.keys())
    placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
    sql = (
        f"INSERT INTO {table_ref(schema, src)} ({', '.join(qi(c) for c in cols)}) "
        f"VALUES ({placeholders}) RETURNING *"
    )
    try:
        r = await pool.fetchrow(sql, *[data[c] for c in cols])
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="Cette clé existe déjà")
    except asyncpg.NotNullViolationError as e:
        raise HTTPException(status_code=400, detail=f"Champ obligatoire manquant : {e}")
    if src.source == "plu":
        enriched = await _fetch_plu_row(pool, catalog.schema, str(r["code_zone"]))
        return enriched or dict(r)
    return dict(r)


async def _update_row_for_catalog(
    catalog: CommuneCatalog,
    source: str,
    pk: str,
    body: dict[str, Any],
    pool: asyncpg.Pool,
    *,
    user_id: Optional[str] = None,
    enforce_write_role: bool = True,
) -> dict:
    if not catalog.enabled:
        raise HTTPException(status_code=403, detail=catalog.disabled_message or "Catalogue indisponible")
    src = get_src(catalog.registry, source)
    if enforce_write_role and user_id:
        assert_can_write_source(src, user_id)
    schema = source_schema(catalog, src)
    editable = {c.name: c for c in src.columns if c.editable}
    data = {k: coerce(editable[k], v, source=src.source) for k, v in body.items() if k in editable}
    if not data:
        raise HTTPException(status_code=400, detail="Aucun champ modifiable fourni")
    if src.aggregated and src.source == "zonage_plu":
        sets, args = [], []
        for i, (k, v) in enumerate(data.items(), start=1):
            sets.append(f"{qi(k)} = ${i}")
            args.append(v)
        idx = len(args) + 1
        sql = (
            f"UPDATE {table_ref(schema, src)} SET {', '.join(sets)} "
            f"WHERE {qi(src.pk)} = ${idx}"
        )
        args.append(str(pk))
        try:
            res = await pool.execute(sql, *args)
        except asyncpg.NotNullViolationError as e:
            raise HTTPException(status_code=400, detail=f"Champ obligatoire vidé : {e}")
        if res.endswith(" 0"):
            raise HTTPException(status_code=404, detail="Entrée introuvable")
        rows = await _fetch_zonage_plu_agg(pool, catalog.schema, libelle=str(pk))
        return rows[0]
    if src.aggregated and src.source == "ppr":
        sets, args = [], []
        for i, (k, v) in enumerate(data.items(), start=1):
            sets.append(f"{qi(k)} = ${i}")
            args.append(v)
        idx = len(args) + 1
        sql = (
            f"UPDATE {table_ref(schema, src)} SET {', '.join(sets)} "
            f"WHERE {qi(src.pk)} = ${idx}"
        )
        args.append(str(pk))
        try:
            res = await pool.execute(sql, *args)
        except asyncpg.NotNullViolationError as e:
            raise HTTPException(status_code=400, detail=f"Champ obligatoire vidé : {e}")
        if res.endswith(" 0"):
            raise HTTPException(status_code=404, detail="Entrée introuvable")
        rows = await _fetch_ppr_agg(pool, catalog.schema, label=str(pk))
        return _serialize_api_row(rows[0], src)
    sets, args = [], []
    for i, (k, v) in enumerate(data.items(), start=1):
        sets.append(f"{qi(k)} = ${i}")
        args.append(v)
    if any(c.name == "updated_at" for c in src.columns):
        sets.append(f"{qi('updated_at')} = now()")
    idx = len(args) + 1
    sql = (
        f"UPDATE {table_ref(schema, src)} SET {', '.join(sets)} "
        f"WHERE {qi(src.pk)}::text = ${idx} RETURNING *"
    )
    args.append(str(pk))
    try:
        r = await pool.fetchrow(sql, *args)
    except asyncpg.NotNullViolationError as e:
        raise HTTPException(status_code=400, detail=f"Champ obligatoire vidé : {e}")
    if not r:
        raise HTTPException(status_code=404, detail="Entrée introuvable")
    if src.source == "plu":
        enriched = await _fetch_plu_row(pool, catalog.schema, str(pk))
        return enriched or dict(r)
    row = dict(r)
    if src.source in _JSONB_COLS:
        return _serialize_api_row(row, src)
    return row


async def _delete_row_for_catalog(
    catalog: CommuneCatalog,
    source: str,
    pk: str,
    pool: asyncpg.Pool,
    *,
    user_id: Optional[str] = None,
    enforce_write_role: bool = True,
) -> Response:
    if not catalog.enabled:
        raise HTTPException(status_code=403, detail=catalog.disabled_message or "Catalogue indisponible")
    src = get_src(catalog.registry, source)
    if enforce_write_role and user_id:
        assert_can_write_source(src, user_id)
    schema = source_schema(catalog, src)
    if not src.deletable:
        raise HTTPException(status_code=405, detail="Suppression non autorisée pour cette source")
    sql = f"DELETE FROM {table_ref(schema, src)} WHERE {qi(src.pk)}::text = $1"
    res = await pool.execute(sql, str(pk))
    if res.endswith(" 0"):
        raise HTTPException(status_code=404, detail="Entrée introuvable")
    return Response(status_code=204)


@router.get("/{commune_slug}/reglements/sources")
async def list_sources(
    commune_slug: str,
    user_id: str = Depends(get_current_user_id),
) -> list[dict]:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _list_sources_for_catalog(catalog, user_id)


@router.get("/{commune_slug}/reglements/{source}")
async def list_rows(
    commune_slug: str,
    source: str,
    user_id: str = Depends(get_current_user_id),
    search: Optional[str] = Query(None),
    limit: int = Query(500, le=2000),
    offset: int = Query(0, ge=0),
    pool: asyncpg.Pool = Depends(get_pool),
) -> list[dict]:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _list_rows_for_catalog(
        catalog, source, pool, search=search, limit=limit, offset=offset
    )


@router.get("/{commune_slug}/reglements/{source}/{pk}")
async def get_row(
    commune_slug: str,
    source: str,
    pk: str,
    user_id: str = Depends(get_current_user_id),
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _get_row_for_catalog(catalog, source, pk, pool)


@router.post("/{commune_slug}/reglements/{source}")
async def create_row(
    commune_slug: str,
    source: str,
    body: dict[str, Any] = Body(...),
    user_id: str = Depends(get_current_user_id),
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _create_row_for_catalog(
        catalog, source, body, pool, user_id=user_id, enforce_write_role=True
    )


@router.patch("/{commune_slug}/reglements/{source}/{pk}")
async def update_row(
    commune_slug: str,
    source: str,
    pk: str,
    body: dict[str, Any] = Body(...),
    user_id: str = Depends(get_current_user_id),
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _update_row_for_catalog(
        catalog, source, pk, body, pool, user_id=user_id, enforce_write_role=True
    )


@router.delete("/{commune_slug}/reglements/{source}/{pk}", status_code=204, response_class=Response)
async def delete_row(
    commune_slug: str,
    source: str,
    pk: str,
    user_id: str = Depends(get_current_user_id),
    pool: asyncpg.Pool = Depends(get_pool),
) -> Response:
    assert_authorized_for_commune_slug(user_id, commune_slug)
    catalog = _load_catalog(commune_slug)
    return await _delete_row_for_catalog(
        catalog, source, pk, pool, user_id=user_id, enforce_write_role=True
    )


@admin_router.get("/catalogues")
async def list_admin_catalogues() -> list[dict]:
    return _list_admin_catalog_summaries()


@admin_router.get("/{commune_slug}/sources")
async def admin_list_sources(commune_slug: str) -> list[dict]:
    catalog = _load_admin_catalog(commune_slug)
    return await _list_sources_for_catalog(catalog, apply_write_roles=False)


@admin_router.get("/{commune_slug}/{source}")
async def admin_list_rows(
    commune_slug: str,
    source: str,
    search: Optional[str] = Query(None),
    limit: int = Query(500, le=2000),
    offset: int = Query(0, ge=0),
    pool: asyncpg.Pool = Depends(get_pool),
) -> list[dict]:
    catalog = _load_admin_catalog(commune_slug)
    return await _list_rows_for_catalog(
        catalog, source, pool, search=search, limit=limit, offset=offset
    )


@admin_router.get("/{commune_slug}/{source}/{pk}")
async def admin_get_row(
    commune_slug: str,
    source: str,
    pk: str,
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    catalog = _load_admin_catalog(commune_slug)
    return await _get_row_for_catalog(catalog, source, pk, pool)


@admin_router.post("/{commune_slug}/{source}")
async def admin_create_row(
    commune_slug: str,
    source: str,
    body: dict[str, Any] = Body(...),
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    catalog = _load_admin_catalog(commune_slug)
    return await _create_row_for_catalog(
        catalog, source, body, pool, enforce_write_role=False
    )


@admin_router.patch("/{commune_slug}/{source}/{pk}")
async def admin_update_row(
    commune_slug: str,
    source: str,
    pk: str,
    body: dict[str, Any] = Body(...),
    pool: asyncpg.Pool = Depends(get_pool),
) -> dict:
    catalog = _load_admin_catalog(commune_slug)
    return await _update_row_for_catalog(
        catalog, source, pk, body, pool, enforce_write_role=False
    )


@admin_router.delete("/{commune_slug}/{source}/{pk}", status_code=204, response_class=Response)
async def admin_delete_row(
    commune_slug: str,
    source: str,
    pk: str,
    pool: asyncpg.Pool = Depends(get_pool),
) -> Response:
    catalog = _load_admin_catalog(commune_slug)
    return await _delete_row_for_catalog(
        catalog, source, pk, pool, enforce_write_role=False
    )
