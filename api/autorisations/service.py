"""SQL des autorisations d'urbanisme (schéma interpolé après liste blanche)."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import asyncpg
from fastapi import HTTPException

SCHEMAS = frozenset({"latresne", "argeles"})
CACHE_HEADERS = {"Cache-Control": "private, no-store"}


def validate_schema(schema: str) -> str:
    s = (schema or "").strip().lower()
    if s not in SCHEMAS:
        raise HTTPException(status_code=404, detail="Commune inconnue")
    return s


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value))


def dumps(payload: Any) -> str:
    return json.dumps(payload, default=_json_default, ensure_ascii=False)


_PROPS = """
json_build_object(
  'id', a.id, 'categorie', a.categorie, 'type_dau', a.type_dau,
  'etat', coalesce(a.etat, 'inconnu'),
  'annee', extract(year from coalesce(a.date_autorisation, a.date_depot))::int,
  'date', coalesce(a.date_autorisation, a.date_depot),
  'geoloc_statut', a.geoloc_statut,
  'nb_logements', a.nb_logements, 'surface_creee', a.surface_creee,
  'sort_key', extract(epoch from coalesce(a.date_autorisation, a.date_depot))::bigint
)
"""


def sql_fc(schema: str, geom_expr: str) -> str:
    return f"""
select json_build_object(
  'type', 'FeatureCollection',
  'features', coalesce(json_agg(json_build_object(
    'type', 'Feature',
    'id', a.id,
    'geometry', st_asgeojson(st_transform(
      case when st_srid({geom_expr}) = 0 then st_setsrid({geom_expr}, 2154) else {geom_expr} end,
      4326), 6)::json,
    'properties', {_PROPS}
  )), '[]'::json))
from {schema}.autorisation a
where {geom_expr} is not null
"""


SQL_NON_LOC = """
select a.id, a.categorie, a.type_dau, coalesce(a.etat, 'inconnu') as etat,
       coalesce(a.date_autorisation, a.date_depot) as date,
       a.adresse, a.geoloc_statut, a.nature_projet,
       (select string_agg(distinct ap.idu, ', ' order by ap.idu)
          from {schema}.autorisation_parcelle ap
         where ap.autorisation_id = a.id) as parcelles_declarees
from {schema}.autorisation a
where a.geoloc_statut in ('non_localise', 'sans_reference')
order by coalesce(a.date_autorisation, a.date_depot) desc nulls last, a.id
"""

SQL_FICHE = """
select row_to_json(t) from (
  select a.*,
    (select coalesce(json_agg(json_build_object(
        'idu', ap.idu, 'methode', ap.methode,
        'idu_origine', ap.idu_origine, 'millesime_origine', ap.millesime_origine
      ) order by ap.rang), '[]'::json)
     from {schema}.autorisation_parcelle ap
     where ap.autorisation_id = a.id) as parcelles
  from {schema}.autorisation a
  where a.id = $1
) t
"""

SQL_PARCELLE = """
select a.id, a.categorie, a.type_dau, a.etat, a.date_depot, a.date_autorisation,
       a.nature_projet, a.nb_logements, a.surface_creee, a.geoloc_statut,
       ap.methode, ap.idu_origine, ap.millesime_origine
from {schema}.autorisation_parcelle ap
join {schema}.autorisation a on a.id = ap.autorisation_id
where ap.idu = $1
order by coalesce(a.date_autorisation, a.date_depot) desc nulls last
"""


async def fetch_geojson(pool: asyncpg.Pool, schema: str) -> dict:
    async with pool.acquire() as conn:
        points = await conn.fetchval(sql_fc(schema, "a.pt"))
        emprises = await conn.fetchval(sql_fc(schema, "a.geom"))
    return {
        "points": _as_obj(points) or {"type": "FeatureCollection", "features": []},
        "emprises": _as_obj(emprises) or {"type": "FeatureCollection", "features": []},
    }


async def fetch_non_localisees(pool: asyncpg.Pool, schema: str) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(SQL_NON_LOC.format(schema=schema))
    return [dict(r) for r in rows]


async def fetch_fiche(pool: asyncpg.Pool, schema: str, aut_id: str) -> dict:
    async with pool.acquire() as conn:
        raw = await conn.fetchval(SQL_FICHE.format(schema=schema), aut_id)
    if raw is None:
        raise HTTPException(status_code=404, detail="Autorisation introuvable")
    data = _as_obj(raw)
    if not data.get("siren"):
        data.pop("demandeur", None)
        data.pop("siren", None)
    data.pop("geom", None)
    data.pop("pt", None)
    return data


async def fetch_par_parcelle(
    pool: asyncpg.Pool, schema: str, idu: str, depuis: int | None = None
) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(SQL_PARCELLE.format(schema=schema), idu)
    out = [dict(r) for r in rows]
    if depuis is not None:
        filtered = []
        for row in out:
            d = row.get("date_autorisation") or row.get("date_depot")
            year = None
            if isinstance(d, date):
                year = d.year
            elif isinstance(d, str) and len(d) >= 4:
                try:
                    year = int(d[:4])
                except ValueError:
                    year = None
            if year is None or year >= depuis:
                filtered.append(row)
        return filtered
    return out


def compact_parcelle_rows(rows: list[dict]) -> list[dict]:
    compact = []
    for row in rows:
        d = row.get("date_autorisation") or row.get("date_depot")
        compact.append({
            "id": row.get("id"),
            "type": row.get("type_dau"),
            "date": d.isoformat() if isinstance(d, date) else d,
            "etat": row.get("etat"),
            "nature": row.get("nature_projet"),
            "nb_logements": row.get("nb_logements"),
            "methode": row.get("methode"),
        })
    return compact


def _as_obj(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, str):
        return json.loads(raw)
    return json.loads(dumps(raw))
