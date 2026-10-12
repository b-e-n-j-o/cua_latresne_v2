"""GET /api/communes/{schema}/autorisations — GeoJSON + fiches + historique parcelle."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response
from fastapi.responses import JSONResponse

from api.autorisations.service import (
    CACHE_HEADERS,
    dumps,
    fetch_fiche,
    fetch_geojson,
    fetch_non_localisees,
    fetch_par_parcelle,
    validate_schema,
)
from api.reglements.router_reglements import get_pool
from services.auth.commune_access import assert_authorized_for_commune_slug
from services.auth.current_user import AuthenticatedUser, get_current_user

import asyncpg

router = APIRouter(prefix="/api/communes/{schema}", tags=["autorisations-urbanisme"])


def schema_dep(schema: str = Path(..., description="Schéma communal, ex. latresne")) -> str:
    return validate_schema(schema)


Schema = Annotated[str, Depends(schema_dep)]
Pool = Annotated[asyncpg.Pool, Depends(get_pool)]
User = Annotated[AuthenticatedUser, Depends(get_current_user)]


def _guard(user: AuthenticatedUser, schema: str) -> None:
    assert_authorized_for_commune_slug(user.id, schema)


@router.get("/autorisations")
async def list_autorisations(schema: Schema, pool: Pool, user: User):
    _guard(user, schema)
    payload = await fetch_geojson(pool, schema)
    return Response(dumps(payload), media_type="application/json", headers=CACHE_HEADERS)


@router.get("/autorisations/non-localisees")
async def list_non_localisees(schema: Schema, pool: Pool, user: User):
    _guard(user, schema)
    rows = await fetch_non_localisees(pool, schema)
    return JSONResponse(content=json_ready(rows), headers=CACHE_HEADERS)


@router.get("/autorisations/{aut_id}")
async def get_autorisation(schema: Schema, pool: Pool, user: User, aut_id: str):
    _guard(user, schema)
    data = await fetch_fiche(pool, schema, aut_id)
    return JSONResponse(content=json_ready(data), headers=CACHE_HEADERS)


@router.get("/parcelles/{idu}/autorisations")
async def autorisations_parcelle(
    schema: Schema,
    pool: Pool,
    user: User,
    idu: str,
    depuis: Annotated[int | None, Query(ge=1900, le=2100)] = None,
):
    _guard(user, schema)
    rows = await fetch_par_parcelle(pool, schema, idu.strip().upper(), depuis)
    return JSONResponse(content=json_ready(rows), headers=CACHE_HEADERS)


def json_ready(payload):
    import json
    from api.autorisations.service import dumps as _dumps
    return json.loads(_dumps(payload))
