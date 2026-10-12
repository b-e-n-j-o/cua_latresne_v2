"""
Routeur — Historique & filiation parcellaire.

Toute la logique métier est en SQL (schéma filiation_api). Ce routeur :
  * traduit la commune de la route en code INSEE (cloisonnement) ;
  * valide les entrées (format idu, plafonds, bornes lon/lat) ;
  * relaie le jsonb renvoyé par Postgres sans le re-parser ;
  * sert les tuiles MVT avec les en-têtes de cache de la spec.

Convention SQL : NULL = introuvable ou hors commune → 404 ; '[]' = vide → 200.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import date
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response

from api.reglements.router_reglements import get_pool

# À ADAPTER : reprendre le mapping qui choisit aujourd'hui le schéma latresne / argeles.
COMMUNES: dict[str, str] = {
    "latresne": "33234",
    "argeles": "66008",
}

# INSEE (2A/2B gérés) + préfixe (3) + section (2) + numéro (4)
IDU_RE = re.compile(r"^[0-9]{2}[0-9AB][0-9]{2}[0-9]{3}[0-9A-Z]{2}[0-9]{4}$")
MAX_IDUS = 50
MAX_VERSIONS = 200
SQL_TIMEOUT_S = 5.0
MVT_MEDIA_TYPE = "application/vnd.mapbox-vector-tile"

# Aligné sur les routes existantes (/communes/argeles/parcelles/geojson) : évite les collisions avec /{commune}/...
router = APIRouter(prefix="/communes/{commune}/filiation", tags=["filiation"])


# --- Dépendances -------------------------------------------------------------

def insee_de(commune: str = Path(..., description="slug commune, ex. latresne")) -> str:
    insee = COMMUNES.get(commune)
    if insee is None:
        raise HTTPException(404, "Commune non chargée dans l'historique")
    return insee


Insee = Annotated[str, Depends(insee_de)]
Pool = Annotated[asyncpg.Pool, Depends(get_pool)]
DateParam = Annotated[date | None, Query(alias="date", description="YYYY-MM-DD, actuel si absent")]


# --- Helpers -----------------------------------------------------------------

def _idu(raw: str) -> str:
    idu = raw.strip().upper()
    if not IDU_RE.match(idu):
        raise HTTPException(422, f"idu invalide : {raw!r}")
    return idu


def _idus(raw: str) -> list[str]:
    """'33234000AM2150,33234000AM2151' (virgules, espaces ou retours à la ligne)."""
    idus = list(dict.fromkeys(_idu(s) for s in re.split(r"[,\s]+", raw) if s.strip()))
    if not 1 <= len(idus) <= MAX_IDUS:
        raise HTTPException(422, f"entre 1 et {MAX_IDUS} parcelles")
    return idus


async def _fetchval(pool: asyncpg.Pool, sql: str, *args):
    try:
        return await pool.fetchval(sql, *args, timeout=SQL_TIMEOUT_S)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Requête trop longue")


async def _json(pool: asyncpg.Pool, sql: str, *args, not_found: str = "Introuvable dans cette commune") -> Response:
    raw = await _fetchval(pool, sql, *args)
    if raw is None:
        raise HTTPException(404, not_found)
    if not isinstance(raw, str):  # si un codec jsonb est déjà enregistré sur le pool
        raw = json.dumps(raw, default=str)
    # Stable entre deux ingestions (cf. spec, staleTime 1 h côté client).
    return Response(raw, media_type="application/json",
                    headers={"Cache-Control": "private, max-age=3600"})


# --- Endpoints ---------------------------------------------------------------

@router.get("/millesimes")
async def millesimes(insee: Insee, pool: Pool):
    return await _json(pool, "select filiation_api.millesimes($1)", insee)


@router.get("/parcelle-au-point")
async def parcelle_au_point(
    insee: Insee, pool: Pool,
    lon: Annotated[float, Query(ge=-180, le=180)],
    lat: Annotated[float, Query(ge=-90, le=90)],
    d: DateParam = None,
):
    return await _json(pool, "select filiation_api.parcelle_au_point($1, $2, $3, $4)",
                       insee, lon, lat, d, not_found="Aucune parcelle à cet endroit à cette date")


@router.get("/version-actuelle")
async def version_actuelle(insee: Insee, pool: Pool, idu: str):
    version = await _fetchval(pool, "select filiation_api.version_actuelle($1, $2)", insee, _idu(idu))
    if version is None:
        raise HTTPException(404, "Parcelle absente du cadastre actuel de la commune")
    return {"version_id": version}


@router.get("/versions/{version_id}")
async def version_info(insee: Insee, pool: Pool, version_id: int):
    return await _json(pool, "select filiation_api.version_info($1, $2)", insee, version_id)


@router.get("/versions/{version_id}/frise")
async def frise(insee: Insee, pool: Pool, version_id: int):
    return await _json(pool, "select filiation_api.frise($1, $2)", insee, version_id)


@router.get("/versions/{version_id}/graphe")
async def graphe(insee: Insee, pool: Pool, version_id: int):
    return await _json(pool, "select filiation_api.graphe($1, $2)", insee, version_id)


@router.get("/parcelles/{idu}/evenements")
async def evenements(insee: Insee, pool: Pool, idu: str):
    return await _json(pool, "select filiation_api.evenements($1, $2)", insee, _idu(idu))


@router.get("/uf/historique")
async def historique_uf(insee: Insee, pool: Pool, idus: str, d: DateParam = None):
    return await _json(pool, "select filiation_api.historique_uf($1, $2, $3)",
                       insee, _idus(idus), d)


@router.get("/uf/emprise")
async def emprise_uf(insee: Insee, pool: Pool, idus: str, d: DateParam = None):
    return await _json(pool, "select filiation_api.emprise_uf($1, $2, $3)",
                       insee, _idus(idus), d,
                       not_found="Parcelles introuvables à cette date dans cette commune")


@router.get("/geom-versions")
async def geom_versions(pool: Pool, insee: Insee, ids: str):
    try:
        versions = list(dict.fromkeys(int(s) for s in ids.split(",") if s.strip()))
    except ValueError:
        raise HTTPException(422, "ids : entiers séparés par des virgules")
    if not 1 <= len(versions) <= MAX_VERSIONS:
        raise HTTPException(422, f"entre 1 et {MAX_VERSIONS} versions")
    return await _json(pool, "select filiation_api.geom_versions($1)", versions)


@router.get("/tuiles/{z}/{x}/{y}.pbf")
async def tuile_cadastre(
    insee: Insee, pool: Pool,
    z: Annotated[int, Path(ge=0, le=22)], x: int, y: int,
    d: DateParam = None,
    v: str | None = None,  # n° de millésime chargé, sert uniquement à casser le cache après ingestion
):
    if not (0 <= x < 2 ** z and 0 <= y < 2 ** z):
        raise HTTPException(422, "tuile hors grille")
    tile = await _fetchval(pool, "select filiation_api.tuile_cadastre($1, $2, $3, $4, $5)",
                           insee, z, x, y, d)
    # Date passée : immuable entre deux ingestions (invalidé via &v=). Actuel : 1 h.
    cache = "public, max-age=86400" if d is not None and d < date.today() else "public, max-age=3600"
    if not tile:
        return Response(status_code=204, headers={"Cache-Control": cache})
    return Response(tile, media_type=MVT_MEDIA_TYPE, headers={"Cache-Control": cache})
