# -*- coding: utf-8 -*-
"""
API veille cadastrale — lecture des photos datées et du fil de mouvements.

    GET  /{slug}/cadastre-veille
    GET  /{slug}/cadastre-veille/evenements
    GET  /{slug}/cadastre-veille/evenements/{id}
    GET  /{slug}/cadastre-veille/photos
    GET  /{slug}/cadastre-veille/parcelle?q=
    GET  /{slug}/cadastre-veille/parcelle/{idu}
    GET  /{slug}/cadastre-veille/parcelle/{idu}/a-date?date=YYYY-MM-DD
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException, Query

from api._env import DB_CONFIG
from api.raa.raa_config import get_raa_config
from api.veille_sig.cadastre.cadastre_schema import assert_schema_name

logger = logging.getLogger("cadastre_veille_api")

router = APIRouter(tags=["cadastre-veille"])

EVENT_TYPES = ("division", "fusion", "recodage", "remaniement", "suppression", "creation")
_IDU_RE = re.compile(r"^[0-9A-Za-z]{5,20}$")
SCRIPT_DIR = Path(__file__).resolve().parent
REPORTS_DIR = SCRIPT_DIR / "reports"
LEGACY_REPORTS_DIR = (
    SCRIPT_DIR.parents[2] / "services" / "ingestion" / "ingestion_cadastre" / "reports"
)


def _db_conn():
    return psycopg2.connect(**DB_CONFIG)


def _as_str(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _as_int(value: Any, default: int) -> int:
    return int(value) if isinstance(value, int) else default


def _require_cfg(commune_slug: str):
    cfg = get_raa_config(commune_slug)
    if not cfg:
        raise HTTPException(
            status_code=404,
            detail=f"Veille cadastrale non disponible pour « {commune_slug} ».",
        )
    assert_schema_name(cfg.schema)
    return cfg


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


def _row(d: dict | None) -> dict | None:
    if d is None:
        return None
    return {k: _jsonable(v) for k, v in d.items()}


def _parse_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _idu_ref(idu: str) -> str:
    s = str(idu or "")
    if len(s) >= 14:
        return f"{s[8:10]}:{s[10:14]}"
    return s


def _tables_ready(cur, schema: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name IN (
            'cadastre_photos', 'parcelles_archives',
            'cadastre_veille_runs', 'cadastre_veille_evenements'
          )
        """,
        (schema,),
    )
    return int(cur.fetchone()["n"]) == 4


def _view_exists(cur, schema: str, name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.views
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
        """,
        (schema, name),
    )
    return cur.fetchone() is not None


def _normalize_query(q: str, insee: str) -> dict[str, Any]:
    raw = (q or "").strip()
    compact = re.sub(r"[\s:_\-./]+", "", raw).upper()
    section = None
    numero = None
    candidates: list[str] = []

    if _IDU_RE.fullmatch(compact) and len(compact) >= 14:
        candidates.append(compact)
    elif re.fullmatch(r"[A-Z0-9]{1,2}\d{1,4}", compact):
        m = re.match(r"^([A-Z0-9]{1,2})(\d{1,4})$", compact)
        if m:
            section = m.group(1).zfill(2)[-2:]
            numero = m.group(2).zfill(4)
            candidates.append(f"{insee}000{section}{numero}")
    elif re.fullmatch(r"\d{1,4}", compact):
        numero = compact.zfill(4)

    return {
        "raw": raw,
        "compact": compact,
        "section": section,
        "numero": numero,
        "candidates": candidates,
    }


def _load_pending_report(insee: str, schema: str) -> dict | None:
    """Dernier rapport dry-run sur disque si la base n’a pas encore de run."""
    patterns = [
        f"diff_parcelles_{insee}_{schema}_parcelles_*.json",
        f"diff_parcelles_{insee}_{schema}_*.json",
    ]
    files: list[Path] = []
    for folder in (REPORTS_DIR, LEGACY_REPORTS_DIR):
        if not folder.is_dir():
            continue
        for pat in patterns:
            files.extend(
                p for p in folder.glob(pat) if not p.name.endswith("_veille.txt")
            )
    files = [p for p in files if "veille" not in p.stem[-8:]]
    if not files:
        return None
    latest = max(files, key=lambda p: p.stat().st_mtime)
    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    veille = data.get("veille") or {}
    evenements = veille.get("evenements") or []
    if not evenements:
        return None
    return {
        "source": "rapport_local",
        "en_attente_apply": True,
        "fichier": latest.name,
        "millesime_pci": (data.get("millesime") or {}).get("millesime_pci")
        or data.get("millesime_pci"),
        "counts": veille.get("counts") or {},
        "evenements": evenements,
        "total_etalab": data.get("total_etalab") or (data.get("etalab") or {}).get("count"),
        "total_db": data.get("total_db") or (data.get("base") or {}).get("count"),
    }


def _event_payload(row: dict, include_liens: bool = False) -> dict:
    parents = _parse_json(row.get("parents")) or []
    enfants = _parse_json(row.get("enfants")) or []
    out = {
        "id": _jsonable(row.get("id")),
        "run_id": _jsonable(row.get("run_id")),
        "type": row.get("type"),
        "nb_parents": row.get("nb_parents"),
        "nb_enfants": row.get("nb_enfants"),
        "parents": parents,
        "enfants": enfants,
        "run_at": _jsonable(row.get("run_at")),
        "millesime_pci": _jsonable(row.get("millesime_pci")),
        "photo_id": _jsonable(row.get("photo_id")),
        "source": row.get("source") or "base",
        "en_attente_apply": bool(row.get("en_attente_apply")),
    }
    if include_liens:
        out["liens"] = _parse_json(row.get("liens")) or []
    return out


def _fetch_geojson(cur, schema: str, table: str, idu: str, extra_where: str = "", params: tuple = ()) -> dict | None:
    sql = f"""
        SELECT
            idu, numero, section, contenance,
            created_etalab, updated_etalab, millesime_pci, imported_at,
            ST_AsGeoJSON(ST_Transform(geom_2154, 4326)) AS geojson
        FROM {schema}.{table}
        WHERE idu = %s {extra_where}
        LIMIT 1
    """
    cur.execute(sql, (idu, *params))
    row = cur.fetchone()
    if not row:
        return None
    out = _row(dict(row))
    if out and out.get("geojson"):
        out["geojson"] = json.loads(out["geojson"]) if isinstance(out["geojson"], str) else out["geojson"]
    out["ref"] = _idu_ref(idu)
    return out


def _filiation_for(cur, schema: str, idu: str, max_hops: int = 4) -> dict:
    has_view = _view_exists(cur, schema, "cadastre_filiation")
    parents: list[dict] = []
    enfants: list[dict] = []
    if has_view:
        cur.execute(
            f"""
            SELECT DISTINCT evenement_id, run_id, run_at, millesime_pci, type,
                   idu_parent, ref_parent, idu_enfant, ref_enfant
            FROM {schema}.cadastre_filiation
            WHERE idu_enfant = %s
            ORDER BY run_at DESC NULLS LAST
            """,
            (idu,),
        )
        parents = [_row(dict(r)) for r in cur.fetchall()]
        cur.execute(
            f"""
            SELECT DISTINCT evenement_id, run_id, run_at, millesime_pci, type,
                   idu_parent, ref_parent, idu_enfant, ref_enfant
            FROM {schema}.cadastre_filiation
            WHERE idu_parent = %s
            ORDER BY run_at DESC NULLS LAST
            """,
            (idu,),
        )
        enfants = [_row(dict(r)) for r in cur.fetchall()]
    else:
        cur.execute(
            f"""
            SELECT e.id AS evenement_id, e.run_id, r.run_at, r.millesime_pci, e.type,
                   e.parents, e.enfants
            FROM {schema}.cadastre_veille_evenements e
            JOIN {schema}.cadastre_veille_runs r ON r.id = e.run_id
            WHERE e.parents @> %s::jsonb OR e.enfants @> %s::jsonb
            ORDER BY r.run_at DESC
            """,
            (json.dumps([{"idu": idu}]), json.dumps([{"idu": idu}])),
        )
        for r in cur.fetchall():
            d = dict(r)
            for p in _parse_json(d.get("parents")) or []:
                for c in _parse_json(d.get("enfants")) or []:
                    link = {
                        "evenement_id": _jsonable(d["evenement_id"]),
                        "run_id": _jsonable(d["run_id"]),
                        "run_at": _jsonable(d["run_at"]),
                        "millesime_pci": _jsonable(d["millesime_pci"]),
                        "type": d["type"],
                        "idu_parent": p.get("idu"),
                        "ref_parent": p.get("ref") or _idu_ref(p.get("idu") or ""),
                        "idu_enfant": c.get("idu"),
                        "ref_enfant": c.get("ref") or _idu_ref(c.get("idu") or ""),
                    }
                    if c.get("idu") == idu:
                        parents.append(link)
                    if p.get("idu") == idu:
                        enfants.append(link)

    ancetres: list[dict] = []
    seen = {idu}
    frontier = [p["idu_parent"] for p in parents if p.get("idu_parent")]
    hops = 0
    while frontier and hops < max_hops:
        nxt = []
        for pid in frontier:
            if not pid or pid in seen:
                continue
            seen.add(pid)
            if has_view:
                cur.execute(
                    f"""
                    SELECT DISTINCT evenement_id, run_at, millesime_pci, type,
                           idu_parent, ref_parent, idu_enfant, ref_enfant
                    FROM {schema}.cadastre_filiation
                    WHERE idu_enfant = %s
                    """,
                    (pid,),
                )
                rows = [_row(dict(r)) for r in cur.fetchall()]
            else:
                rows = []
            if rows:
                ancetres.extend(rows)
                nxt.extend(r["idu_parent"] for r in rows if r.get("idu_parent"))
        frontier = nxt
        hops += 1

    return {"parents": parents, "enfants": enfants, "ancetres": ancetres}


def _photos(cur, schema: str) -> list[dict]:
    if _view_exists(cur, schema, "cadastre_photos_periode"):
        cur.execute(
            f"""
            SELECT id, archived_at, millesime_pci, millesime_suivant, motif,
                   nb_parcelles, note, debut, fin
            FROM {schema}.cadastre_photos_periode
            ORDER BY archived_at ASC, id
            """
        )
    else:
        cur.execute(
            f"""
            SELECT id, archived_at, millesime_pci, millesime_suivant, motif,
                   nb_parcelles, note,
                   COALESCE(millesime_pci, archived_at::date) AS debut,
                   millesime_suivant AS fin
            FROM {schema}.cadastre_photos
            ORDER BY archived_at ASC, id
            """
        )
    return [_row(dict(r)) for r in cur.fetchall()]


def _latest_snapshot(cur, schema: str, idu: str) -> dict | None:
    return _fetch_geojson(cur, schema, "parcelles", idu)


def _archive_snapshot(cur, schema: str, idu: str, photo_id: str) -> dict | None:
    return _fetch_geojson(
        cur, schema, "parcelles_archives", idu, "AND photo_id = %s", (photo_id,)
    )


def _pick_photo_for_date(photos: list[dict], jour: date) -> dict | None:
    chosen = None
    for p in photos:
        debut = p.get("debut")
        fin = p.get("fin")
        d0 = date.fromisoformat(debut) if isinstance(debut, str) else debut
        d1 = date.fromisoformat(fin) if isinstance(fin, str) else fin
        if d0 and jour < d0:
            continue
        if d1 and jour >= d1:
            continue
        chosen = p
    return chosen


def _search_parcelles(cur, schema: str, parsed: dict, limit: int = 20) -> list[dict]:
    clauses = []
    params: list[Any] = []
    if parsed["candidates"]:
        clauses.append("idu = ANY(%s)")
        params.append(parsed["candidates"])
    if parsed["section"] and parsed["numero"]:
        clauses.append("(UPPER(section) = %s AND LPAD(numero, 4, '0') = %s)")
        params.extend([parsed["section"], parsed["numero"]])
    elif parsed["numero"] and not parsed["section"]:
        clauses.append("LPAD(numero, 4, '0') = %s")
        params.append(parsed["numero"])
    if parsed["compact"] and len(parsed["compact"]) >= 4:
        clauses.append("idu ILIKE %s")
        params.append(f"%{parsed['compact']}%")
    if not clauses:
        return []
    cur.execute(
        f"""
        SELECT idu, numero, section, contenance,
               created_etalab, updated_etalab, millesime_pci, imported_at
        FROM {schema}.parcelles
        WHERE {" OR ".join(clauses)}
        ORDER BY section, numero
        LIMIT %s
        """,
        (*params, limit),
    )
    rows = []
    for r in cur.fetchall():
        d = _row(dict(r))
        d["ref"] = _idu_ref(d["idu"])
        rows.append(d)
    return rows


@router.get("/{commune_slug}/cadastre-veille")
def cadastre_veille_accueil(commune_slug: str):
    cfg = _require_cfg(commune_slug)
    schema = cfg.schema
    pending = None
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            ready = _tables_ready(cur, schema)
            latest = {"nb_parcelles": 0, "millesime_pci": None, "imported_at": None}
            try:
                cur.execute(f"SELECT COUNT(*) AS n FROM {schema}.parcelles")
                latest["nb_parcelles"] = int(cur.fetchone()["n"])
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = 'parcelles'
                      AND column_name IN ('millesime_pci', 'imported_at')
                    """,
                    (schema,),
                )
                cols = {r["column_name"] for r in cur.fetchall()}
                parts = []
                if "millesime_pci" in cols:
                    parts.append("MAX(millesime_pci) AS millesime_pci")
                if "imported_at" in cols:
                    parts.append("MAX(imported_at) AS imported_at")
                if parts:
                    cur.execute(f"SELECT {', '.join(parts)} FROM {schema}.parcelles")
                    latest.update(_row(dict(cur.fetchone())) or {})
            except Exception:
                conn.rollback()

            photos = []
            last_run = None
            counts = {t: 0 for t in EVENT_TYPES}
            nb_evenements = 0
            if ready:
                photos = _photos(cur, schema)
                cur.execute(
                    f"""
                    SELECT id, photo_id, run_at, millesime_pci, mode,
                           total_etalab, total_db, counts, apply_stats
                    FROM {schema}.cadastre_veille_runs
                    ORDER BY run_at DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row:
                    last_run = _row(dict(row))
                    last_run["counts"] = _parse_json(last_run.get("counts")) or {}
                    last_run["apply_stats"] = _parse_json(last_run.get("apply_stats")) or {}
                cur.execute(
                    f"""
                    SELECT type, COUNT(*) AS n
                    FROM {schema}.cadastre_veille_evenements
                    GROUP BY type
                    """
                )
                for r in cur.fetchall():
                    counts[r["type"]] = int(r["n"])
                    nb_evenements += int(r["n"])

    if nb_evenements == 0:
        pending = _load_pending_report(cfg.insee, schema)
        if pending:
            counts = {t: int((pending.get("counts") or {}).get(t, 0)) for t in EVENT_TYPES}
            nb_evenements = sum(counts.values())

    return {
        "commune": cfg.commune_label,
        "slug": cfg.slug,
        "insee": cfg.insee,
        "schema": schema,
        "schema_ready": ready,
        "latest": latest,
        "photos": photos,
        "last_run": last_run,
        "counts": counts,
        "nb_evenements": nb_evenements,
        "pending": pending,
        "source": "PCI open data Etalab (pas le plan cadastral opposable DGFiP)",
    }


@router.get("/{commune_slug}/cadastre-veille/photos")
def cadastre_veille_photos(commune_slug: str):
    cfg = _require_cfg(commune_slug)
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not _tables_ready(cur, cfg.schema):
                return {"photos": []}
            return {"photos": _photos(cur, cfg.schema)}


@router.get("/{commune_slug}/cadastre-veille/evenements")
def cadastre_veille_evenements(
    commune_slug: str,
    event_type: str | None = Query(default=None, alias="type"),
    q: str | None = Query(default=None),
    limit: int = Query(default=80, ge=1, le=300),
    offset: int = Query(default=0, ge=0),
):
    cfg = _require_cfg(commune_slug)
    kind = (_as_str(event_type) or "").lower() or None
    q = _as_str(q)
    limit = _as_int(limit, 80)
    offset = _as_int(offset, 0)
    if kind and kind not in EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Type inconnu: {event_type}")

    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not _tables_ready(cur, cfg.schema):
                items = []
            else:
                where = ["TRUE"]
                params: list[Any] = []
                if kind:
                    where.append("e.type = %s")
                    params.append(kind)
                if q:
                    parsed = _normalize_query(q, cfg.insee)
                    needle = parsed["candidates"][0] if parsed["candidates"] else parsed["compact"]
                    if needle:
                        where.append(
                            "(e.parents::text ILIKE %s OR e.enfants::text ILIKE %s)"
                        )
                        like = f"%{needle}%"
                        params.extend([like, like])
                cur.execute(
                    f"""
                    SELECT e.id, e.run_id, e.type, e.nb_parents, e.nb_enfants,
                           e.parents, e.enfants,
                           r.run_at, r.millesime_pci, r.photo_id
                    FROM {cfg.schema}.cadastre_veille_evenements e
                    JOIN {cfg.schema}.cadastre_veille_runs r ON r.id = e.run_id
                    WHERE {" AND ".join(where)}
                    ORDER BY r.run_at DESC, e.id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (*params, limit, offset),
                )
                items = [_event_payload(dict(r)) for r in cur.fetchall()]

    source = "base"
    en_attente = False
    if not items:
        pending = _load_pending_report(cfg.insee, cfg.schema)
        if pending:
            evs = pending["evenements"]
            if kind:
                evs = [e for e in evs if e.get("type") == kind]
            if q:
                parsed = _normalize_query(q, cfg.insee)
                needle = (parsed["candidates"][0] if parsed["candidates"] else parsed["compact"] or q).upper()
                evs = [
                    e
                    for e in evs
                    if needle in json.dumps(e, ensure_ascii=False).upper()
                ]
            items = [
                _event_payload(
                    {
                        **e,
                        "id": f"pending-{i}",
                        "run_id": None,
                        "run_at": None,
                        "millesime_pci": pending.get("millesime_pci"),
                        "source": "rapport_local",
                        "en_attente_apply": True,
                    }
                )
                for i, e in enumerate(evs[offset : offset + limit])
            ]
            source = "rapport_local"
            en_attente = True

    return {"evenements": items, "source": source, "en_attente_apply": en_attente}


@router.get("/{commune_slug}/cadastre-veille/evenements/{event_id}")
def cadastre_veille_evenement(commune_slug: str, event_id: str):
    cfg = _require_cfg(commune_slug)
    if str(event_id).startswith("pending-"):
        pending = _load_pending_report(cfg.insee, cfg.schema)
        if not pending:
            raise HTTPException(status_code=404, detail="Événement introuvable.")
        try:
            idx = int(str(event_id).split("-", 1)[1])
            ev = pending["evenements"][idx]
        except (ValueError, IndexError):
            raise HTTPException(status_code=404, detail="Événement introuvable.")
        payload = _event_payload(
            {
                **ev,
                "id": event_id,
                "millesime_pci": pending.get("millesime_pci"),
                "source": "rapport_local",
                "en_attente_apply": True,
            },
            include_liens=True,
        )
        return {"evenement": payload, "geoms": []}

    try:
        eid = int(event_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Événement introuvable.")

    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if not _tables_ready(cur, cfg.schema):
                raise HTTPException(status_code=404, detail="Événement introuvable.")
            cur.execute(
                f"""
                SELECT e.*, r.run_at, r.millesime_pci, r.photo_id
                FROM {cfg.schema}.cadastre_veille_evenements e
                JOIN {cfg.schema}.cadastre_veille_runs r ON r.id = e.run_id
                WHERE e.id = %s
                """,
                (eid,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Événement introuvable.")
            ev = _event_payload(dict(row), include_liens=True)
            geoms = []
            photo_id = ev.get("photo_id")
            for role, items in (("parent", ev.get("parents") or []), ("enfant", ev.get("enfants") or [])):
                for p in items:
                    idu = p.get("idu")
                    if not idu:
                        continue
                    snap = None
                    if role == "parent" and photo_id:
                        snap = _archive_snapshot(cur, cfg.schema, idu, photo_id)
                    if snap is None:
                        snap = _latest_snapshot(cur, cfg.schema, idu)
                    if snap and snap.get("geojson"):
                        geoms.append({"role": role, "idu": idu, "ref": _idu_ref(idu), **snap})
    return {"evenement": ev, "geoms": geoms}


@router.get("/{commune_slug}/cadastre-veille/parcelle")
def cadastre_veille_recherche_parcelle(
    commune_slug: str,
    q: str = Query(..., min_length=2),
):
    cfg = _require_cfg(commune_slug)
    parsed = _normalize_query(q, cfg.insee)
    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            resultats = _search_parcelles(cur, cfg.schema, parsed)
            archives = []
            if _tables_ready(cur, cfg.schema) and (parsed["candidates"] or parsed["compact"]):
                needle = parsed["candidates"][0] if parsed["candidates"] else parsed["compact"]
                cur.execute(
                    f"""
                    SELECT DISTINCT a.idu, a.section, a.numero
                    FROM {cfg.schema}.parcelles_archives a
                    WHERE a.idu = ANY(%s) OR a.idu ILIKE %s
                    LIMIT 20
                    """,
                    (parsed["candidates"] or [needle], f"%{needle}%"),
                )
                for r in cur.fetchall():
                    d = _row(dict(r))
                    d["ref"] = _idu_ref(d["idu"])
                    d["source"] = "archive"
                    archives.append(d)

    known = {r["idu"] for r in resultats}
    for a in archives:
        if a["idu"] not in known:
            resultats.append(a)
            known.add(a["idu"])

    pending = _load_pending_report(cfg.insee, cfg.schema)
    if pending:
        needle = (parsed["candidates"][0] if parsed["candidates"] else parsed["compact"] or q).upper()
        for ev in pending["evenements"]:
            for role, items in (("parent", ev.get("parents") or []), ("enfant", ev.get("enfants") or [])):
                for p in items:
                    idu = (p.get("idu") or "").upper()
                    if not idu or idu in known:
                        continue
                    blob = f"{idu} {p.get('ref') or ''} {p.get('section') or ''}{p.get('numero') or ''}".upper()
                    hay = blob.replace(":", "").replace(" ", "")
                    needles = {n for n in (needle, parsed["compact"]) if n and len(n) >= 4}
                    if not needles or not any(n in hay for n in needles):
                        continue
                    resultats.append({
                        "idu": idu,
                        "ref": p.get("ref") or _idu_ref(idu),
                        "section": p.get("section"),
                        "numero": p.get("numero"),
                        "source": "rapport_local",
                    })
                    known.add(idu)
    return {"q": q, "resultats": resultats}


@router.get("/{commune_slug}/cadastre-veille/parcelle/{idu}")
def cadastre_veille_parcelle(commune_slug: str, idu: str):
    cfg = _require_cfg(commune_slug)
    idu = idu.strip().upper()
    if not _IDU_RE.fullmatch(idu):
        parsed = _normalize_query(idu, cfg.insee)
        if parsed["candidates"]:
            idu = parsed["candidates"][0]
        else:
            raise HTTPException(status_code=400, detail="IDU invalide.")

    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            actuel = _latest_snapshot(cur, cfg.schema, idu)
            versions = []
            evenements = []
            filiation = {"parents": [], "enfants": [], "ancetres": []}
            photos = []
            if _tables_ready(cur, cfg.schema):
                photos = _photos(cur, cfg.schema)
                cur.execute(
                    f"""
                    SELECT p.id AS photo_id, p.archived_at, p.millesime_pci,
                           p.millesime_suivant, p.motif, p.note,
                           a.contenance, a.created_etalab, a.updated_etalab,
                           ST_AsGeoJSON(ST_Transform(a.geom_2154, 4326)) AS geojson
                    FROM {cfg.schema}.parcelles_archives a
                    JOIN {cfg.schema}.cadastre_photos p ON p.id = a.photo_id
                    WHERE a.idu = %s
                    ORDER BY p.archived_at ASC
                    """,
                    (idu,),
                )
                for r in cur.fetchall():
                    d = _row(dict(r))
                    if d.get("geojson") and isinstance(d["geojson"], str):
                        d["geojson"] = json.loads(d["geojson"])
                    d["ref"] = _idu_ref(idu)
                    versions.append(d)
                cur.execute(
                    f"""
                    SELECT e.id, e.run_id, e.type, e.nb_parents, e.nb_enfants,
                           e.parents, e.enfants, r.run_at, r.millesime_pci, r.photo_id
                    FROM {cfg.schema}.cadastre_veille_evenements e
                    JOIN {cfg.schema}.cadastre_veille_runs r ON r.id = e.run_id
                    WHERE e.parents @> %s::jsonb OR e.enfants @> %s::jsonb
                    ORDER BY r.run_at DESC, e.id DESC
                    """,
                    (json.dumps([{"idu": idu}]), json.dumps([{"idu": idu}])),
                )
                evenements = [_event_payload(dict(r)) for r in cur.fetchall()]
                filiation = _filiation_for(cur, cfg.schema, idu)

    pending = _load_pending_report(cfg.insee, cfg.schema) if not evenements else None
    if pending:
        for i, ev in enumerate(pending["evenements"]):
            blob = json.dumps(ev, ensure_ascii=False)
            if idu not in blob:
                continue
            evenements.append(
                _event_payload(
                    {
                        **ev,
                        "id": f"pending-{i}",
                        "millesime_pci": pending.get("millesime_pci"),
                        "source": "rapport_local",
                        "en_attente_apply": True,
                    }
                )
            )
        for ev in evenements:
            if ev.get("source") != "rapport_local":
                continue
            for p in ev.get("parents") or []:
                if p.get("idu") == idu:
                    continue
                if any(c.get("idu") == idu for c in ev.get("enfants") or []):
                    filiation["parents"].append(
                        {
                            "type": ev["type"],
                            "idu_parent": p.get("idu"),
                            "ref_parent": p.get("ref"),
                            "idu_enfant": idu,
                            "ref_enfant": _idu_ref(idu),
                            "millesime_pci": ev.get("millesime_pci"),
                        }
                    )
            for c in ev.get("enfants") or []:
                if c.get("idu") == idu:
                    continue
                if any(p.get("idu") == idu for p in ev.get("parents") or []):
                    filiation["enfants"].append(
                        {
                            "type": ev["type"],
                            "idu_parent": idu,
                            "ref_parent": _idu_ref(idu),
                            "idu_enfant": c.get("idu"),
                            "ref_enfant": c.get("ref"),
                            "millesime_pci": ev.get("millesime_pci"),
                        }
                    )

    if not actuel and not versions and not evenements:
        raise HTTPException(
            status_code=404,
            detail=f"Parcelle {idu} introuvable dans le latest et les archives.",
        )

    return {
        "idu": idu,
        "ref": _idu_ref(idu),
        "actuel": actuel,
        "presente_aujourdhui": actuel is not None,
        "versions": versions,
        "photos": photos,
        "evenements": evenements,
        "filiation": filiation,
    }


@router.get("/{commune_slug}/cadastre-veille/parcelle/{idu}/a-date")
def cadastre_veille_parcelle_a_date(
    commune_slug: str,
    idu: str,
    date_ref: str = Query(..., alias="date", description="YYYY-MM-DD"),
):
    cfg = _require_cfg(commune_slug)
    idu = idu.strip().upper()
    if not _IDU_RE.fullmatch(idu):
        parsed = _normalize_query(idu, cfg.insee)
        if parsed["candidates"]:
            idu = parsed["candidates"][0]
        else:
            raise HTTPException(status_code=400, detail="IDU invalide.")
    try:
        jour = date.fromisoformat(date_ref)
    except ValueError:
        raise HTTPException(status_code=400, detail="Date invalide (YYYY-MM-DD).")

    with _db_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            actuel = _latest_snapshot(cur, cfg.schema, idu)
            latest_millesime = None
            if actuel and actuel.get("millesime_pci"):
                latest_millesime = date.fromisoformat(str(actuel["millesime_pci"])[:10])
            elif actuel:
                latest_millesime = jour

            photos = _photos(cur, cfg.schema) if _tables_ready(cur, cfg.schema) else []
            photo = _pick_photo_for_date(photos, jour)

            # Après le dernier millésime connu → latest
            use_latest = False
            if not photos:
                use_latest = True
            elif latest_millesime and jour >= latest_millesime and (
                not photo or (photo.get("fin") and jour >= date.fromisoformat(str(photo["fin"])[:10]))
            ):
                use_latest = True
            elif photo is None and actuel:
                # Date postérieure à toutes les photos
                last_fin = photos[-1].get("fin") if photos else None
                last_debut = photos[-1].get("debut") if photos else None
                borne = last_fin or last_debut
                if borne and jour >= date.fromisoformat(str(borne)[:10]):
                    use_latest = True

            if use_latest:
                if actuel:
                    return {
                        "idu": idu,
                        "ref": _idu_ref(idu),
                        "date": jour.isoformat(),
                        "presente": True,
                        "source": "latest",
                        "photo": None,
                        "parcelle": actuel,
                        "note": "État actuel de la carte (dernière version Etalab importée).",
                    }
                return {
                    "idu": idu,
                    "ref": _idu_ref(idu),
                    "date": jour.isoformat(),
                    "presente": False,
                    "source": "latest",
                    "photo": None,
                    "parcelle": None,
                    "note": "Absente du cadastre actuel. Consulter la filiation pour les parents.",
                    "filiation": _filiation_for(cur, cfg.schema, idu) if _tables_ready(cur, cfg.schema) else None,
                }

            if photo is None:
                return {
                    "idu": idu,
                    "ref": _idu_ref(idu),
                    "date": jour.isoformat(),
                    "presente": False,
                    "source": None,
                    "photo": None,
                    "parcelle": None,
                    "note": "Aucune photo ne couvre cette date (état initial non encore archivé).",
                }

            snap = _archive_snapshot(cur, cfg.schema, idu, photo["id"])
            if snap:
                return {
                    "idu": idu,
                    "ref": _idu_ref(idu),
                    "date": jour.isoformat(),
                    "presente": True,
                    "source": "archive",
                    "photo": photo,
                    "parcelle": snap,
                    "note": f"Photo {photo.get('motif')} — millésime {photo.get('millesime_pci') or 'non daté'}.",
                }

            filiation = _filiation_for(cur, cfg.schema, idu)
            return {
                "idu": idu,
                "ref": _idu_ref(idu),
                "date": jour.isoformat(),
                "presente": False,
                "source": "archive",
                "photo": photo,
                "parcelle": None,
                "note": "Cette parcelle n'existait pas sur la photo de cette date. Voir les parents.",
                "filiation": filiation,
            }
