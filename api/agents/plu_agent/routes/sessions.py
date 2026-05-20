"""
Sessions PLU — persistance Supabase, chargement zonage, endpoints session(s).

Pour modifier la création de session, l'historique ou le SQL : tout est ici.
"""

import json
import logging
import time

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException
from google.genai import types
from pydantic import BaseModel, Field

from .._env import DB_CONFIG, GEMINI_MODEL
from .schemas import ToolCallLog, Usage

try:
    from ..tools import get_zonage_et_reglements
except ImportError:
    from tools import get_zonage_et_reglements

logger = logging.getLogger("plu_api")
router = APIRouter()

# ---------------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------------

class SessionRequest(BaseModel):
    section: str | None = Field(None, examples=["AC"])
    numero:  str | None = Field(None, examples=["45"])
    idu:     str | None = Field(None, examples=["66008000AC0045"])
    geojson: str | None = Field(None, description="GeoJSON WGS84 si géométrie ad hoc")
    question: str | None = Field(
        None,
        examples=["Cette parcelle est-elle constructible ?"],
        description="Question initiale optionnelle — déclenche un premier tour immédiatement.",
    )


class SessionResponse(BaseModel):
    session_id:    str
    zones:         list[dict]
    zones_summary: str
    answer:        str | None = None
    tool_calls:    list[ToolCallLog] = []
    usage:         Usage | None = None
    latency_ms:    int | None = None
    model:         str
    map_data:      dict | None = None  # déprécié : toujours null ; géométries via GET /session/{id}/map
    show_map:      bool = False


class SessionStateResponse(BaseModel):
    session_id:   str
    created_at:   str
    updated_at:   str
    section:      str | None
    numero:       str | None
    idu:          str | None
    zones:        list[dict]
    total_tokens: int
    total_turns:  int
    messages:     list[dict]


class SessionListItem(BaseModel):
    session_id:    str
    title:         str
    zones_summary: str
    total_turns:   int
    updated_at:    str
    preview:       str | None = None


class SessionsListResponse(BaseModel):
    sessions: list[SessionListItem]

# ---------------------------------------------------------------------------
# Persistance Supabase
# ---------------------------------------------------------------------------

def _db_conn():
    return psycopg2.connect(**DB_CONFIG)


def _parse_json_field(value):
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return value


def zones_summary(zones: list[dict]) -> str:
    return ", ".join(
        f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
        for z in zones
    ) or "aucune zone trouvée"


def _session_title(section, numero, idu, preview) -> str:
    if section and numero:
        return f"Parcelle {section} {numero}"
    if idu:
        return f"IDU {idu}"
    if preview:
        text = preview.strip().replace("\n", " ")
        return text[:72] + ("…" if len(text) > 72 else "")
    return "Conversation PLU"


def session_create(section, numero, idu, geojson, zones, model) -> str:
    sql = """
        INSERT INTO argeles.plu_sessions
            (section, numero, idu, geojson, zones, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                section, numero, idu, geojson,
                json.dumps(zones, default=str),
                model,
            ))
            session_id = str(cur.fetchone()[0])
    conn.close()
    return session_id


def session_get(session_id: str) -> dict | None:
    sql = "SELECT * FROM argeles.plu_sessions WHERE id = %s;"
    conn = _db_conn()
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (session_id,))
            row = cur.fetchone()
    conn.close()
    if not row:
        return None
    session = dict(row)
    session["zones"] = _parse_json_field(session.get("zones")) or []
    return session


def messages_get(session_id: str) -> list[dict]:
    sql = """
        SELECT role, content, tool_calls, created_at
        FROM argeles.plu_messages
        WHERE session_id = %s
        ORDER BY created_at ASC;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (session_id,))
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for row in rows:
        row["tool_calls"] = _parse_json_field(row.get("tool_calls"))
    return rows


def messages_insert(session_id, user_message, model_answer, tool_calls,
                    prompt_tokens, candidate_tokens, total_tokens, latency_ms):
    sql_msg = """
        INSERT INTO argeles.plu_messages
            (session_id, role, content, tool_calls, prompt_tokens,
             candidate_tokens, total_tokens, latency_ms)
        VALUES
            (%s, 'user',  %s, NULL,  NULL, NULL, NULL, NULL),
            (%s, 'model', %s, %s,    %s,   %s,   %s,   %s);
    """
    sql_session = """
        UPDATE argeles.plu_sessions
        SET total_tokens = total_tokens + COALESCE(%s, 0),
            total_turns  = total_turns  + 1,
            updated_at   = now()
        WHERE id = %s;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_msg, (
                session_id, user_message,
                session_id, model_answer,
                json.dumps(tool_calls, default=str),
                prompt_tokens, candidate_tokens, total_tokens, latency_ms,
            ))
            cur.execute(sql_session, (total_tokens, session_id))
    conn.close()


def session_delete(session_id: str) -> bool:
    sql_msgs = "DELETE FROM argeles.plu_messages WHERE session_id = %s;"
    sql_session = "DELETE FROM argeles.plu_sessions WHERE id = %s RETURNING id;"
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_msgs, (session_id,))
            cur.execute(sql_session, (session_id,))
            deleted = cur.fetchone() is not None
    conn.close()
    return deleted


def _sessions_list(limit: int = 50) -> list[dict]:
    sql = """
        SELECT s.id, s.section, s.numero, s.idu, s.zones, s.total_turns, s.updated_at,
            (SELECT content FROM argeles.plu_messages m
             WHERE m.session_id = s.id AND m.role = 'user'
             ORDER BY m.created_at ASC LIMIT 1) AS preview
        FROM argeles.plu_sessions s
        ORDER BY s.updated_at DESC
        LIMIT %s;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (limit,))
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for row in rows:
        row["zones"] = _parse_json_field(row.get("zones")) or []
        row["id"] = str(row["id"])
    return rows

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/sessions", response_model=SessionsListResponse)
def list_sessions(limit: int = 50):
    limit = max(1, min(limit, 100))
    rows = _sessions_list(limit=limit)
    return SessionsListResponse(sessions=[
        SessionListItem(
            session_id=row["id"],
            title=_session_title(
                row.get("section"), row.get("numero"), row.get("idu"), row.get("preview")
            ),
            zones_summary=zones_summary(row.get("zones") or []),
            total_turns=row.get("total_turns") or 0,
            updated_at=str(row["updated_at"]),
            preview=row.get("preview"),
        )
        for row in rows
    ])


@router.post("/session", response_model=SessionResponse)
def create_session(req: SessionRequest):
    from .chat import run_turn, _map_requested  # import local — évite cycle au chargement

    has_geo = any([req.section and req.numero, req.idu, req.geojson])
    if not has_geo:
        raise HTTPException(status_code=422, detail="Fournir section+numero, idu, ou geojson.")

    t0 = time.monotonic()

    zones_result = get_zonage_et_reglements(
        DB_CONFIG,
        section=req.section,
        numero=req.numero,
        idu=req.idu,
        geojson=req.geojson,
    )
    if zones_result.get("error"):
        raise HTTPException(status_code=400, detail=zones_result["error"])

    zones = zones_result.get("zones", [])

    session_id = session_create(
        section=req.section.upper() if req.section else None,
        numero=str(req.numero).strip() if req.numero else None,
        idu=req.idu,
        geojson=req.geojson,
        zones=zones,
        model=GEMINI_MODEL,
    )
    logger.info(f"session créée : {session_id} — zones : {zones_summary(zones)}")

    answer     = None
    tool_calls = []
    usage      = None
    show_map   = False
    latency_ms = int((time.monotonic() - t0) * 1000)

    if req.question:
        contents = [types.Content(role="user", parts=[types.Part(text=req.question)])]
        try:
            answer, tool_calls, usage = run_turn(zones, contents)
        except Exception as e:
            logger.error(f"agentic_loop error : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

        latency_ms = int((time.monotonic() - t0) * 1000)
        show_map = _map_requested(tool_calls)
        messages_insert(
            session_id=session_id,
            user_message=req.question,
            model_answer=answer,
            tool_calls=[tc.model_dump() for tc in tool_calls],
            prompt_tokens=usage.prompt_tokens,
            candidate_tokens=usage.candidate_tokens,
            total_tokens=usage.total_tokens,
            latency_ms=latency_ms,
        )

    return SessionResponse(
        session_id=session_id,
        zones=zones,
        zones_summary=zones_summary(zones),
        answer=answer,
        tool_calls=tool_calls,
        usage=usage,
        latency_ms=latency_ms,
        model=GEMINI_MODEL,
        map_data=None,
        show_map=show_map,
    )


@router.delete("/session/{session_id}", status_code=204)
def delete_session(session_id: str):
    if not session_delete(session_id):
        raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")


@router.get("/session/{session_id}", response_model=SessionStateResponse)
def get_session(session_id: str):
    session = session_get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")
    messages = messages_get(session_id)
    return SessionStateResponse(
        session_id=str(session["id"]),
        created_at=str(session["created_at"]),
        updated_at=str(session["updated_at"]),
        section=session.get("section"),
        numero=session.get("numero"),
        idu=session.get("idu"),
        zones=session.get("zones") or [],
        total_tokens=session.get("total_tokens", 0),
        total_turns=session.get("total_turns", 0),
        messages=[
            {
                "role":       m["role"],
                "content":    m["content"],
                "tool_calls": m.get("tool_calls"),
                "created_at": str(m["created_at"]),
            }
            for m in messages
        ],
    )
