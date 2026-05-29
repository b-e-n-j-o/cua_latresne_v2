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
from ..commune_context import get_current_profile
from ..commune_profile import CommuneProfile
from .schemas import ToolCallLog, Usage

try:
    from ..tools.utils import get_zonage_et_reglements
    from ..tools.utils.parcel_geom import normalize_parcel_refs, parcelles_refs_to_json
except ImportError:
    from tools.utils import get_zonage_et_reglements
    from tools.utils.parcel_geom import normalize_parcel_refs, parcelles_refs_to_json

logger = logging.getLogger("plu_api")

# ---------------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------------

class ParcelleRef(BaseModel):
    section: str
    numero: str


class SessionRequest(BaseModel):
    section: str | None = Field(None, examples=["AC"])
    numero:  str | None = Field(None, examples=["45"])
    idu:     str | None = Field(None, examples=["66008000AC0045"])
    parcelles: list[ParcelleRef] | None = Field(
        None,
        description="Unité foncière : liste de couples section + numéro contigus.",
    )
    idus: list[str] | None = Field(None, description="Unité foncière : liste d'IDU contigus.")
    question: str | None = Field(
        None,
        examples=["Cette parcelle est-elle constructible ?"],
        description=(
            "Question initiale — déclenche un premier tour immédiatement. "
            "Les refs parcellaires sont optionnelles (zonage préchargé si fournies)."
        ),
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
    if not zones:
        return ""
    return ", ".join(
        f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
        for z in zones
    )


def _session_title(section, numero, idu, preview, parcelles_refs: str | None = None) -> str:
    if parcelles_refs:
        try:
            data = json.loads(parcelles_refs)
            n = len(data.get("parcelles") or []) + len(data.get("idus") or [])
            if n > 1:
                return f"Unité foncière ({n} parcelles)"
        except (json.JSONDecodeError, TypeError):
            pass
    if section and numero:
        return f"Parcelle {section} {numero}"
    if idu:
        return f"IDU {idu}"
    if preview:
        text = preview.strip().replace("\n", " ")
        return text[:72] + ("…" if len(text) > 72 else "")
    return "Conversation PLU"


def session_create(section, numero, idu, parcelles_refs, zones, model) -> str:
    """parcelles_refs : JSON stocké dans la colonne geojson (liste de refs, pas de géométrie)."""
    sessions = get_current_profile().sessions_table()
    sql = f"""
        INSERT INTO {sessions}
            (section, numero, idu, geojson, zones, model)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                section, numero, idu, parcelles_refs,
                json.dumps(zones, default=str),
                model,
            ))
            session_id = str(cur.fetchone()[0])
    conn.close()
    return session_id


def session_get(session_id: str) -> dict | None:
    sessions = get_current_profile().sessions_table()
    sql = f"SELECT * FROM {sessions} WHERE id = %s;"
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
    messages = get_current_profile().messages_table()
    sql = f"""
        SELECT role, content, tool_calls, gemini_parts, created_at
        FROM {messages}
        WHERE session_id = %s
        ORDER BY created_at ASC, id ASC;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (session_id,))
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    for row in rows:
        row["tool_calls"] = _parse_json_field(row.get("tool_calls"))
        row["gemini_parts"] = _parse_json_field(row.get("gemini_parts"))
    return rows


def messages_insert(
    session_id,
    user_message,
    model_answer,
    tool_calls=None,
    gemini_parts=None,
    prompt_tokens=None,
    candidate_tokens=None,
    total_tokens=None,
    latency_ms=None,
):
    profile = get_current_profile()
    messages = profile.messages_table()
    sessions = profile.sessions_table()
    sql_msg = f"""
        INSERT INTO {messages}
            (session_id, role, content, tool_calls, gemini_parts,
             prompt_tokens, candidate_tokens, total_tokens, latency_ms)
        VALUES
            (%s, 'user',  %s, NULL, NULL, NULL, NULL, NULL, NULL),
            (%s, 'model', %s, %s,   %s,   %s,   %s,   %s,   %s);
    """
    sql_session = f"""
        UPDATE {sessions}
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
                json.dumps(tool_calls, default=str) if tool_calls is not None else None,
                json.dumps(gemini_parts, default=str) if gemini_parts is not None else None,
                prompt_tokens, candidate_tokens, total_tokens, latency_ms,
            ))
            cur.execute(sql_session, (total_tokens, session_id))
    conn.close()


def session_delete(session_id: str) -> bool:
    profile = get_current_profile()
    sql_msgs = f"DELETE FROM {profile.messages_table()} WHERE session_id = %s;"
    sql_session = f"DELETE FROM {profile.sessions_table()} WHERE id = %s RETURNING id;"
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_msgs, (session_id,))
            cur.execute(sql_session, (session_id,))
            deleted = cur.fetchone() is not None
    conn.close()
    return deleted


def _sessions_list(limit: int = 50) -> list[dict]:
    profile = get_current_profile()
    sessions = profile.sessions_table()
    messages = profile.messages_table()
    sql = f"""
        SELECT s.id, s.section, s.numero, s.idu, s.zones, s.total_turns, s.updated_at,
            (SELECT content FROM {messages} m
             WHERE m.session_id = s.id AND m.role = 'user'
             ORDER BY m.created_at ASC LIMIT 1) AS preview
        FROM {sessions} s
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

def register(router: APIRouter, profile: CommuneProfile, bind) -> None:
    @router.get("/sessions", response_model=SessionsListResponse)
    @bind
    def list_sessions(limit: int = 50):
        limit = max(1, min(limit, 100))
        rows = _sessions_list(limit=limit)
        return SessionsListResponse(sessions=[
            SessionListItem(
                session_id=row["id"],
                title=_session_title(
                    row.get("section"),
                    row.get("numero"),
                    row.get("idu"),
                    row.get("preview"),
                    row.get("geojson"),
                ),
                zones_summary=zones_summary(row.get("zones") or []),
                total_turns=row.get("total_turns") or 0,
                updated_at=str(row["updated_at"]),
                preview=row.get("preview"),
            )
            for row in rows
        ])

    @router.post("/session", response_model=SessionResponse)
    @bind
    def create_session(req: SessionRequest):
        from .chat import run_turn, serialize_contents, session_show_map

        parcelles_arg = (
            [{"section": p.section, "numero": p.numero} for p in req.parcelles]
            if req.parcelles
            else None
        )
        refs = normalize_parcel_refs(
            parcelles=parcelles_arg,
            idus=req.idus,
            section=req.section,
            numero=req.numero,
            idu=req.idu,
        )

        t0 = time.monotonic()
        current_profile = get_current_profile()
        is_france_live_profile = current_profile.slug == "france"

        zones: list[dict] = []
        session_section = None
        session_numero = None
        session_idu = None
        parcelles_refs = None

        if refs and not is_france_live_profile:
            zones_result = get_zonage_et_reglements(
                DB_CONFIG,
                parcelles=parcelles_arg,
                idus=req.idus,
                section=req.section,
                numero=req.numero,
                idu=req.idu,
            )
            if zones_result.get("error"):
                raise HTTPException(status_code=400, detail=zones_result["error"])

            zones = zones_result.get("zones", [])
            first = refs[0]
            session_section = first["section"] if first["type"] == "sn" else None
            session_numero = first["numero"] if first["type"] == "sn" else None
            session_idu = first["idu"] if first["type"] == "idu" else None
            parcelles_refs = parcelles_refs_to_json(
                parcelles=parcelles_arg,
                idus=req.idus,
                section=req.section,
                numero=req.numero,
                idu=req.idu,
            )
        elif refs and is_france_live_profile:
            # Profil France (GPU live) : pas de préchargement SQL local
            first = refs[0]
            session_section = first["section"] if first["type"] == "sn" else None
            session_numero = first["numero"] if first["type"] == "sn" else None
            session_idu = first["idu"] if first["type"] == "idu" else None
            parcelles_refs = parcelles_refs_to_json(
                parcelles=parcelles_arg,
                idus=req.idus,
                section=req.section,
                numero=req.numero,
                idu=req.idu,
            )
        else:
            logger.info("session sans référence parcellaire — zonage non préchargé")

        session_id = session_create(
            section=session_section,
            numero=session_numero,
            idu=session_idu,
            parcelles_refs=parcelles_refs,
            zones=zones,
            model=GEMINI_MODEL,
        )
        logger.info(f"session créée : {session_id} — zones : {zones_summary(zones)}")

        answer = None
        tool_calls = []
        usage = None
        latency_ms = int((time.monotonic() - t0) * 1000)

        if req.question:
            contents = [types.Content(role="user", parts=[types.Part(text=req.question)])]
            try:
                answer, tool_calls, usage, new_contents = run_turn(zones, contents)
            except Exception as e:
                logger.error(f"agentic_loop error : {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

            latency_ms = int((time.monotonic() - t0) * 1000)
            messages_insert(
                session_id=session_id,
                user_message=req.question,
                model_answer=answer,
                tool_calls=[tc.model_dump() for tc in tool_calls],
                gemini_parts=serialize_contents(new_contents),
                prompt_tokens=usage.prompt_tokens,
                candidate_tokens=usage.candidate_tokens,
                total_tokens=usage.total_tokens,
                latency_ms=latency_ms,
            )

        # Carte session (GET /map) dépend des tables géométriques locales ; on la désactive pour france.
        show_map = (
            session_show_map(session_get(session_id))
            if refs and not is_france_live_profile
            else False
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
    @bind
    def delete_session(session_id: str):
        if not session_delete(session_id):
            raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")

    @router.get("/session/{session_id}", response_model=SessionStateResponse)
    @bind
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
                    "role": m["role"],
                    "content": m["content"],
                    "tool_calls": m.get("tool_calls"),
                    "created_at": str(m["created_at"]),
                }
                for m in messages
            ],
        )
