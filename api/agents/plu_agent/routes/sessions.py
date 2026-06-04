"""
Sessions PLU — persistance Supabase, chargement zonage, endpoints session(s).

Pour modifier la création de session, l'historique ou le SQL : tout est ici.
"""

import json
import logging
import time

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Depends, HTTPException
from google.genai import types
from pydantic import BaseModel, Field

from .._env import DB_CONFIG, GEMINI_MODEL
from ..commune_context import get_current_profile
from ..commune_profile import CommuneProfile
from .llm_raw_context import ensure_raw_llm_context_column
from .plu_auth import ensure_user_id_column, get_plu_user_id, session_belongs_to_user
from .schemas import RawLlmContextResponse, SessionMessageItem, ToolCallLog, Usage

try:
    from ..tools.utils import get_zonage_et_reglements
    from ..tools.utils.parcel_geom import (
        normalize_parcel_refs,
        parcelles_refs_to_json,
        refs_from_tool_calls,
    )
except ImportError:
    from tools.utils import get_zonage_et_reglements
    from tools.utils.parcel_geom import (
        normalize_parcel_refs,
        parcelles_refs_to_json,
        refs_from_tool_calls,
    )

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
    model_message_id: str | None = None
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
    messages:     list[SessionMessageItem]


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


def session_create(
    section,
    numero,
    idu,
    parcelles_refs,
    zones,
    model,
    user_id: str,
) -> str:
    """parcelles_refs : JSON stocké dans la colonne geojson (liste de refs, pas de géométrie)."""
    profile = get_current_profile()
    sessions = profile.sessions_table()
    sql = f"""
        INSERT INTO {sessions}
            (section, numero, idu, geojson, zones, model, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    conn = _db_conn()
    with conn:
        ensure_user_id_column(conn, profile.schema)
        with conn.cursor() as cur:
            cur.execute(sql, (
                section, numero, idu, parcelles_refs,
                json.dumps(zones, default=str),
                model,
                user_id,
            ))
            session_id = str(cur.fetchone()[0])
    conn.close()
    return session_id


def session_persist_refs(session_id: str, **refs_kw) -> bool:
    """
    Enregistre les refs parcellaires sur la session (carte GET /map, show_map).
    Retourne True si au moins une ref a été persistée.
    """
    refs = normalize_parcel_refs(
        refs_kw.get("parcelles"),
        refs_kw.get("idus"),
        refs_kw.get("section"),
        refs_kw.get("numero"),
        refs_kw.get("idu"),
    )
    if not refs:
        return False

    first = refs[0]
    section = first["section"] if first["type"] == "sn" else None
    numero = first["numero"] if first["type"] == "sn" else None
    idu_col = first["idu"] if first["type"] == "idu" else None

    parcelles_list = [
        {"section": r["section"], "numero": r["numero"]}
        for r in refs
        if r["type"] == "sn"
    ] or None
    idus_list = [r["idu"] for r in refs if r["type"] == "idu"] or None
    parcelles_refs = parcelles_refs_to_json(
        parcelles=parcelles_list,
        idus=idus_list,
        section=section,
        numero=numero,
        idu=idu_col,
    )

    sessions = get_current_profile().sessions_table()
    sql = f"""
        UPDATE {sessions}
        SET section = COALESCE(%s, section),
            numero  = COALESCE(%s, numero),
            idu     = COALESCE(%s, idu),
            geojson = COALESCE(%s, geojson),
            updated_at = now()
        WHERE id = %s;
    """
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql, (section, numero, idu_col, parcelles_refs, session_id))
    conn.close()
    return True


def session_persist_refs_from_tool_calls(
    session_id: str,
    tool_calls: list[dict] | None,
) -> bool:
    """Persiste les refs extraites des tool_calls d'un tour agentique."""
    kw = refs_from_tool_calls(tool_calls)
    if not kw:
        return False
    return session_persist_refs(session_id, **kw)


def session_get(session_id: str) -> dict | None:
    sessions = get_current_profile().sessions_table()
    sql = f"SELECT * FROM {sessions} WHERE id = %s;"
    conn = _db_conn()
    with conn:
        ensure_user_id_column(conn, get_current_profile().schema)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (session_id,))
            row = cur.fetchone()
    conn.close()
    if not row:
        return None
    session = dict(row)
    session["zones"] = _parse_json_field(session.get("zones")) or []
    if session.get("user_id") is not None:
        session["user_id"] = str(session["user_id"])
    return session


def require_session_for_user(session_id: str, user_id: str) -> dict:
    """Charge une session ou 404 si absente / autre utilisateur."""
    session = session_get(session_id)
    if not session_belongs_to_user(session, user_id):
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} introuvable.",
        )
    return session


def messages_get(session_id: str) -> list[dict]:
    messages = get_current_profile().messages_table()
    sql = f"""
        SELECT id, role, content, tool_calls, gemini_parts, created_at,
               (raw_llm_context IS NOT NULL) AS has_raw_context
        FROM {messages}
        WHERE session_id = %s
        ORDER BY created_at ASC, id ASC;
    """
    profile = get_current_profile()
    conn = _db_conn()
    with conn:
        ensure_raw_llm_context_column(conn, profile.schema)
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
    raw_llm_context: dict | None = None,
) -> str | None:
    """Insère user + model ; retourne l'id du message model (pour GET raw-context)."""
    profile = get_current_profile()
    messages = profile.messages_table()
    sessions = profile.sessions_table()
    sql_user = f"""
        INSERT INTO {messages}
            (session_id, role, content, tool_calls, gemini_parts,
             prompt_tokens, candidate_tokens, total_tokens, latency_ms)
        VALUES (%s, 'user', %s, NULL, NULL, NULL, NULL, NULL, NULL);
    """
    sql_model = f"""
        INSERT INTO {messages}
            (session_id, role, content, tool_calls, gemini_parts,
             prompt_tokens, candidate_tokens, total_tokens, latency_ms, raw_llm_context)
        VALUES (%s, 'model', %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    sql_session = f"""
        UPDATE {sessions}
        SET total_tokens = total_tokens + COALESCE(%s, 0),
            total_turns  = total_turns  + 1,
            updated_at   = now()
        WHERE id = %s;
    """
    conn = _db_conn()
    model_message_id: str | None = None
    with conn:
        ensure_raw_llm_context_column(conn, profile.schema)
        with conn.cursor() as cur:
            cur.execute(sql_user, (session_id, user_message))
            cur.execute(
                sql_model,
                (
                    session_id,
                    model_answer,
                    json.dumps(tool_calls, default=str) if tool_calls is not None else None,
                    json.dumps(gemini_parts, default=str) if gemini_parts is not None else None,
                    prompt_tokens,
                    candidate_tokens,
                    total_tokens,
                    latency_ms,
                    json.dumps(raw_llm_context, default=str) if raw_llm_context else None,
                ),
            )
            row = cur.fetchone()
            if row:
                model_message_id = str(row[0])
            cur.execute(sql_session, (total_tokens, session_id))
    conn.close()
    return model_message_id


def message_get_raw_context(session_id: str, message_id: str) -> dict | None:
    messages = get_current_profile().messages_table()
    sql = f"""
        SELECT id, session_id, role, raw_llm_context
        FROM {messages}
        WHERE id = %s AND session_id = %s
        LIMIT 1;
    """
    conn = _db_conn()
    with conn:
        ensure_raw_llm_context_column(conn, get_current_profile().schema)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (message_id, session_id))
            row = cur.fetchone()
    conn.close()
    if not row:
        return None
    raw = _parse_json_field(row.get("raw_llm_context"))
    if not raw:
        return None
    return {
        "message_id": str(row["id"]),
        "session_id": str(row["session_id"]),
        "role": row.get("role"),
        "raw_context": raw,
    }


def session_delete(session_id: str, user_id: str) -> bool:
    if not session_belongs_to_user(session_get(session_id), user_id):
        return False
    profile = get_current_profile()
    sql_msgs = f"DELETE FROM {profile.messages_table()} WHERE session_id = %s;"
    sql_session = (
        f"DELETE FROM {profile.sessions_table()} "
        f"WHERE id = %s AND user_id = %s RETURNING id;"
    )
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql_msgs, (session_id,))
            cur.execute(sql_session, (session_id, user_id))
            deleted = cur.fetchone() is not None
    conn.close()
    return deleted


def _sessions_list(user_id: str, limit: int = 50) -> list[dict]:
    profile = get_current_profile()
    sessions = profile.sessions_table()
    messages = profile.messages_table()
    sql = f"""
        SELECT s.id, s.section, s.numero, s.idu, s.zones, s.total_turns, s.updated_at,
            (SELECT content FROM {messages} m
             WHERE m.session_id = s.id AND m.role = 'user'
             ORDER BY m.created_at ASC LIMIT 1) AS preview
        FROM {sessions} s
        WHERE s.user_id = %s
        ORDER BY s.updated_at DESC
        LIMIT %s;
    """
    conn = _db_conn()
    with conn:
        ensure_user_id_column(conn, profile.schema)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (user_id, limit))
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
    def list_sessions(
        limit: int = 50,
        user_id: str = Depends(get_plu_user_id),
    ):
        limit = max(1, min(limit, 100))
        rows = _sessions_list(user_id=user_id, limit=limit)
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
    def create_session(
        req: SessionRequest,
        user_id: str = Depends(get_plu_user_id),
    ):
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
            user_id=user_id,
        )
        logger.info(f"session créée : {session_id} — zones : {zones_summary(zones)}")

        answer = None
        tool_calls = []
        usage = None
        model_message_id = None
        latency_ms = int((time.monotonic() - t0) * 1000)

        if req.question:
            contents = [types.Content(role="user", parts=[types.Part(text=req.question)])]
            try:
                answer, tool_calls, usage, new_contents, raw_llm_context = run_turn(
                    zones,
                    contents,
                    user_message=req.question,
                    prior_messages=[],
                )
            except Exception as e:
                logger.error(f"agentic_loop error : {e}", exc_info=True)
                raise HTTPException(status_code=500, detail=str(e))

            latency_ms = int((time.monotonic() - t0) * 1000)
            tool_calls_payload = [tc.model_dump() for tc in tool_calls]
            model_message_id = messages_insert(
                session_id=session_id,
                user_message=req.question,
                model_answer=answer,
                tool_calls=tool_calls_payload,
                gemini_parts=serialize_contents(new_contents),
                prompt_tokens=usage.prompt_tokens,
                candidate_tokens=usage.candidate_tokens,
                total_tokens=usage.total_tokens,
                latency_ms=latency_ms,
                raw_llm_context=raw_llm_context,
            )
            session_persist_refs_from_tool_calls(session_id, tool_calls_payload)

        # Carte session (GET /map) dépend des tables géométriques locales ; on la désactive pour france.
        session_row = session_get(session_id)
        show_map = False
        if not is_france_live_profile and session_row:
            msgs = messages_get(session_id) if req.question else None
            show_map = session_show_map(session_row, msgs)

        return SessionResponse(
            session_id=session_id,
            zones=zones,
            zones_summary=zones_summary(zones),
            answer=answer,
            tool_calls=tool_calls,
            usage=usage,
            latency_ms=latency_ms,
            model=GEMINI_MODEL,
            model_message_id=model_message_id if req.question else None,
            map_data=None,
            show_map=show_map,
        )

    @router.delete("/session/{session_id}", status_code=204)
    @bind
    def delete_session(
        session_id: str,
        user_id: str = Depends(get_plu_user_id),
    ):
        if not session_delete(session_id, user_id):
            raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")

    @router.get("/session/{session_id}", response_model=SessionStateResponse)
    @bind
    def get_session(
        session_id: str,
        user_id: str = Depends(get_plu_user_id),
    ):
        session = require_session_for_user(session_id, user_id)
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
                SessionMessageItem(
                    id=str(m["id"]),
                    role=m["role"],
                    content=m["content"],
                    tool_calls=m.get("tool_calls"),
                    created_at=str(m["created_at"]),
                    has_raw_context=bool(m.get("has_raw_context")),
                )
                for m in messages
            ],
        )

    @router.get(
        "/session/{session_id}/messages/{message_id}/raw-context",
        response_model=RawLlmContextResponse,
    )
    @bind
    def get_message_raw_context(
        session_id: str,
        message_id: str,
        user_id: str = Depends(get_plu_user_id),
    ):
        """Contexte brut LLM (prompt + sorties tools) pour un message assistant."""
        require_session_for_user(session_id, user_id)
        row = message_get_raw_context(session_id, message_id)
        if not row:
            raise HTTPException(
                status_code=404,
                detail="Contexte brut introuvable pour ce message.",
            )
        if row.get("role") != "model":
            raise HTTPException(
                status_code=400,
                detail="Le contexte brut n'est disponible que pour les réponses assistant.",
            )
        return RawLlmContextResponse(
            message_id=row["message_id"],
            session_id=row["session_id"],
            raw_context=row["raw_context"],
        )
