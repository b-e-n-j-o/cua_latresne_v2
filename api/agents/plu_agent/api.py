#!/usr/bin/env python3
"""
api.py
------
Router FastAPI — Agent PLU Argelès-sur-Mer avec sessions Supabase.

Monté dans main.py :
    app.include_router(plu_agent_argeles_router)

Endpoints (préfixe /api/plu/argeles) :
    POST /api/plu/argeles/session       — crée une session, charge le contexte
    POST /api/plu/argeles/chat/{id}     — tour de conversation
    GET  /api/plu/argeles/sessions      — liste des sessions (historique)
    GET  /api/plu/argeles/session/{id}  — état d'une session
    GET  /api/plu/argeles/tools         — tools disponibles (debug)
    GET  /api/plu/argeles/healthz

Lancer en standalone (dev) :
    python api.py
    uvicorn api.agents.plu_agent.api:app --reload --port 8001
"""

import os
import json
import logging
import time

import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from google import genai
from google.genai import types

try:
    from .tools import TOOL_DECLARATIONS, build_dispatch, get_zonage_et_reglements
except ImportError:
    from tools import TOOL_DECLARATIONS, build_dispatch, get_zonage_et_reglements

logger = logging.getLogger("plu_api")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host":            os.environ["SUPABASE_HOST"],
    "port":            int(os.environ.get("SUPABASE_PORT", 5432)),
    "dbname":          os.environ["SUPABASE_DB"],
    "user":            os.environ["SUPABASE_USER"],
    "password":        os.environ["SUPABASE_PASSWORD"],
    "sslmode":         "require",
    "connect_timeout": 15,
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")

SYSTEM_PROMPT_BASE = """
Tu es un expert en droit de l'urbanisme français, spécialisé dans l'analyse des PLU.
Tu as accès au règlement PLU de la commune d'Argelès-sur-Mer (INSEE 66008).

Workflow :
1. Si la question mentionne une parcelle (section + numéro, ou IDU) → appelle
   get_zonage_et_reglements avec ces paramètres directement.
2. Si la question contient un GeoJSON → appelle get_zonage_et_reglements avec geojson=...
3. Pour un diagnostic rapide sans texte réglementaire → get_zones_for_geometry.

Règles de réponse :
- Cite toujours les zones concernées et leurs pourcentages de couverture.
- Appuie-toi sur les articles du règlement pour justifier tes conclusions.
- Traite chaque zone séparément si plusieurs zones sont concernées.
- Signale si une zone est trouvée mais sans règlement disponible.
- Utilise EXACTEMENT les codes de zone retournés par les tools, sans les modifier.
- Formate tes réponses en Markdown (titres, listes, gras).
""".strip()

# ---------------------------------------------------------------------------
# Helpers DB (sessions)
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


def _session_create(
    section: str | None,
    numero:  str | None,
    idu:     str | None,
    geojson: str | None,
    zones:   list[dict],
    model:   str,
) -> str:
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


def _session_get(session_id: str) -> dict | None:
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


def _messages_get(session_id: str) -> list[dict]:
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


def _sessions_list(limit: int = 50) -> list[dict]:
    """Sessions récentes avec aperçu du premier message utilisateur."""
    sql = """
        SELECT
            s.id,
            s.section,
            s.numero,
            s.idu,
            s.zones,
            s.total_turns,
            s.updated_at,
            (
                SELECT content FROM argeles.plu_messages m
                WHERE m.session_id = s.id AND m.role = 'user'
                ORDER BY m.created_at ASC
                LIMIT 1
            ) AS preview
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


def _session_title(
    section: str | None,
    numero: str | None,
    idu: str | None,
    preview: str | None,
) -> str:
    if section and numero:
        return f"Parcelle {section} {numero}"
    if idu:
        return f"IDU {idu}"
    if preview:
        text = preview.strip().replace("\n", " ")
        return text[:72] + ("…" if len(text) > 72 else "")
    return "Conversation PLU"


def _messages_insert(
    session_id:       str,
    user_message:     str,
    model_answer:     str,
    tool_calls:       list[dict],
    prompt_tokens:    int | None,
    candidate_tokens: int | None,
    total_tokens:     int | None,
    latency_ms:       int,
) -> None:
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
        SET
            total_tokens = total_tokens + COALESCE(%s, 0),
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


# ---------------------------------------------------------------------------
# Schémas Pydantic
# ---------------------------------------------------------------------------

class SessionRequest(BaseModel):
    """Crée une session. Fournir section+numero, idu, ou geojson."""
    section: str | None = Field(None, examples=["AC"])
    numero:  str | None = Field(None, examples=["45"])
    idu:     str | None = Field(None, examples=["66008000AC0045"])
    geojson: str | None = Field(None, description="GeoJSON WGS84 si géométrie ad hoc")
    question: str | None = Field(
        None,
        examples=["Cette parcelle est-elle constructible ?"],
        description="Question initiale optionnelle — déclenche un premier tour immédiatement.",
    )


class ChatRequest(BaseModel):
    message: str = Field(..., description="Message de l'utilisateur")


class ToolCallLog(BaseModel):
    name:           str
    args:           dict
    result_summary: str


class Usage(BaseModel):
    prompt_tokens:    int | None = None
    candidate_tokens: int | None = None
    total_tokens:     int | None = None


class SessionResponse(BaseModel):
    session_id:    str
    zones:         list[dict]
    zones_summary: str
    answer:        str | None = None
    tool_calls:    list[ToolCallLog] = []
    usage:         Usage | None = None
    latency_ms:    int | None = None
    model:         str


class ChatResponse(BaseModel):
    session_id:  str
    answer:      str
    tool_calls:  list[ToolCallLog] = []
    usage:       Usage
    latency_ms:  int
    model:       str


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
# Boucle agentique
# ---------------------------------------------------------------------------

def _call_tool(dispatch: dict, name: str, args: dict) -> tuple[str, str]:
    fn = dispatch.get(name)
    if fn is None:
        err = {"error": f"Tool inconnu : {name}"}
        return json.dumps(err), f"tool inconnu : {name}"
    result     = fn(**args)
    result_str = json.dumps(result, ensure_ascii=False, default=str)
    if "zones" in result and result["zones"]:
        summary = ", ".join(
            f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
            for z in result["zones"]
        )
    elif "error" in result and result["error"]:
        summary = f"erreur : {result['error']}"
    else:
        summary = "ok"
    return result_str, summary


def _agentic_loop(
    client:   genai.Client,
    dispatch: dict,
    contents: list,
    config:   types.GenerateContentConfig,
) -> tuple[str, list[ToolCallLog], Usage]:
    tool_calls_log: list[ToolCallLog] = []
    total_prompt = total_candidates = total_tokens = 0

    while True:
        response  = client.models.generate_content(
            model=GEMINI_MODEL, contents=contents, config=config
        )
        candidate = response.candidates[0]
        contents.append(candidate.content)

        meta = getattr(response, "usage_metadata", None)
        if meta:
            total_prompt     += getattr(meta, "prompt_token_count",     0) or 0
            total_candidates += getattr(meta, "candidates_token_count", 0) or 0
            total_tokens     += getattr(meta, "total_token_count",      0) or 0

        function_calls = [
            p.function_call for p in candidate.content.parts
            if p.function_call is not None
        ]

        if not function_calls:
            usage = Usage(
                prompt_tokens=total_prompt or None,
                candidate_tokens=total_candidates or None,
                total_tokens=total_tokens or None,
            )
            return response.text, tool_calls_log, usage

        parts = []
        for fc in function_calls:
            logger.info(f"tool_call → {fc.name}({dict(fc.args)})")
            result_str, summary = _call_tool(dispatch, fc.name, dict(fc.args))
            logger.info(f"  ↳ {summary}")
            tool_calls_log.append(ToolCallLog(
                name=fc.name, args=dict(fc.args), result_summary=summary
            ))
            parts.append(types.Part.from_function_response(
                name=fc.name, response={"result": result_str}
            ))
        contents.append(types.Content(role="user", parts=parts))


def _build_system_prompt(zones: list[dict]) -> str:
    """System prompt enrichi avec les règlements — chargé à la création de session."""
    if not zones:
        return SYSTEM_PROMPT_BASE

    zones_block = "\n\n## Contexte réglementaire chargé pour cette session\n\n"
    for z in zones:
        code      = z.get("code_zone", "?")
        pct       = z.get("pct_parcelle_couverte", "?")
        surf      = z.get("superficie_intersection_m2", "?")
        nom       = z.get("nom_zone") or code
        reglement = z.get("reglementation") or "(règlement non disponible)"
        zones_block += (
            f"### Zone {code} — {nom} "
            f"({pct}% de la parcelle, {surf} m²)\n\n"
            f"{reglement}\n\n---\n\n"
        )
    return SYSTEM_PROMPT_BASE + zones_block


def _build_contents_from_db(messages: list[dict]) -> list:
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "model" else "user"
        contents.append(types.Content(
            role=role,
            parts=[types.Part(text=msg["content"])],
        ))
    return contents


def _zones_summary(zones: list[dict]) -> str:
    return ", ".join(
        f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
        for z in zones
    ) or "aucune zone trouvée"


def _build_gemini_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(api_key=GEMINI_API_KEY)
    return genai.Client()


def _run_turn(
    zones: list[dict],
    contents: list,
) -> tuple[str, list[ToolCallLog], Usage]:
    client   = _build_gemini_client()
    dispatch = build_dispatch(DB_CONFIG)
    config   = types.GenerateContentConfig(
        system_instruction=_build_system_prompt(zones),
        tools=[TOOL_DECLARATIONS],
        temperature=0.1,
    )
    return _agentic_loop(client, dispatch, contents, config)


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api/plu/argeles", tags=["plu-agent-argeles"])


@router.get("/healthz")
def health():
    return {"status": "ok", "model": GEMINI_MODEL}


@router.get("/tools")
def list_tools():
    return {
        "tools": [
            {"name": fd.name, "description": fd.description}
            for fd in TOOL_DECLARATIONS.function_declarations
        ]
    }


@router.get("/sessions", response_model=SessionsListResponse)
def list_sessions(limit: int = 50):
    """Liste les sessions récentes pour l'historique du chat."""
    limit = max(1, min(limit, 100))
    rows = _sessions_list(limit=limit)
    return SessionsListResponse(sessions=[
        SessionListItem(
            session_id=row["id"],
            title=_session_title(row.get("section"), row.get("numero"), row.get("idu"), row.get("preview")),
            zones_summary=_zones_summary(row.get("zones") or []),
            total_turns=row.get("total_turns") or 0,
            updated_at=str(row["updated_at"]),
            preview=row.get("preview"),
        )
        for row in rows
    ])


@router.post("/session", response_model=SessionResponse)
def create_session(req: SessionRequest):
    """
    Crée une session de chat PLU.
    Charge le contexte géographique (zones + règlements) via PostGIS.
    Si `question` est fournie, exécute aussi le premier tour de conversation.
    """
    has_geo = any([req.section and req.numero, req.idu, req.geojson])
    if not has_geo:
        raise HTTPException(
            status_code=422,
            detail="Fournir section+numero, idu, ou geojson.",
        )

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

    session_id = _session_create(
        section=req.section.upper() if req.section else None,
        numero=str(req.numero).strip() if req.numero else None,
        idu=req.idu,
        geojson=req.geojson,
        zones=zones,
        model=GEMINI_MODEL,
    )
    logger.info(f"session créée : {session_id} — zones : {_zones_summary(zones)}")

    answer     = None
    tool_calls: list[ToolCallLog] = []
    usage      = None
    latency_ms = int((time.monotonic() - t0) * 1000)

    if req.question:
        contents = [types.Content(role="user", parts=[types.Part(text=req.question)])]
        try:
            answer, tool_calls, usage = _run_turn(zones, contents)
        except Exception as e:
            logger.error(f"agentic_loop error : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

        latency_ms = int((time.monotonic() - t0) * 1000)
        _messages_insert(
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
        zones_summary=_zones_summary(zones),
        answer=answer,
        tool_calls=tool_calls,
        usage=usage,
        latency_ms=latency_ms,
        model=GEMINI_MODEL,
    )


@router.post("/chat/{session_id}", response_model=ChatResponse)
def chat(session_id: str, req: ChatRequest):
    """
    Tour de conversation dans une session existante.
    L'historique et le contexte réglementaire sont rechargés depuis Supabase.
    """
    t0 = time.monotonic()

    session = _session_get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")

    zones    = session.get("zones") or []
    messages = _messages_get(session_id)

    logger.info(
        f"session {session_id} — {len(messages)} messages — nouveau : {req.message!r}"
    )

    contents = _build_contents_from_db(messages)
    contents.append(types.Content(role="user", parts=[types.Part(text=req.message)]))

    try:
        answer, tool_calls, usage = _run_turn(zones, contents)
    except Exception as e:
        logger.error(f"agentic_loop error : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    latency_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        f"session {session_id} — {latency_ms}ms | "
        f"tools={[tc.name for tc in tool_calls]} | tokens={usage.total_tokens}"
    )

    _messages_insert(
        session_id=session_id,
        user_message=req.message,
        model_answer=answer,
        tool_calls=[tc.model_dump() for tc in tool_calls],
        prompt_tokens=usage.prompt_tokens,
        candidate_tokens=usage.candidate_tokens,
        total_tokens=usage.total_tokens,
        latency_ms=latency_ms,
    )

    return ChatResponse(
        session_id=session_id,
        answer=answer,
        tool_calls=tool_calls,
        usage=usage,
        latency_ms=latency_ms,
        model=GEMINI_MODEL,
    )


@router.get("/session/{session_id}", response_model=SessionStateResponse)
def get_session(session_id: str):
    """État complet d'une session avec tous ses messages."""
    session = _session_get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")
    messages = _messages_get(session_id)
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


# ---------------------------------------------------------------------------
# App standalone (dev — CORS géré par main.py en prod)
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Agent PLU Argelès-sur-Mer",
    description="LLM outillé pour l'analyse réglementaire PLU via PostGIS — sessions Supabase",
    version="2.0.0",
)
app.include_router(router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
