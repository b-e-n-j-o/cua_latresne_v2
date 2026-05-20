"""
Chat PLU — prompt, boucle Gemini, endpoint POST /chat/{session_id}.

Pour modifier le comportement du LLM (prompt, tools, boucle) : tout est ici.
"""

import json
import logging
import time

from fastapi import APIRouter, HTTPException
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

from .._env import DB_CONFIG, GEMINI_API_KEY, GEMINI_MODEL

try:
    from ..tools import TOOL_DECLARATIONS, build_dispatch
except ImportError:
    from tools import TOOL_DECLARATIONS, build_dispatch

from .sessions import messages_get, messages_insert, session_get

logger = logging.getLogger("plu_api")
router = APIRouter()

# ---------------------------------------------------------------------------
# Prompt système (modifier ici pour ajuster le comportement du LLM)
# ---------------------------------------------------------------------------

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
# Schémas
# ---------------------------------------------------------------------------

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


class ChatResponse(BaseModel):
    session_id:  str
    answer:      str
    tool_calls:  list[ToolCallLog] = []
    usage:       Usage
    latency_ms:  int
    model:       str

# ---------------------------------------------------------------------------
# Boucle agentique Gemini
# ---------------------------------------------------------------------------

def _build_gemini_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(api_key=GEMINI_API_KEY)
    return genai.Client()


def _build_system_prompt(zones: list[dict]) -> str:
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


def build_contents_from_db(messages: list[dict]) -> list:
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "model" else "user"
        contents.append(types.Content(
            role=role,
            parts=[types.Part(text=msg["content"])],
        ))
    return contents


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


def run_turn(
    zones: list[dict],
    contents: list,
) -> tuple[str, list[ToolCallLog], Usage]:
    """Exécuté aussi par sessions.py pour le premier tour à la création."""
    client   = _build_gemini_client()
    dispatch = build_dispatch(DB_CONFIG)
    config   = types.GenerateContentConfig(
        system_instruction=_build_system_prompt(zones),
        tools=[TOOL_DECLARATIONS],
        temperature=0.1,
    )
    return _agentic_loop(client, dispatch, contents, config)

# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/chat/{session_id}", response_model=ChatResponse)
def chat(session_id: str, req: ChatRequest):
    """Tour de conversation dans une session existante."""
    t0 = time.monotonic()

    session = session_get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")

    zones    = session.get("zones") or []
    messages = messages_get(session_id)

    logger.info(
        f"session {session_id} — {len(messages)} messages — nouveau : {req.message!r}"
    )

    contents = build_contents_from_db(messages)
    contents.append(types.Content(role="user", parts=[types.Part(text=req.message)]))

    try:
        answer, tool_calls, usage = run_turn(zones, contents)
    except Exception as e:
        logger.error(f"agentic_loop error : {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    latency_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        f"session {session_id} — {latency_ms}ms | "
        f"tools={[tc.name for tc in tool_calls]} | tokens={usage.total_tokens}"
    )

    messages_insert(
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
