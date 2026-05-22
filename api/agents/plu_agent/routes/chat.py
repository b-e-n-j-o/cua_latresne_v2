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
from ..commune_context import get_current_profile
from ..commune_profile import CommuneProfile
from .schemas import ToolCallLog, Usage

try:
    from ..tools import TOOL_DECLARATIONS, build_dispatch
except ImportError:
    from tools import TOOL_DECLARATIONS, build_dispatch

from .sessions import messages_get, messages_insert, session_get

logger = logging.getLogger("plu_api")

# ---------------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str = Field(..., description="Message de l'utilisateur")


class ChatResponse(BaseModel):
    session_id:  str
    answer:      str
    tool_calls:  list[ToolCallLog] = []
    usage:       Usage
    latency_ms:  int
    model:       str
    map_data:    dict | None = None  # GeoJSON optionnel ; préférer show_map + GET /session/{id}/map
    show_map:    bool = False        # True si la session a des refs parcellaires (carte via GET /map)


# ---------------------------------------------------------------------------
# Boucle agentique Gemini
# ---------------------------------------------------------------------------

def _build_gemini_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(api_key=GEMINI_API_KEY)
    return genai.Client()


def _build_system_prompt(zones: list[dict]) -> str:
    base = get_current_profile().system_prompt
    if not zones:
        return base

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
    return base + zones_block


def build_contents_from_db(messages: list[dict]) -> list:
    contents = []
    for msg in messages:
        role = "model" if msg["role"] == "model" else "user"
        contents.append(types.Content(
            role=role,
            parts=[types.Part(text=msg["content"])],
        ))
    return contents


def session_show_map(session: dict) -> bool:
    """True si la session porte des refs cadastrales (carte via GET /session/{id}/map)."""
    try:
        from ..tools.utils.parcel_geom import refs_from_session
    except ImportError:
        from tools.utils.parcel_geom import refs_from_session
    return bool(refs_from_session(session))


def _parcelle_result_for_llm(result: dict) -> dict:
    """Retire les géométries du tool get_parcelle pour le contexte LLM."""
    if result.get("error"):
        return result
    out = dict(result)
    p = out.get("parcelle")
    if isinstance(p, dict):
        out["parcelle"] = {k: v for k, v in p.items() if k != "geojson_wgs84"}
    parcelles = out.get("parcelles")
    if isinstance(parcelles, list):
        out["parcelles"] = [
            {k: v for k, v in item.items() if k != "geojson_wgs84"}
            for item in parcelles
            if isinstance(item, dict)
        ]
    unite = out.get("unite_fonciere")
    if isinstance(unite, dict):
        out["unite_fonciere"] = {
            k: v for k, v in unite.items() if k != "geojson_wgs84"
        }
    return out


def _strip_geo_from_items(items: list | None) -> list:
    if not isinstance(items, list):
        return []
    return [
        {k: v for k, v in item.items() if k != "geojson_geom"}
        for item in items
        if isinstance(item, dict)
    ]


def _contexte_result_for_llm(result: dict) -> dict:
    """Réponse get_contexte_parcelle sans géométries résiduelles."""
    if result.get("error"):
        return result
    out = dict(result)
    for key in ("surfaciques", "lineaires", "ponctuelles"):
        out[key] = _strip_geo_from_items(out.get(key))
    infos = out.get("informations")
    if isinstance(infos, dict):
        out["informations"] = {
            **infos,
            "surfaciques": _strip_geo_from_items(infos.get("surfaciques")),
            "lineaires": _strip_geo_from_items(infos.get("lineaires")),
            "ponctuelles": _strip_geo_from_items(infos.get("ponctuelles")),
        }
    return out


def _result_for_llm(tool_name: str, result: dict) -> dict:
    if tool_name == "get_contexte_parcelle":
        return _contexte_result_for_llm(result)
    if tool_name == "get_parcelle":
        return _parcelle_result_for_llm(result)
    return result


def _zones_for_summary(result: dict) -> list[dict]:
    zones = result.get("zones")
    if isinstance(zones, list):
        return zones
    return []


def _call_tool(dispatch: dict, name: str, args: dict) -> tuple[str, str, dict | None]:
    """
    Exécute le tool.
    Retourne (json_result, résumé_court, raw_result).
    raw_result est le dict Python brut (logs uniquement, non persisté).
    """
    fn = dispatch.get(name)
    if fn is None:
        err = {"error": f"Tool inconnu : {name}"}
        return json.dumps(err), f"tool inconnu : {name}", err

    result     = fn(**args)
    result_str = json.dumps(
        _result_for_llm(name, result),
        ensure_ascii=False,
        default=str,
    )

    # Résumé lisible pour les logs et la sidebar
    zone_items = _zones_for_summary(result)
    if zone_items:
        summary = ", ".join(
            f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
            for z in zone_items
        )
    elif result.get("zones_count") is not None or result.get("prescriptions_count") is not None:
        summary = (
            f"contexte parcelle — {result.get('zones_count', len(zone_items))} zone(s), "
            f"{result.get('prescriptions_count', 0)} prescription(s), "
            f"{result.get('servitudes_count', 0)} servitude(s), "
            f"{result.get('informations_count', 0)} information(s)"
        )
    elif "error" in result and result["error"]:
        summary = f"erreur : {result['error']}"
    else:
        summary = "ok"

    return result_str, summary, result


def _agentic_loop(
    client:   genai.Client,
    dispatch: dict,
    contents: list,
    config:   types.GenerateContentConfig,
) -> tuple[str, list[ToolCallLog], Usage]:
    """
    Boucle tool-calling jusqu'à réponse finale.
    Retourne (answer, tool_calls_log, usage).
    Les ToolCallLog incluent raw_result pour les logs (exclu de la persistance).
    """
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
            result_str, summary, raw_result = _call_tool(dispatch, fc.name, dict(fc.args))
            logger.info(f"  ↳ {summary}")

            tool_calls_log.append(ToolCallLog(
                name=fc.name,
                args=dict(fc.args),
                result_summary=summary,
                raw_result=raw_result,   # stocké en mémoire, exclu de la sérialisation Pydantic
            ))
            parts.append(types.Part.from_function_response(
                name=fc.name, response={"result": result_str}
            ))
        contents.append(types.Content(role="user", parts=parts))


def run_turn(
    zones: list[dict],
    contents: list,
) -> tuple[str, list[ToolCallLog], Usage]:
    """
    Exécute un tour agentique complet.
    Appelé aussi par sessions.py pour le premier tour à la création de session.
    """
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

def register(router: APIRouter, profile: CommuneProfile, bind) -> None:
    @router.post("/chat/{session_id}", response_model=ChatResponse)
    @bind
    def chat(session_id: str, req: ChatRequest):
        """
        Tour de conversation dans une session existante.
        show_map=true si la session a des refs parcellaires ; le frontend charge le GeoJSON via GET /map.
        """
        t0 = time.monotonic()

        session = session_get(session_id)
        if not session:
            raise HTTPException(status_code=404, detail=f"Session {session_id} introuvable.")

        zones = session.get("zones") or []
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

        show_map = session_show_map(session)

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
            map_data=None,
            show_map=show_map,
        )