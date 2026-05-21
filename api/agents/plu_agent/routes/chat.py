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
from .schemas import ToolCallLog, Usage

try:
    from ..tools import TOOL_DECLARATIONS, build_dispatch
except ImportError:
    from tools import TOOL_DECLARATIONS, build_dispatch

from .sessions import messages_get, messages_insert, session_get

logger = logging.getLogger("plu_api")
router = APIRouter()

# ---------------------------------------------------------------------------
# Prompt système
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_BASE = """
Tu es un expert en droit de l'urbanisme français, spécialisé dans l'analyse des PLU.
Tu as accès au règlement PLU de la commune d'Argelès-sur-Mer (INSEE 66008).

Workflow :
1. Si la question mentionne une parcelle (section + numéro, ou IDU) → appelle
   get_zonage_et_reglements avec ces paramètres directement.
2. Si la question contient un GeoJSON → appelle get_zonage_et_reglements avec geojson=...
3. Pour un diagnostic rapide sans texte réglementaire → get_zones_for_geometry.
4. Si l'utilisateur demande à voir la carte, la localisation ou une représentation
   visuelle → appelle get_map_data (section+numéro ou IDU). La carte s'affiche
   automatiquement dans l'interface ; tu reçois seulement un résumé des zones, pas les coordonnées.
5. Pour une question de DROIT GÉNÉRAL de l'urbanisme (définitions, procédures,
   notions juridiques) non liée à une parcelle précise → appelle
   search_articles_urbanisme.
6. Si un NUMÉRO d'article est cité (ex: L421-6, R151-1) ou si un article
   référencé est nécessaire → appelle get_article_urbanisme_by_num.
   Les tools PLU (zonage) concernent Argelès ; le Code de l'urbanisme est national.

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


class ChatResponse(BaseModel):
    session_id:  str
    answer:      str
    tool_calls:  list[ToolCallLog] = []
    usage:       Usage
    latency_ms:  int
    model:       str
    map_data:    dict | None = None  # GeoJSON optionnel ; préférer show_map + GET /session/{id}/map
    show_map:    bool = False        # True si get_map_data a été appelé ce tour


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


def _map_result_for_llm(result: dict) -> dict:
    """
    Réponse allégée pour Gemini : pas de géométries.
    Les GeoJSON complets restent dans raw_result → map_data API ou GET /session/{id}/map.
    """
    if result.get("error"):
        return {"error": result["error"], "map_ready": False}

    parcelle = result.get("parcelle") or {}
    props = parcelle.get("properties") or {}
    zone_items = _zones_for_summary(result)

    return {
        "map_ready": True,
        "message": (
            "Carte générée pour l'utilisateur (affichage côté interface, sans coordonnées ici). "
            "Décris le zonage à partir des zones ci-dessous."
        ),
        "parcelle": {
            k: props.get(k)
            for k in ("idu", "section", "numero", "contenance")
            if props.get(k) is not None
        },
        "zones": [
            {
                "code_zone": z.get("code_zone"),
                "libelle": z.get("libelle"),
                "libelong": z.get("libelong"),
                "typezone": z.get("typezone"),
                "pct_parcelle_couverte": z.get("pct_parcelle_couverte"),
                "color": z.get("color"),
            }
            for z in zone_items
        ],
        "zones_count": len(zone_items),
        "error": None,
    }


def _parcelle_result_for_llm(result: dict) -> dict:
    """Retire geojson_wgs84 du tool get_parcelle pour le contexte LLM."""
    if result.get("error"):
        return result
    p = result.get("parcelle")
    if not isinstance(p, dict):
        return result
    slim = {k: v for k, v in p.items() if k != "geojson_wgs84"}
    return {**result, "parcelle": slim}


def _result_for_llm(tool_name: str, result: dict) -> dict:
    if tool_name == "get_map_data":
        return _map_result_for_llm(result)
    if tool_name == "get_parcelle":
        return _parcelle_result_for_llm(result)
    return result


def _zones_for_summary(result: dict) -> list[dict]:
    """Normalise zones : liste SQL (zonage) ou GeoJSON FeatureCollection (map_data)."""
    zones = result.get("zones")
    if not zones:
        return []
    if isinstance(zones, dict) and zones.get("type") == "FeatureCollection":
        return [f.get("properties") or {} for f in zones.get("features") or []]
    if isinstance(zones, list):
        return zones
    return []


def _call_tool(dispatch: dict, name: str, args: dict) -> tuple[str, str, dict | None]:
    """
    Exécute le tool.
    Retourne (json_result, résumé_court, raw_result).
    raw_result est le dict Python brut — utilisé pour extraire map_data sans re-parser.
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
    elif "parcelle" in result and result.get("parcelle"):
        summary = "données cartographiques ok"
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
    Les ToolCallLog incluent raw_result pour extraction post-boucle (ex: map_data).
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


def _map_requested(tool_calls: list[ToolCallLog]) -> bool:
    """True si get_map_data a réussi ce tour (signal UI, sans renvoyer le GeoJSON)."""
    for tc in tool_calls:
        if tc.name != "get_map_data" or not tc.raw_result:
            continue
        r = tc.raw_result
        if not r.get("error") and r.get("parcelle"):
            return True
    return False


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

@router.post("/chat/{session_id}", response_model=ChatResponse)
def chat(session_id: str, req: ChatRequest):
    """
    Tour de conversation dans une session existante.
    Si le LLM appelle get_map_data, show_map=true ; le frontend charge le GeoJSON via GET /map.
    """
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

    show_map = _map_requested(tool_calls)

    # Persistance — raw_result exclu par Field(exclude=True), pas de fuite en base
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