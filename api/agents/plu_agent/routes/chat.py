"""
Chat PLU — prompt, boucle Gemini, endpoint POST /chat/{session_id}.

Pour modifier le comportement du LLM (prompt, tools, boucle) : tout est ici.
"""

import json
import logging
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

from .._env import DB_CONFIG, GEMINI_MODEL, MISTRAL_CHAT_MODEL
from ..commune_context import get_current_profile
from ..commune_profile import CommuneProfile
from ..vertex_client import build_vertex_client
from .llm_raw_context import TurnRawContextCapture, build_capture
from .schemas import CONTEXT_TOKEN_LIMIT, ToolCallLog, Usage, context_limit_fields

try:
    from ..tools import build_dispatch, build_tool_declarations
except ImportError:
    from tools import build_dispatch, build_tool_declarations

from .plu_auth import get_plu_user_id, is_plu_superadmin
from .sessions import (
    messages_get,
    messages_insert,
    require_session_for_user,
    session_get,
    session_persist_refs,
    session_persist_refs_from_tool_calls,
)

logger = logging.getLogger("plu_api")

# ---------------------------------------------------------------------------
# Schémas
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str = Field(..., description="Message de l'utilisateur")
    provider: Literal["gemini", "mistral"] | None = Field(
        None,
        description="Stack LLM. Défaut : celle de la session (sinon Gemini/Vertex).",
    )
    response_mode: Literal["concis", "approfondi"] | None = Field(
        None,
        description="Longueur de réponse (prompt). Défaut concis. Pas de troncature max_tokens.",
    )


class ChatResponse(BaseModel):
    session_id:  str
    answer:      str
    tool_calls:  list[ToolCallLog] = []
    usage:       Usage | None = None
    latency_ms:  int
    model:       str
    provider:    str = "mistral"
    model_message_id: str | None = None
    map_data:    dict | None = None  # GeoJSON optionnel ; préférer show_map + GET /session/{id}/map
    show_map:    bool = False        # True si la session a des refs parcellaires (carte via GET /map)
    context_tokens: int | None = None
    context_limit: int = CONTEXT_TOKEN_LIMIT
    context_limit_reached: bool = False


# ---------------------------------------------------------------------------
# Boucle agentique Gemini
# ---------------------------------------------------------------------------

def _build_gemini_client() -> genai.Client:
    return build_vertex_client()


LLM_PROBE_PROMPT = "Réponds uniquement par le mot PONG."


def classify_llm_error(exc: BaseException) -> str:
    """Classe une exception Gemini : quota, auth, model, network, error."""
    text = str(exc).lower()
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    try:
        code = int(code) if code is not None else None
    except (TypeError, ValueError):
        code = None
    if code == 429 or any(
        token in text
        for token in ("resource_exhausted", "quota", "rate limit", "rate_limit")
    ):
        return "quota"
    if "service_disabled" in text or "has not been used in project" in text:
        return "service_disabled"
    if "tier_not_allowed" in text or "not available in your subscription" in text:
        return "tier"
    if code in (401, 403) or any(
        token in text
        for token in ("unauthenticated", "permission_denied", "api key", "api_key")
    ):
        return "auth"
    if code == 404 or "not_found" in text or "not found" in text:
        return "model"
    if any(
        token in text
        for token in ("timeout", "timed out", "connection", "unavailable", "temporarily")
    ):
        return "network"
    return "error"


def _text_from_gemini_response(response) -> str:
    text = (getattr(response, "text", None) or "").strip()
    if text:
        return text
    candidates = getattr(response, "candidates", None) or []
    if not candidates:
        return ""
    content = getattr(candidates[0], "content", None)
    parts = getattr(content, "parts", None) or []
    return "".join((getattr(p, "text", None) or "") for p in parts).strip()


def _probe_generate(model: str) -> dict:
    t0 = time.monotonic()
    try:
        client = _build_gemini_client()
        config_kwargs: dict = {
            "temperature": 0,
            "max_output_tokens": 64,
        }
        thinking_cls = getattr(types, "ThinkingConfig", None)
        if thinking_cls is not None:
            config_kwargs["thinking_config"] = thinking_cls(thinking_budget=0)
        response = client.models.generate_content(
            model=model,
            contents=LLM_PROBE_PROMPT,
            config=types.GenerateContentConfig(**config_kwargs),
        )
        text = _text_from_gemini_response(response)
        candidates = getattr(response, "candidates", None) or []
        latency_ms = int((time.monotonic() - t0) * 1000)
        if not candidates:
            return {
                "ok": False,
                "model": model,
                "reason": "empty",
                "error": "Réponse Gemini sans candidat",
                "latency_ms": latency_ms,
            }
        return {
            "ok": True,
            "model": model,
            "answer": text,
            "latency_ms": latency_ms,
        }
    except Exception as e:
        logger.warning("llm generate probe failed : %s", e)
        return {
            "ok": False,
            "model": model,
            "reason": classify_llm_error(e),
            "error": str(e),
            "latency_ms": int((time.monotonic() - t0) * 1000),
        }


def _probe_embedding() -> dict:
    """Même appel Vertex que search_articles_urbanisme (_embed_query)."""
    try:
        from ..tools.recherche_articles import EMBED_DIM, EMBED_MODEL, _embed_query
    except ImportError:
        from tools.recherche_articles import EMBED_DIM, EMBED_MODEL, _embed_query

    t0 = time.monotonic()
    try:
        vec = _embed_query("ping urbanisme")
        dim = len(vec) if vec is not None else 0
        latency_ms = int((time.monotonic() - t0) * 1000)
        if dim != EMBED_DIM:
            return {
                "ok": False,
                "model": EMBED_MODEL,
                "stack": "vertex",
                "reason": "empty",
                "error": f"Embedding inattendu : {dim}d (attendu {EMBED_DIM}d)",
                "dim": dim,
                "latency_ms": latency_ms,
            }
        return {
            "ok": True,
            "model": EMBED_MODEL,
            "stack": "vertex",
            "dim": dim,
            "latency_ms": latency_ms,
        }
    except Exception as e:
        logger.warning("llm embedding probe failed : %s", e)
        return {
            "ok": False,
            "model": EMBED_MODEL,
            "stack": "vertex",
            "reason": classify_llm_error(e),
            "error": str(e),
            "latency_ms": int((time.monotonic() - t0) * 1000),
        }


def probe_llm(*, profile: CommuneProfile | None = None) -> dict:
    """
    Ping Vertex : generateContent (chat) + embed_content (recherche articles).

    Sans tools ni session.  Les deux doivent répondre — un 403 embeddings
    ne doit plus passer inaperçu derrière un generate 200.
    """
    current = profile or get_current_profile()
    model = current.gemini_model or GEMINI_MODEL
    generate = _probe_generate(model)
    embedding = _probe_embedding()
    failed = next((p for p in (generate, embedding) if not p.get("ok")), None)
    return {
        "ok": failed is None,
        "stack": "vertex",
        "commune": current.slug,
        "model": model,
        "answer": generate.get("answer"),
        "generate": generate,
        "embedding": embedding,
        "reason": None if failed is None else failed.get("reason"),
        "error": None if failed is None else failed.get("error"),
        "latency_ms": (generate.get("latency_ms") or 0) + (embedding.get("latency_ms") or 0),
    }

def _resolve_parcel_identity(refs_kw: dict | None) -> dict | None:
    """Lookup SS NNNN puis get_parcelle sur les feuilles trouvées."""
    if not refs_kw:
        return None
    try:
        from ..commune_context import get_current_profile_optional
        from ..tools.parcelle import get_parcelle
        from ..tools.utils.parcel_geom import (
            found_rows_to_refs_kwargs,
            lookup_parcel_refs,
            normalize_parcel_refs,
        )
        from ..tools.utils.parcel_ref_parse import official_label
    except ImportError:
        from commune_context import get_current_profile_optional
        from tools.parcelle import get_parcelle
        from tools.utils.parcel_geom import (
            found_rows_to_refs_kwargs,
            lookup_parcel_refs,
            normalize_parcel_refs,
        )
        from tools.utils.parcel_ref_parse import official_label

    keys = ("parcelles", "idus", "section", "numero", "idu")
    kw = {k: refs_kw[k] for k in keys if refs_kw.get(k) is not None}
    if not kw:
        return None
    refs = normalize_parcel_refs(
        kw.get("parcelles"), kw.get("idus"), kw.get("section"), kw.get("numero"), kw.get("idu")
    )
    looked = lookup_parcel_refs(DB_CONFIG, refs)
    profile = get_current_profile_optional()
    meta = {
        "commune": profile.label if profile else None,
        "insee": profile.insee if profile else None,
        "missing_labels": [
            (
                r["idu"]
                if r.get("type") == "idu"
                else official_label(r["section"], r["numero"])
            )
            for r in looked["missing"]
        ],
    }
    found_kw = found_rows_to_refs_kwargs(looked["found"])
    if not found_kw:
        return {
            "error": "Aucune des parcelles demandées n'existe dans cette commune.",
            "parcelle": None,
            "parcelles": [],
            **meta,
        }
    identity = get_parcelle(DB_CONFIG, **found_kw)
    identity.update(meta)
    return identity


RESPONSE_MODES = ("concis", "approfondi")
DEFAULT_RESPONSE_MODE = "concis"

_RESPONSE_MODE_PROMPTS = {
    "concis": (
        "\n\n## Longueur de réponse (mode concis)\n"
        "Réponds de façon ciblée : uniquement ce que la question demande.\n"
        "N'appelle get_reglement_zone, get_reglement_pprmvt ni get_reglement_ppri "
        "que si la question porte sur le règlement, la constructibilité ou une règle précise.\n"
        "Ne recopie pas de longs extraits. Vise 1 à 3 paragraphes ou une liste courte.\n"
        "Cite les codes de zone et % quand c'est utile. "
        "Une ligne en fin : ce qu'on peut approfondir si besoin.\n"
    ),
    "approfondi": (
        "\n\n## Longueur de réponse (mode approfondi)\n"
        "Réponse détaillée et structurée. Cite les extraits utiles des règlements "
        "chargés via les tools. Traite les spécificités de chaque zone concernée. "
        "Si le contexte est trop large, invite à relancer sur un point précis.\n"
    ),
}


def normalize_response_mode(raw: str | None) -> str:
    v = (raw or DEFAULT_RESPONSE_MODE).strip().lower()
    return v if v in _RESPONSE_MODE_PROMPTS else DEFAULT_RESPONSE_MODE


def _build_system_prompt(
    zones: list[dict],
    *,
    parcel_identity: dict | None = None,
    response_mode: str | None = None,
) -> str:
    """Prompt système + identité parcellaire + mode de longueur."""
    try:
        from ..tools.utils.parcel_ref_parse import format_parcel_identity_prompt
    except ImportError:
        from tools.utils.parcel_ref_parse import format_parcel_identity_prompt

    mode = normalize_response_mode(response_mode)
    parts = [get_current_profile().system_prompt]
    ident = format_parcel_identity_prompt(parcel_identity)
    if ident:
        parts.append(ident)
    parts.append(_RESPONSE_MODE_PROMPTS[mode])
    return "".join(parts)


def serialize_contents(items: list[types.Content]) -> list[dict]:
    """Sérialise la chaîne Gemini d'un tour pour persistance JSONB."""
    return [c.model_dump(mode="json", exclude_none=True) for c in items]


def deserialize_contents(blob) -> list[types.Content]:
    """Reconstruit les Content depuis le JSONB (psycopg2 renvoie déjà du Python)."""
    if isinstance(blob, str):
        blob = json.loads(blob)
    return [types.Content.model_validate(d) for d in (blob or [])]


def build_contents_from_db(messages: list[dict]) -> list:
    """
    Rejoue l'historique Gemini : gemini_parts (fc/fr + texte) si présent,
    sinon fallback texte plat (anciennes sessions).
    """
    contents = []
    for msg in messages:
        parts_blob = msg.get("gemini_parts")
        if msg["role"] == "model" and parts_blob:
            contents.extend(deserialize_contents(parts_blob))
        else:
            role = "model" if msg["role"] == "model" else "user"
            contents.append(types.Content(
                role=role,
                parts=[types.Part(text=msg["content"])],
            ))
    return contents


def session_show_map(session: dict, messages: list[dict] | None = None) -> bool:
    """True si la session a des refs cadastrales (stockées ou déductibles de l'historique)."""
    try:
        from ..tools.utils.parcel_geom import resolve_session_refs
    except ImportError:
        from tools.utils.parcel_geom import resolve_session_refs
    return bool(resolve_session_refs(session, messages))


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
    parcelles = out.get("parcelles")
    if isinstance(parcelles, list):
        out["parcelles"] = [
            {
                k: v
                for k, v in item.items()
                if k not in {"geojson_wgs84", "geom_wkb", "geom"}
            }
            for item in parcelles
            if isinstance(item, dict)
        ]
    zones = out.get("zones")
    if isinstance(zones, list):
        drop = {"reglementation", "texte_id", "texte_ids", "codes_texte", "textes"}
        out["zones"] = [
            {k: v for k, v in z.items() if k not in drop}
            if isinstance(z, dict)
            else z
            for z in zones
        ]
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


def _extra_layers_summary(result: dict) -> str:
    """Résumé des couches catalogue intersectées (groupées par layer_id)."""
    groups = result.get("couches_supplementaires") or {}
    counts: dict[str, int] = {}
    for items in groups.values():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            lid = str(item.get("layer_id") or "?")
            counts[lid] = counts.get(lid, 0) + 1
    return ", ".join(f"{lid}({n})" for lid, n in sorted(counts.items()))


def _call_tool(
    dispatch: dict,
    name: str,
    args: dict,
    capture: TurnRawContextCapture | None = None,
) -> tuple[str, str, dict | None]:
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
    result_for_llm = _result_for_llm(name, result)
    result_str = json.dumps(
        result_for_llm,
        ensure_ascii=False,
        default=str,
    )

    if capture is not None:
        capture.add_tool_invocation(
            name=name,
            args=args,
            raw_result=result,
            result_sent_to_llm=result_str,
            result_summary="",  # rempli après calcul du summary
        )

    # Résumé lisible pour les logs et la sidebar
    zone_items = _zones_for_summary(result)
    extra_summary = _extra_layers_summary(result)
    if zone_items:
        summary = ", ".join(
            f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
            for z in zone_items
        )
        if extra_summary:
            summary += f" | extra: {extra_summary}"
        elif result.get("couches_supplementaires_count") == 0 and name == "get_contexte_parcelle":
            summary += " | extra: aucune"
    elif result.get("zones_count") is not None or result.get("prescriptions_count") is not None:
        extra_bit = f", extra: {extra_summary}" if extra_summary else ""
        summary = (
            f"contexte parcelle — {result.get('zones_count', len(zone_items))} zone(s), "
            f"{result.get('prescriptions_count', 0)} prescription(s), "
            f"{result.get('servitudes_count', 0)} servitude(s), "
            f"{result.get('informations_count', 0)} information(s)"
            f"{extra_bit}"
        )
    elif result.get("found") is not None and name == "get_reglement_zone":
        if result.get("found"):
            n = len(result.get("reglementation") or "")
            summary = f"règlement {result.get('code_zone')} — {n} caractères"
        else:
            summary = f"zone {result.get('code_zone')} — non trouvé"
    elif name == "get_reglement_pprmvt":
        dg_ok = result.get("dispositions_generales_found", 0)
        z_ok = result.get("zones_found", 0)
        z_req = len(result.get("zones_requested") or [])
        summary = f"PPRMVT — DG {dg_ok}/3, zones {z_ok}/{z_req}"
        if result.get("error"):
            summary += f" | {result['error']}"
    elif name == "get_reglement_ppri":
        dc_ok = result.get("dispositions_communes_found", 0)
        z_ok = result.get("zones_found", 0)
        z_req = len(result.get("zones_requested") or [])
        summary = f"PPRI — DG {dc_ok}/1, zones {z_ok}/{z_req}"
        if result.get("error"):
            summary += f" | {result['error']}"
    elif name == "get_ppr_reglement":
        z_req = result.get("zone_codes_requested") or []
        z_ok = result.get("zones_found", 0)
        labels = result.get("sous_zone_labels") or []
        summary = f"PPR — {z_ok} bloc(s), zones {', '.join(z_req) or '—'}"
        if labels:
            summary += f" | sous-zones {', '.join(labels[:3])}"
        if result.get("hors_zonage_ppr"):
            summary += " | zone III (hors zonage 1/2)"
        if result.get("error"):
            summary += f" | {result['error']}"
    elif name == "get_pprif_reglement":
        z_req = result.get("zone_codes_requested") or []
        z_ok = result.get("zones_found", 0)
        summary = f"PPRIF — {z_ok} bloc(s), zones {', '.join(z_req) or '—'}"
        if result.get("error"):
            summary += f" | {result['error']}"
    elif "error" in result and result["error"]:
        summary = f"erreur : {result['error']}"
    else:
        summary = "ok"

    if capture is not None and capture.tool_invocations:
        capture.tool_invocations[-1]["result_summary"] = summary

    return result_str, summary, result


def _agentic_loop(
    client:   genai.Client,
    dispatch: dict,
    contents: list,
    config:   types.GenerateContentConfig,
    capture:  TurnRawContextCapture | None = None,
) -> tuple[str, list[ToolCallLog], Usage]:
    """
    Boucle tool-calling jusqu'à réponse finale.
    Retourne (answer, tool_calls_log, usage).
    Les ToolCallLog incluent raw_result pour les logs (exclu de la persistance).
    """
    tool_calls_log: list[ToolCallLog] = []
    total_prompt = total_candidates = total_tokens = 0
    last_prompt = 0
    gemini_round = 0

    while True:
        gemini_round += 1
        response  = client.models.generate_content(
            model=GEMINI_MODEL, contents=contents, config=config
        )
        candidate = response.candidates[0]
        contents.append(candidate.content)

        meta = getattr(response, "usage_metadata", None)
        if meta:
            last_prompt = getattr(meta, "prompt_token_count", 0) or 0
            total_prompt     += last_prompt
            total_candidates += getattr(meta, "candidates_token_count", 0) or 0
            total_tokens     += getattr(meta, "total_token_count",      0) or 0

        function_calls = [
            p.function_call for p in candidate.content.parts
            if p.function_call is not None
        ]

        if capture is not None:
            capture.add_gemini_round(
                meta,
                round_index=gemini_round,
                with_tool_calls=bool(function_calls),
            )

        if not function_calls:
            usage = Usage(
                prompt_tokens=total_prompt or None,
                candidate_tokens=total_candidates or None,
                total_tokens=total_tokens or None,
                context_tokens=last_prompt or None,
            )
            if capture is not None:
                capture.set_model_answer(response.text)
            return response.text, tool_calls_log, usage

        parts = []
        for fc in function_calls:
            logger.info(f"tool_call → {fc.name}({dict(fc.args)})")
            result_str, summary, raw_result = _call_tool(
                dispatch, fc.name, dict(fc.args), capture=capture
            )
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


def serialize_turn(provider: str, new_contents) -> list | dict:
    if provider == "mistral":
        from ..mistral_client import serialize_mistral_turn

        return serialize_mistral_turn(new_contents)
    return serialize_contents(new_contents)


def run_turn(
    zones: list[dict],
    contents: list,
    *,
    user_message: str = "",
    prior_messages: list[dict] | None = None,
    provider: str = "gemini",
    parcel_identity: dict | None = None,
    response_mode: str | None = None,
) -> tuple[str, list[ToolCallLog], Usage, list, dict]:
    """
    Exécute un tour agentique complet.
    Appelé aussi par sessions.py pour le premier tour à la création de session.
    Retourne aussi new_contents : la chaîne Gemini du tour (fc/fr + texte final).
    """
    if provider == "mistral":
        from .chat_mistral import run_turn_mistral

        return run_turn_mistral(
            zones,
            contents,
            user_message=user_message,
            prior_messages=prior_messages,
            parcel_identity=parcel_identity,
            response_mode=response_mode,
        )

    profile    = get_current_profile()
    tool_names = profile.llm_tool_names
    client     = _build_gemini_client()
    dispatch   = build_dispatch(DB_CONFIG, tool_names)
    system_instruction = _build_system_prompt(
        zones, parcel_identity=parcel_identity, response_mode=response_mode
    )
    config     = types.GenerateContentConfig(
        system_instruction=system_instruction,
        tools=[build_tool_declarations(tool_names)],
        temperature=0.1,
    )
    capture = build_capture(
        system_instruction=system_instruction,
        session_zones=zones,
        user_message=user_message,
        prior_messages=prior_messages or [],
        commune_slug=profile.slug,
        model_name=profile.gemini_model or GEMINI_MODEL,
    )
    start = len(contents)
    answer, tool_calls, usage = _agentic_loop(
        client, dispatch, contents, config, capture=capture
    )
    return answer, tool_calls, usage, contents[start:], capture.to_dict()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

def register(router: APIRouter, profile: CommuneProfile, bind) -> None:
    @router.post("/chat/{session_id}", response_model=ChatResponse)
    @bind
    def chat(
        session_id: str,
        req: ChatRequest,
        user_id: str = Depends(get_plu_user_id),
    ):
        """
        Tour de conversation dans une session existante.
        show_map=true si la session a des refs parcellaires ; le frontend charge le GeoJSON via GET /map.
        """
        t0 = time.monotonic()

        session = require_session_for_user(session_id, user_id)

        from ..mistral_client import (
            build_mistral_messages_from_db,
            resolve_provider,
        )

        provider = resolve_provider(req.provider, session_model=session.get("model"))
        model_name = MISTRAL_CHAT_MODEL if provider == "mistral" else GEMINI_MODEL

        zones = session.get("zones") or []
        messages = messages_get(session_id)

        try:
            from ..tools.utils.parcel_geom import refs_from_session, refs_from_user_text
        except ImportError:
            from tools.utils.parcel_geom import refs_from_session, refs_from_user_text

        from_text = refs_from_user_text(req.message)
        refs_kw = from_text or refs_from_session(session)
        if from_text:
            session_persist_refs(session_id, **from_text)
        parcel_identity = _resolve_parcel_identity(refs_kw)

        logger.info(
            f"session {session_id} — {len(messages)} messages — "
            f"provider={provider} — nouveau : {req.message!r}"
        )

        if provider == "mistral":
            contents = build_mistral_messages_from_db(
                messages,
                _build_system_prompt(
                    zones,
                    parcel_identity=parcel_identity,
                    response_mode=req.response_mode,
                ),
            )
            contents.append({"role": "user", "content": req.message})
        else:
            contents = build_contents_from_db(messages)
            contents.append(types.Content(role="user", parts=[types.Part(text=req.message)]))

        try:
            answer, tool_calls, usage, new_contents, raw_llm_context = run_turn(
                zones,
                contents,
                user_message=req.message,
                prior_messages=messages,
                provider=provider,
                parcel_identity=parcel_identity,
                response_mode=req.response_mode,
            )
        except Exception as e:
            logger.error(f"agentic_loop error : {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

        latency_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            f"session {session_id} — {latency_ms}ms | {provider} | "
            f"tools={[tc.name for tc in tool_calls]} | "
            f"tokens={usage.total_tokens} | cost=${usage.cost_usd}"
        )

        tool_calls_payload = [tc.model_dump() for tc in tool_calls]

        model_message_id = messages_insert(
            session_id=session_id,
            user_message=req.message,
            model_answer=answer,
            tool_calls=tool_calls_payload,
            gemini_parts=serialize_turn(provider, new_contents),
            prompt_tokens=usage.prompt_tokens,
            candidate_tokens=usage.candidate_tokens,
            total_tokens=usage.total_tokens,
            latency_ms=latency_ms,
            raw_llm_context=raw_llm_context,
        )

        session_persist_refs_from_tool_calls(session_id, tool_calls_payload)

        session = session_get(session_id) or session
        messages = messages_get(session_id)
        show_map = session_show_map(session, messages)

        resp = ChatResponse(
            session_id=session_id,
            answer=answer,
            tool_calls=tool_calls,
            usage=usage,
            latency_ms=latency_ms,
            model=model_name,
            provider=provider,
            model_message_id=model_message_id,
            map_data=None,
            show_map=show_map,
            **context_limit_fields(usage.context_tokens),
        )
        if not is_plu_superadmin(user_id):
            resp.usage = None
            resp.tool_calls = []
            resp.model_message_id = None
        return resp