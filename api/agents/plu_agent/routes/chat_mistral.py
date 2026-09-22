"""
Boucle agentique Mistral (GLM 5.2) — parallèle à Gemini / Vertex.

Mêmes tools Python (``build_dispatch``), même prompt système, autre API.
"""

from __future__ import annotations

import functools
import logging

from .._env import (
    DB_CONFIG,
    MISTRAL_CHAT_MODEL,
    MISTRAL_EMBED_DIM,
    MISTRAL_EMBED_MODEL,
    MISTRAL_REASONING_EFFORT,
)
from ..commune_context import get_current_profile
from ..llm_cost import resolve_mistral_round_usage
from ..mistral_client import (
    gemini_decls_to_mistral_tools,
    mistral_chat_complete,
    mistral_configured,
    parse_tool_args,
    resolve_chat_model,
)
from .llm_raw_context import TurnRawContextCapture, build_capture
from .schemas import ToolCallLog, Usage

try:
    from ..tools import TOOL_DECLARATIONS_BY_NAME, build_dispatch
except ImportError:
    from tools import TOOL_DECLARATIONS_BY_NAME, build_dispatch

logger = logging.getLogger("plu_api")

MAX_MISTRAL_ROUNDS = 12


def _mistral_dispatch(tool_names: tuple[str, ...]) -> dict:
    dispatch = build_dispatch(DB_CONFIG, tool_names)
    if "search_articles_urbanisme" in dispatch:
        from ..tools.recherche_articles import search_articles_urbanisme

        dispatch["search_articles_urbanisme"] = functools.partial(
            search_articles_urbanisme,
            DB_CONFIG,
            embed_backend="mistral",
        )
    return dispatch


def _mistral_tools(tool_names: tuple[str, ...]) -> list[dict]:
    decls = [
        TOOL_DECLARATIONS_BY_NAME[n]
        for n in tool_names
        if n in TOOL_DECLARATIONS_BY_NAME
    ]
    return gemini_decls_to_mistral_tools(decls)


def _agentic_loop_mistral(
    messages: list[dict],
    dispatch: dict,
    tools: list[dict],
    *,
    model: str,
    capture: TurnRawContextCapture | None = None,
) -> tuple[str, list[ToolCallLog], Usage, list[dict]]:
    from .chat import _call_tool

    tool_calls_log: list[ToolCallLog] = []
    total_prompt = total_candidates = total_tokens = 0
    last_prompt = 0
    total_input_usd = total_output_usd = total_cost_usd = 0.0
    tokens_sources: list[str] = []
    price_in = price_out = None
    turn_messages: list[dict] = []

    for round_index in range(1, MAX_MISTRAL_ROUNDS + 1):
        result = mistral_chat_complete(
            messages,
            tools=tools,
            model=model,
            temperature=0.1,
            reasoning_effort=MISTRAL_REASONING_EFFORT,
        )
        round_usage = resolve_mistral_round_usage(
            api_usage=result.get("usage") or {},
            messages=messages,
            result=result,
            model=result.get("model") or model,
            tools=tools,
        )
        last_prompt = int(round_usage.get("prompt_token_count") or 0)
        total_prompt += last_prompt
        total_candidates += int(round_usage.get("candidates_token_count") or 0)
        total_tokens += int(round_usage.get("total_token_count") or 0)
        total_input_usd += float(round_usage.get("input_usd") or 0)
        total_output_usd += float(round_usage.get("output_usd") or 0)
        total_cost_usd += float(round_usage.get("total_usd") or 0)
        if round_usage.get("tokens_source"):
            tokens_sources.append(str(round_usage["tokens_source"]))
        if round_usage.get("price_input_per_m") is not None:
            price_in = round_usage.get("price_input_per_m")
        if round_usage.get("price_output_per_m") is not None:
            price_out = round_usage.get("price_output_per_m")

        raw_message = result.get("raw_message") or {}
        assistant_msg = {
            "role": "assistant",
            "content": raw_message.get("content")
            if raw_message.get("content") is not None
            else result.get("content") or None,
        }
        tool_calls = result.get("tool_calls") or []
        if tool_calls:
            assistant_msg["tool_calls"] = tool_calls

        messages.append(assistant_msg)
        turn_messages.append(assistant_msg)

        if capture is not None:
            capture.add_llm_round(
                round_usage,
                round_index=round_index,
                with_tool_calls=bool(tool_calls),
                model=result.get("model") or model,
            )

        if not tool_calls:
            source = (
                "mixed"
                if tokens_sources and len(set(tokens_sources)) > 1
                else (tokens_sources[0] if tokens_sources else "estimate")
            )
            usage_out = Usage(
                prompt_tokens=total_prompt or None,
                candidate_tokens=total_candidates or None,
                total_tokens=total_tokens or None,
                context_tokens=last_prompt or None,
                tokens_source=source,
                input_usd=round(total_input_usd, 6) or None,
                output_usd=round(total_output_usd, 6) or None,
                cost_usd=round(total_cost_usd, 6) or None,
                price_input_per_m=price_in,
                price_output_per_m=price_out,
            )
            answer = result.get("content") or ""
            if capture is not None:
                capture.set_model_answer(answer)
            return answer, tool_calls_log, usage_out, turn_messages

        for tc in tool_calls:
            fn = (tc.get("function") or {}) if isinstance(tc, dict) else {}
            name = fn.get("name") or ""
            args = parse_tool_args(fn.get("arguments"))
            tc_id = tc.get("id") if isinstance(tc, dict) else None
            logger.info("tool_call [mistral] → %s(%s)", name, args)
            result_str, summary, raw_result = _call_tool(
                dispatch, name, args, capture=capture
            )
            logger.info("  ↳ %s", summary)
            tool_calls_log.append(
                ToolCallLog(
                    name=name,
                    args=args,
                    result_summary=summary,
                    raw_result=raw_result,
                )
            )
            tool_msg = {
                "role": "tool",
                "name": name,
                "content": result_str,
            }
            if tc_id:
                tool_msg["tool_call_id"] = tc_id
            messages.append(tool_msg)
            turn_messages.append(tool_msg)

    raise RuntimeError(
        f"Boucle Mistral : plus de {MAX_MISTRAL_ROUNDS} tours tool-calling."
    )


def run_turn_mistral(
    zones: list[dict],
    messages: list[dict],
    *,
    user_message: str = "",
    prior_messages: list[dict] | None = None,
    parcel_identity: dict | None = None,
    response_mode: str | None = None,
) -> tuple[str, list[ToolCallLog], Usage, list[dict], dict]:
    """Même contrat que ``run_turn`` Gemini ; ``new_contents`` = messages du tour."""
    if not mistral_configured():
        raise RuntimeError("MISTRAL_API_KEY absente — impossible d'utiliser la stack Mistral.")

    from .chat import _build_system_prompt

    profile = get_current_profile()
    tool_names = profile.llm_tool_names
    system_instruction = _build_system_prompt(
        zones, parcel_identity=parcel_identity, response_mode=response_mode
    )
    if not messages or messages[0].get("role") != "system":
        messages = [{"role": "system", "content": system_instruction}, *messages]
    else:
        messages[0]["content"] = system_instruction

    dispatch = _mistral_dispatch(tool_names)
    tools = _mistral_tools(tool_names)
    capture = build_capture(
        system_instruction=system_instruction,
        session_zones=zones,
        user_message=user_message,
        prior_messages=prior_messages or [],
        commune_slug=profile.slug,
        model_name=resolve_chat_model(),
    )
    start = len(messages)
    answer, tool_calls, usage, _turn = _agentic_loop_mistral(
        messages, dispatch, tools, model=resolve_chat_model(), capture=capture
    )
    raw = capture.to_dict()
    raw["provider"] = "mistral"
    raw["stack"] = "mistral"
    return answer, tool_calls, usage, messages[start:], raw


def probe_mistral() -> dict:
    """Ping generate (GLM 5.2) + embed (mistral-embed)."""
    import time

    from .chat import LLM_PROBE_PROMPT, classify_llm_error

    generate: dict
    embedding: dict
    t0 = time.monotonic()
    try:
        result = mistral_chat_complete(
            [{"role": "user", "content": LLM_PROBE_PROMPT}],
            model=MISTRAL_CHAT_MODEL,
            temperature=0,
            reasoning_effort="none",
        )
        text = (result.get("content") or "").strip()
        generate = {
            "ok": True,
            "model": result.get("model") or MISTRAL_CHAT_MODEL,
            "answer": text,
            "latency_ms": int((time.monotonic() - t0) * 1000),
        }
    except Exception as e:
        generate = {
            "ok": False,
            "model": MISTRAL_CHAT_MODEL,
            "reason": classify_llm_error(e),
            "error": str(e),
            "latency_ms": int((time.monotonic() - t0) * 1000),
        }

    t1 = time.monotonic()
    try:
        from ..mistral_client import embed_query_mistral

        vec = embed_query_mistral("ping urbanisme")
        dim = len(vec)
        embedding = {
            "ok": dim > 0,
            "model": MISTRAL_EMBED_MODEL,
            "stack": "mistral",
            "dim": dim,
            "expected_dim": MISTRAL_EMBED_DIM,
            "latency_ms": int((time.monotonic() - t1) * 1000),
        }
        if dim != MISTRAL_EMBED_DIM:
            embedding["ok"] = False
            embedding["reason"] = "empty"
            embedding["error"] = f"Embedding inattendu : {dim}d (attendu {MISTRAL_EMBED_DIM}d)"
    except Exception as e:
        embedding = {
            "ok": False,
            "model": MISTRAL_EMBED_MODEL,
            "stack": "mistral",
            "reason": classify_llm_error(e),
            "error": str(e),
            "latency_ms": int((time.monotonic() - t1) * 1000),
        }

    failed = next((p for p in (generate, embedding) if not p.get("ok")), None)
    commune = ""
    try:
        commune = get_current_profile().slug
    except Exception:
        commune = ""
    return {
        "ok": failed is None,
        "stack": "mistral",
        "commune": commune,
        "model": generate.get("model") or MISTRAL_CHAT_MODEL,
        "answer": generate.get("answer"),
        "generate": generate,
        "embedding": embedding,
        "reason": None if failed is None else failed.get("reason"),
        "error": None if failed is None else failed.get("error"),
        "latency_ms": (generate.get("latency_ms") or 0) + (embedding.get("latency_ms") or 0),
    }


__all__ = [
    "run_turn_mistral",
    "probe_mistral",
]
