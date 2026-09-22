"""
Capture et persistance du contexte brut LLM par tour (debug / audit).

Contenu sauvegardé (JSONB ``plu_messages.raw_llm_context``) :
  - prompt système assemblé (profil + zones préchargées)
  - historique user/model avant le tour
  - message utilisateur du tour
  - chaque appel tool : args, résultat brut, JSON renvoyé au modèle
  - réponse finale du modèle
  - ``gemini_rounds`` / ``gemini_usage_total`` : tokens facturés Gemini (usage_metadata)
  - ``llm_rounds`` / ``llm_usage_total`` / ``cost`` : tokens + USD Mistral/GLM
    (usage API si présent, sinon estimateur tiktoken)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg2

from ..llm_cost import merge_round_usages
from ..llm_token_usage import merge_token_usages, parse_usage_metadata

logger = logging.getLogger("plu_api")

_SCHEMAS_COLUMN_READY: set[str] = set()
_SCHEMAS_METRIQUES_READY: set[str] = set()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_raw_llm_context_column(conn, schema: str) -> None:
    if schema in _SCHEMAS_COLUMN_READY:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE "{schema}".plu_messages
            ADD COLUMN IF NOT EXISTS raw_llm_context JSONB
            """
        )
    _SCHEMAS_COLUMN_READY.add(schema)
    logger.info("Colonne raw_llm_context prête sur %s.plu_messages", schema)


def ensure_metriques_tour_column(conn, schema: str) -> None:
    if schema in _SCHEMAS_METRIQUES_READY:
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE "{schema}".plu_messages
            ADD COLUMN IF NOT EXISTS metriques_tour JSONB
            """
        )
    _SCHEMAS_METRIQUES_READY.add(schema)
    logger.info("Colonne metriques_tour prête sur %s.plu_messages", schema)


def metriques_depuis_raw_dict(
    raw: dict[str, Any] | None,
    *,
    latency_ms: int | None = None,
    prompt_tokens: int | None = None,
    candidate_tokens: int | None = None,
) -> dict[str, Any]:
    """Construit le JSONB compact lot 0.2 depuis un snapshot raw_llm_context."""
    raw = raw or {}
    rounds = raw.get("llm_rounds") or raw.get("gemini_rounds") or []
    invocations = raw.get("tool_invocations") or []
    usage = raw.get("llm_usage_total") or raw.get("gemini_usage_total") or {}
    cost = raw.get("cost") or {}
    chars = len(raw.get("system_instruction") or "")
    chars += len(raw.get("user_message") or "")
    for msg in raw.get("prior_messages") or []:
        if isinstance(msg, dict):
            chars += len(str(msg.get("content") or ""))
    for inv in invocations:
        if not isinstance(inv, dict):
            continue
        sent = inv.get("result_sent_to_llm")
        if isinstance(sent, str):
            chars += len(sent)
        elif sent is not None:
            chars += len(json.dumps(sent, ensure_ascii=False, default=str))
    tokens_in = usage.get("prompt_token_count")
    if tokens_in is None:
        tokens_in = prompt_tokens
    tokens_out = usage.get("candidates_token_count")
    if tokens_out is None:
        tokens_out = candidate_tokens
    total_tokens = usage.get("total_token_count")
    if total_tokens is None and tokens_in is not None and tokens_out is not None:
        total_tokens = int(tokens_in) + int(tokens_out)
    context_tokens = None
    if isinstance(rounds, list) and rounds:
        last = rounds[-1]
        if isinstance(last, dict):
            context_tokens = last.get("prompt_token_count")
    return {
        "version": 1,
        "nb_rounds": len(rounds) if isinstance(rounds, list) else None,
        "tool_calls": [
            inv.get("name")
            for inv in invocations
            if isinstance(inv, dict) and inv.get("name")
        ],
        "latence_ms": latency_ms,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "total_tokens": total_tokens,
        "context_tokens": context_tokens,
        "tokens_source": usage.get("tokens_source") or cost.get("tokens_source"),
        "cost_usd": cost.get("total_usd"),
        "input_usd": cost.get("input_usd"),
        "output_usd": cost.get("output_usd"),
        "caracteres_injectes": chars,
    }


@dataclass
class TurnRawContextCapture:
    """Accumule le contexte d'un tour agentique avant persistance."""

    system_instruction: str
    session_zones: list[dict]
    user_message: str
    prior_messages: list[dict] = field(default_factory=list)
    commune_slug: str | None = None
    model_name: str | None = None
    tool_invocations: list[dict[str, Any]] = field(default_factory=list)
    model_answer: str | None = None
    gemini_rounds: list[dict[str, Any]] = field(default_factory=list)
    llm_rounds: list[dict[str, Any]] = field(default_factory=list)

    def add_gemini_round(
        self,
        usage_metadata: Any,
        *,
        round_index: int,
        with_tool_calls: bool,
    ) -> None:
        tokens = parse_usage_metadata(usage_metadata)
        self.gemini_rounds.append(
            {
                "round": round_index,
                "with_tool_calls": with_tool_calls,
                **tokens,
            }
        )

    def add_llm_round(
        self,
        usage: dict[str, Any],
        *,
        round_index: int,
        with_tool_calls: bool,
        model: str | None = None,
    ) -> None:
        self.llm_rounds.append(
            {
                "round": round_index,
                "with_tool_calls": with_tool_calls,
                "model": model,
                **usage,
            }
        )

    def add_tool_invocation(
        self,
        *,
        name: str,
        args: dict,
        raw_result: dict | None,
        result_sent_to_llm: str,
        result_summary: str,
    ) -> None:
        self.tool_invocations.append(
            {
                "index": len(self.tool_invocations) + 1,
                "name": name,
                "args": args,
                "result_summary": result_summary,
                "result_raw": raw_result,
                "result_sent_to_llm": _safe_json_parse(result_sent_to_llm),
            }
        )

    def set_model_answer(self, text: str | None) -> None:
        self.model_answer = text

    def to_dict(self) -> dict[str, Any]:
        gemini_usage_total = merge_token_usages(*self.gemini_rounds)
        llm_usage_total = merge_round_usages(*self.llm_rounds) if self.llm_rounds else {}
        cost = None
        if llm_usage_total:
            cost = {
                "input_usd": llm_usage_total.get("input_usd"),
                "output_usd": llm_usage_total.get("output_usd"),
                "total_usd": llm_usage_total.get("total_usd"),
                "price_input_per_m": llm_usage_total.get("price_input_per_m"),
                "price_output_per_m": llm_usage_total.get("price_output_per_m"),
                "tokens_source": llm_usage_total.get("tokens_source"),
                "model": llm_usage_total.get("model") or self.model_name,
            }

        return {
            "version": 2,
            "captured_at": _utc_now_iso(),
            "commune": self.commune_slug,
            "model": self.model_name,
            "system_instruction": self.system_instruction,
            "prior_messages": self.prior_messages,
            "user_message": self.user_message,
            "tool_invocations": self.tool_invocations,
            "tool_count": len(self.tool_invocations),
            "model_answer": self.model_answer,
            "gemini_rounds": self.gemini_rounds,
            "gemini_usage_total": gemini_usage_total,
            "llm_rounds": self.llm_rounds,
            "llm_usage_total": llm_usage_total,
            "cost": cost,
        }

    def metriques_tour(
        self,
        *,
        latency_ms: int | None = None,
        prompt_tokens: int | None = None,
        candidate_tokens: int | None = None,
    ) -> dict[str, Any]:
        """JSONB compact (lot 0.2), interrogeable en SQL."""
        return metriques_depuis_raw_dict(
            self.to_dict(),
            latency_ms=latency_ms,
            prompt_tokens=prompt_tokens,
            candidate_tokens=candidate_tokens,
        )


def _safe_json_parse(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text


def prior_messages_for_capture(messages: list[dict]) -> list[dict]:
    """Historique léger (sans gemini_parts) pour le snapshot."""
    out: list[dict] = []
    for msg in messages:
        role = msg.get("role")
        content = (msg.get("content") or "").strip()
        if not content and role != "model":
            continue
        entry: dict[str, Any] = {
            "role": role,
            "content": content,
        }
        if msg.get("id"):
            entry["id"] = str(msg["id"])
        tc = msg.get("tool_calls")
        if tc:
            entry["tool_calls"] = [
                {
                    "name": t.get("name"),
                    "args": t.get("args"),
                    "result_summary": t.get("result_summary"),
                }
                for t in tc
                if isinstance(t, dict)
            ]
        out.append(entry)
    return out


def build_capture(
    *,
    system_instruction: str,
    session_zones: list[dict],
    user_message: str,
    prior_messages: list[dict],
    commune_slug: str | None = None,
    model_name: str | None = None,
) -> TurnRawContextCapture:
    return TurnRawContextCapture(
        system_instruction=system_instruction,
        session_zones=session_zones,
        user_message=user_message,
        prior_messages=prior_messages_for_capture(prior_messages),
        commune_slug=commune_slug,
        model_name=model_name,
    )
