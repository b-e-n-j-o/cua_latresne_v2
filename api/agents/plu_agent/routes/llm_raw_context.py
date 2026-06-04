"""
Capture et persistance du contexte brut LLM par tour (debug / audit).

Contenu sauvegardé (JSONB ``plu_messages.raw_llm_context``) :
  - prompt système assemblé (profil + zones préchargées)
  - historique user/model avant le tour
  - message utilisateur du tour
  - chaque appel tool : args, résultat brut, JSON renvoyé au modèle
  - réponse finale du modèle
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import psycopg2

logger = logging.getLogger("plu_api")

_SCHEMAS_COLUMN_READY: set[str] = set()


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
        return {
            "version": 1,
            "captured_at": _utc_now_iso(),
            "commune": self.commune_slug,
            "model": self.model_name,
            "system_instruction": self.system_instruction,
            "session_zones_preloaded": self.session_zones,
            "prior_messages": self.prior_messages,
            "user_message": self.user_message,
            "tool_invocations": self.tool_invocations,
            "tool_count": len(self.tool_invocations),
            "model_answer": self.model_answer,
        }


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
