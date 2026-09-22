# -*- coding: utf-8 -*-
"""Estimateur tokens + tarif GLM 5.2 (1,40 $ in / 4,40 $ out)."""

from __future__ import annotations

from api.agents.plu_agent.llm_cost import (
    cost_usd,
    estimate_tokens,
    prix_modele,
    resolve_mistral_round_usage,
)
from api.agents.plu_agent.routes.llm_raw_context import metriques_depuis_raw_dict


def test_prix_glm() -> None:
    assert prix_modele("zai-glm-5-2") == (1.40, 4.40)


def test_cost_usd_glm_un_million() -> None:
    cost = cost_usd("zai-glm-5-2", 1_000_000, 1_000_000)
    assert cost["input_usd"] == 1.4
    assert cost["output_usd"] == 4.4
    assert cost["total_usd"] == 5.8


def test_cost_usd_petit_tour() -> None:
    cost = cost_usd("zai-glm-5-2", 10_000, 2_000)
    assert cost["input_usd"] == 0.014
    assert cost["output_usd"] == 0.0088
    assert cost["total_usd"] == 0.0228


def test_estimate_tokens_non_vide() -> None:
    n = estimate_tokens("parcelle AL 74 zone agricole")
    assert n > 0
    assert estimate_tokens("") == 0
    assert estimate_tokens(None) == 0


def test_resolve_prefers_api_usage() -> None:
    out = resolve_mistral_round_usage(
        api_usage={"prompt_tokens": 1200, "completion_tokens": 80, "total_tokens": 1280},
        messages=[{"role": "user", "content": "infos AL 74"}],
        result={"content": "ok", "tool_calls": []},
        model="zai-glm-5-2",
    )
    assert out["tokens_source"] == "api"
    assert out["prompt_token_count"] == 1200
    assert out["candidates_token_count"] == 80
    assert out["total_token_count"] == 1280
    assert out["total_usd"] == cost_usd("zai-glm-5-2", 1200, 80)["total_usd"]


def test_resolve_estime_si_usage_api_vide() -> None:
    messages = [
        {"role": "system", "content": "Expert PLU."},
        {"role": "user", "content": "Quelles règles pour la parcelle AL 74 ?"},
    ]
    result = {
        "content": "La parcelle est en zone Ap.",
        "raw_message": {"content": "La parcelle est en zone Ap."},
        "tool_calls": [],
    }
    out = resolve_mistral_round_usage(
        api_usage={},
        messages=messages,
        result=result,
        model="zai-glm-5-2",
        tools=[{"type": "function", "function": {"name": "get_contexte_parcelle"}}],
    )
    assert out["tokens_source"] == "estimate"
    assert out["prompt_token_count"] > 0
    assert out["candidates_token_count"] > 0
    assert out["total_usd"] > 0
    assert out["price_input_per_m"] == 1.4
    assert out["price_output_per_m"] == 4.4


def test_metriques_incluent_le_cout() -> None:
    raw = {
        "system_instruction": "sys",
        "user_message": "AL 74",
        "prior_messages": [],
        "tool_invocations": [],
        "llm_rounds": [{"round": 1}],
        "llm_usage_total": {
            "prompt_token_count": 1000,
            "candidates_token_count": 200,
            "total_token_count": 1200,
            "tokens_source": "estimate",
        },
        "cost": {
            "input_usd": 0.0014,
            "output_usd": 0.00088,
            "total_usd": 0.00228,
            "tokens_source": "estimate",
        },
    }
    m = metriques_depuis_raw_dict(raw, latency_ms=1234)
    assert m["tokens_in"] == 1000
    assert m["tokens_out"] == 200
    assert m["total_tokens"] == 1200
    assert m["cost_usd"] == 0.00228
    assert m["tokens_source"] == "estimate"
    assert m["latence_ms"] == 1234
