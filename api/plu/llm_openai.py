# backend/utils/llm_openai.py
# -*- coding: utf-8 -*-

import os
import json
from typing import Optional, Literal, Dict, Any
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

ReasoningEffort = Literal["low", "medium", "high"]


# ============================================================
# GPT-4.x (Responses API, temperature-based)
# ============================================================

def call_gpt_4(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_output_tokens: Optional[int] = 800,
) -> str:
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
        max_output_tokens=max_output_tokens,
    )

    return response.output_text.strip()


# ============================================================
# GPT-5.x (reasoning-based, free text)
# ============================================================

def call_gpt_5(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    reasoning_effort: ReasoningEffort = "medium",
    max_output_tokens: Optional[int] = 800,
) -> str:
    response = client.responses.create(
        model=model,
        reasoning={"effort": reasoning_effort},
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=max_output_tokens,
    )

    return response.output_text.strip()


# ============================================================
# GPT-5.x (JSON STRICT via schema)
# ============================================================

def call_gpt5_json(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    json_schema: Dict[str, Any],
    reasoning_effort: ReasoningEffort = "medium",
    max_output_tokens: Optional[int] = 1200,
) -> Dict[str, Any]:
    """
    GPT-5 – Structured Outputs (JSON strict)
    Le schéma est fourni via text_format (API officielle).
    """
    response = client.responses.parse(
        model=model,
        reasoning={"effort": reasoning_effort},
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        text_format=json_schema,
        max_output_tokens=max_output_tokens,
    )

    if response.output_parsed is None:
        raise RuntimeError(
            "GPT-5 n’a pas pu produire une sortie conforme au schéma JSON."
        )

    return response.output_parsed



def call_gpt5_raw(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    reasoning_effort: str = "medium",
    max_output_tokens: Optional[int] = 800,
) -> Dict[str, Any]:
    """
    Appel GPT-5 qui retourne la réponse OpenAI brute (dict),
    sans extraction de output_text.
    """

    response = client.responses.create(
        model=model,
        reasoning={"effort": reasoning_effort},
        input=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_output_tokens=max_output_tokens,
    )

    # Retourner l'objet tel quel (sérialisé)
    return response.model_dump()
