# backend/utils/llm_mistral.py
# -*- coding: utf-8 -*-

import os
from typing import Optional
from dotenv import load_dotenv
from mistralai import Mistral

load_dotenv()

client = Mistral(api_key=os.getenv("MISTRAL_API_KEY"))


# ============================================================
# Appel générique Mistral (CAG / RAG / synthèse)
# ============================================================

def call_mistral(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_output_tokens: int = 800,
) -> str:
    """
    Appel LLM Mistral (ministral / mistral-medium)
    """

    response = client.chat.complete(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
        max_tokens=max_output_tokens,
    )

    return response.choices[0].message.content.strip()
