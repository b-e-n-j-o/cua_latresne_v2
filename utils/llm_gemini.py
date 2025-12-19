# backend/utils/llm_gemini.py
# -*- coding: utf-8 -*-

import os
from typing import Optional
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))


def call_gemini(
    *,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    max_output_tokens: int = 800,
) -> str:
    """
    Appel LLM Gemini (flash-lite / flash / pro)
    Compatible CAG / RAG / synthèse.
    """

    generation_config = genai.types.GenerationConfig(
        temperature=temperature,
        max_output_tokens=max_output_tokens,
    )

    llm = genai.GenerativeModel(
        model_name=model,
        system_instruction=system_prompt,
        generation_config=generation_config,
    )

    response = llm.generate_content(user_prompt)

    return (response.text or "").strip()
