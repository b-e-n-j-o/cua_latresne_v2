#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_llm_gemini.py
------------------
Test minimal des appels Gemini.
"""

from llm_gemini import call_gemini


def test_gemini_flash_lite():
    print("\n=== TEST GEMINI 2.5 FLASH-LITE ===")

    result = call_gemini(
        model="gemini-2.5-flash-lite",
        system_prompt="Tu es un assistant de test.",
        user_prompt="Réponds STRICTEMENT par le mot OK.",
        temperature=0.0,
        max_output_tokens=32,
    )

    print("Réponse Gemini :", repr(result))


if __name__ == "__main__":
    test_gemini_flash_lite()
