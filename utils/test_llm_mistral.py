#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_llm_mistral.py
-------------------
Test minimal des appels LLM Mistral.
"""

from llm_mistral import call_mistral


def test_mistral_free():
    print("\n=== TEST MISTRAL FREE (ministral-14b) ===")

    result = call_mistral(
        model="ministral-14b-latest",
        system_prompt="Tu es un assistant de test.",
        user_prompt="comment vas tu aujourd'hui.",
        temperature=0.0,
        max_output_tokens=200,
    )

    print("Réponse Mistral Free :", repr(result))


def test_mistral_premium():
    print("\n=== TEST MISTRAL PREMIUM (mistral-medium) ===")

    result = call_mistral(
        model="mistral-medium-latest",
        system_prompt="Tu es un assistant de test.",
        user_prompt="Réponds STRICTEMENT par le mot OK.",
        temperature=0.2,
        max_output_tokens=32,
    )

    print("Réponse Mistral Premium :", repr(result))


if __name__ == "__main__":
    test_mistral_free()
    # test_mistral_premium()
