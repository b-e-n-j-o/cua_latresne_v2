#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
test_llm_openai.py
------------------
Test minimal des fonctions call_gpt_4 et call_gpt_5.
"""

from llm_openai import call_gpt_4, call_gpt_5


def test_gpt4():
    print("\n=== TEST GPT-4.x ===")

    result = call_gpt_4(
        model="gpt-4.1-nano",
        system_prompt="Tu es un assistant de test.",
        user_prompt="Réponds simplement par le mot OK.",
        temperature=0.0,
        max_output_tokens=20,
    )

    print("Réponse GPT-4 :", result)


def test_gpt5():
    print("\n=== TEST GPT-5.x ===")

    result = call_gpt_5(
        model="gpt-5-mini",
        system_prompt="Tu es un assistant de test.",
        user_prompt="Comment vas tu aujourd'hui ?",
        reasoning_effort="low",
        max_output_tokens=200,
    )

    print("Réponse GPT-5 :", result)


if __name__ == "__main__":
    test_gpt4()
    test_gpt5()
