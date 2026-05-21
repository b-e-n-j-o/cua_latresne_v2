#!/usr/bin/env python3
"""
plu_agent.py
------------
Agent conversationnel Gemini pour l'analyse PLU d'Argelès-sur-Mer.
Les tools sont définis dans tools.py.

Usage:
    python plu_agent.py
    python plu_agent.py --section AC --numero 8770
    python plu_agent.py --question "La parcelle AC 8770 est-elle constructible ?"
"""

import os
import json
import argparse
from dotenv import load_dotenv
from google import genai
from google.genai import types

from tools import TOOL_DECLARATIONS, build_dispatch

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

DB_CONFIG = {
    "host":     os.environ["SUPABASE_HOST"],
    "port":     int(os.environ.get("SUPABASE_PORT", 5432)),
    "dbname":   os.environ["SUPABASE_DB"],
    "user":     os.environ["SUPABASE_USER"],
    "password": os.environ["SUPABASE_PASSWORD"],
    "sslmode":  "require",
    "connect_timeout": 15,
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
GEMINI_MODEL   = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite")

SYSTEM_PROMPT = """
Tu es un expert en droit de l'urbanisme français, spécialisé dans l'analyse des PLU.
Tu as accès au règlement PLU de la commune d'Argelès-sur-Mer (INSEE 66008).

Workflow pour une question sur une parcelle :
1. Appelle get_contexte_parcelle (zonage + prescriptions) avec section+numero, idu,
   ou parcelles[] / idus[] pour une unité foncière contiguë.
2. La carte est gérée par l'interface, pas par un tool LLM.

Règles de réponse :
- Cite toujours les zones concernées et leurs pourcentages de couverture
- Appuie-toi sur les articles du règlement pour justifier tes conclusions
- Traite chaque zone séparément si plusieurs zones sont concernées
- Signale si une zone est trouvée mais sans règlement disponible
- Utilise EXACTEMENT les codes de zone retournés par les tools, sans les modifier
""".strip()

# ---------------------------------------------------------------------------
# Boucle agentique
# ---------------------------------------------------------------------------

def call_tool(dispatch: dict, name: str, args: dict) -> str:
    fn = dispatch.get(name)
    if fn is None:
        return json.dumps({"error": f"Tool inconnu : {name}"})
    result = fn(**args)
    return json.dumps(result, ensure_ascii=False, default=str)


def agentic_loop(
    client: genai.Client,
    dispatch: dict,
    contents: list,
    config: types.GenerateContentConfig,
    verbose: bool = True,
) -> str:
    while True:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=contents,
            config=config,
        )

        candidate = response.candidates[0]
        contents.append(candidate.content)

        function_calls = [
            part.function_call
            for part in candidate.content.parts
            if part.function_call is not None
        ]

        if not function_calls:
            return response.text

        tool_response_parts = []
        for fc in function_calls:
            if verbose:
                print(f"  → tool_call : {fc.name}({dict(fc.args)})")

            result_str = call_tool(dispatch, fc.name, dict(fc.args))

            if verbose:
                try:
                    parsed = json.loads(result_str)
                    if "zones" in parsed:
                        summary = [
                            f"{z.get('code_zone')} ({z.get('pct_parcelle_couverte', '?')}%)"
                            for z in parsed.get("zones", [])
                        ]
                        print(f"     ↳ zones : {summary}")
                    elif "parcelle" in parsed and parsed["parcelle"]:
                        p = parsed["parcelle"]
                        print(f"     ↳ parcelle : {p.get('idu')} — {p.get('superficie_m2', 0):.0f} m²")
                    elif "error" in parsed and parsed["error"]:
                        print(f"     ↳ erreur : {parsed['error']}")
                except Exception:
                    pass

            tool_response_parts.append(
                types.Part.from_function_response(
                    name=fc.name,
                    response={"result": result_str},
                )
            )

        contents.append(types.Content(role="user", parts=tool_response_parts))


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def build_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(api_key=GEMINI_API_KEY)
    return genai.Client()


def build_config() -> types.GenerateContentConfig:
    return types.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT,
        tools=[TOOL_DECLARATIONS],
        temperature=0.1,
    )


def make_seed_message(section: str = None, numero: str = None) -> str:
    if section and numero:
        return (
            f"Je travaille sur la parcelle cadastrale section {section.upper()} numéro {numero}. "
            "Quelles sont les zones PLU concernées, leurs pourcentages de couverture, "
            "et quels sont leurs grands principes réglementaires ?"
        )
    return None


def run_chat(
    section: str = None,
    numero: str = None,
    verbose: bool = True,
) -> None:
    client   = build_client()
    config   = build_config()
    dispatch = build_dispatch(DB_CONFIG)
    contents = []

    print("\n🗺️  Agent PLU — Argelès-sur-Mer")
    print("   Posez vos questions sur le règlement PLU (Ctrl+C pour quitter)\n")

    seed = make_seed_message(section=section, numero=numero)
    if seed:
        print(f"[Amorce]\n{seed}\n")
        contents.append(types.Content(role="user", parts=[types.Part(text=seed)]))
        answer = agentic_loop(client, dispatch, contents, config, verbose=verbose)
        print(f"Assistant :\n{answer}\n")

    while True:
        try:
            user_input = input("Vous : ").strip()
        except (KeyboardInterrupt, EOFError):
            print("\nFin de session.")
            break

        if not user_input or user_input.lower() in ("exit", "quit", "q"):
            print("Fin de session.")
            break

        contents.append(types.Content(role="user", parts=[types.Part(text=user_input)]))
        answer = agentic_loop(client, dispatch, contents, config, verbose=verbose)
        print(f"\nAssistant :\n{answer}\n")


def run_agent(question: str, verbose: bool = True) -> str:
    client   = build_client()
    config   = build_config()
    dispatch = build_dispatch(DB_CONFIG)
    contents = [types.Content(role="user", parts=[types.Part(text=question)])]

    if verbose:
        print(f"\n{'='*60}\nQuestion : {question}\n{'='*60}\n")

    answer = agentic_loop(client, dispatch, contents, config, verbose=verbose)

    if verbose:
        print(f"\nRéponse :\n{answer}\n")

    return answer


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Agent PLU Argelès-sur-Mer")
    parser.add_argument("--section",    default=None, help="Section cadastrale (ex: AC)")
    parser.add_argument("--numero",     default=None, help="Numéro de parcelle (ex: 8770)")
    parser.add_argument("--question",   default=None, help="Question unique (non-interactif)")
    parser.add_argument("--no-verbose", action="store_true")
    args = parser.parse_args()

    verbose = not args.no_verbose

    if args.question:
        q = args.question
        if args.section and args.numero:
            q = f"Parcelle section {args.section} numéro {args.numero}. {q}"
        run_agent(q, verbose=verbose)
    else:
        run_chat(
            section=args.section,
            numero=args.numero,
            verbose=verbose,
        )


if __name__ == "__main__":
    main()