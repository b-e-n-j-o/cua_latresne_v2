"""
Résumé réglementaire (« laïus ») à partir de Markdown de règlement complet.
Prompt : prompts/laius.txt
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from google.genai import types

from .shared import (
    compute_cost,
    get_client,
    log_gemini_tokens,
    parse_usage_metadata,
    validate_gemini_model,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
LAIUS_PROMPT_PATH = ROOT / "prompts" / "laius.txt"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strip_code_fences(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def parse_laius_prompt(path: Path = LAIUS_PROMPT_PATH) -> tuple[str, str]:
    """Découpe le prompt en instructions système et gabarit utilisateur."""
    raw = path.read_text(encoding="utf-8")
    if "[SYSTEM]" not in raw or "[USER]" not in raw:
        raise ValueError(f"Format prompt invalide (attendu [SYSTEM] / [USER]) : {path}")

    _, rest = raw.split("[SYSTEM]", 1)
    system_part, user_part = rest.split("[USER]", 1)
    return system_part.strip(), user_part.strip()


def build_laius_user_message(
    user_template: str,
    *,
    markdown_text: str,
    label_zone: str,
) -> str:
    """Remplace les métadonnées et injecte le texte réglementaire."""
    msg = re.sub(
        r'label_zone:\s*\{[^}]+\}',
        lambda _m: f'label_zone: {label_zone}',
        user_template,
        count=1,
    )
    # Pas de re.sub avec le texte en replacement : les règlements contiennent souvent
    # des backslashes (\l, \c, etc.) que re interprète comme séquences d'échappement.
    needle = '"""\n{texte_markdown_de_la_zone}\n"""'
    injected = f'"""\n{markdown_text.strip()}\n"""'
    if needle not in msg:
        raise ValueError("Placeholder {texte_markdown_de_la_zone} introuvable dans le prompt")
    return msg.replace(needle, injected, 1)


def summarize_to_laius(
    markdown_text: str,
    *,
    model: str | None = None,
    log_context: str | None = None,
    label_zone: str,
    prompt_path: Path = LAIUS_PROMPT_PATH,
) -> tuple[str, dict]:
    """Appelle Gemini pour produire un laïus Markdown à partir d'un règlement long."""
    model_id = validate_gemini_model(model)
    system_instruction, user_template = parse_laius_prompt(prompt_path)
    user_message = build_laius_user_message(
        user_template,
        markdown_text=markdown_text,
        label_zone=label_zone,
    )

    t0 = time.perf_counter()
    response = get_client().models.generate_content(
        model=model_id,
        contents=user_message,
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            temperature=0.1,
            max_output_tokens=8192,
        ),
    )
    elapsed_sec = time.perf_counter() - t0

    raw_out = response.text or ""
    laius_md = _strip_code_fences(raw_out)

    tokens = parse_usage_metadata(response.usage_metadata)
    ctx = log_context or label_zone
    log_gemini_tokens(logger, context=ctx, phase="laius", tokens=tokens)

    input_tok = tokens["prompt_token_count"]
    output_tok = tokens["candidates_token_count"]
    thinking_tok = tokens["thoughts_token_count"]
    cached_tok = tokens["cached_content_token_count"]
    billable_output = output_tok + thinking_tok
    cost = compute_cost(input_tok, billable_output, cached_tok)

    stats = {
        "input": input_tok,
        "output_visible": output_tok,
        "thinking": thinking_tok,
        "cached": cached_tok,
        "tokens": tokens,
        "ratio_chars": len(laius_md) / max(len(markdown_text), 1),
        "elapsed_sec": elapsed_sec,
        **cost,
    }
    return laius_md, stats


def process_md_content(
    output_basename: str,
    label_zone: str,
    md_text: str,
    outputs_dir: Path,
    *,
    model: str | None = None,
) -> dict:
    """
    Produit un laïus Markdown nommé comme le fichier source
    (``outputs/<basename>``).
    """
    outputs_dir.mkdir(parents=True, exist_ok=True)

    model_id = validate_gemini_model(model)
    t_zone_start = time.perf_counter()

    try:
        laius_md, stats = summarize_to_laius(
            md_text,
            model=model_id,
            log_context=output_basename,
            label_zone=label_zone,
        )
    except Exception as e:
        logger.exception("Laius échoué pour %s", output_basename)
        return {
            "zone": output_basename,
            "status": "laius_failed",
            "error": str(e),
        }

    if not laius_md.strip():
        return {
            "zone": output_basename,
            "status": "laius_failed",
            "error": "Réponse vide du modèle",
        }

    out_path = outputs_dir / output_basename
    out_path.write_text(laius_md, encoding="utf-8")

    total_zone_sec = time.perf_counter() - t_zone_start
    tokens = {
        "extract": stats["tokens"],
        "judge": None,
        "total": stats["tokens"],
    }
    log_gemini_tokens(logger, context=output_basename, phase="total_fichier", tokens=tokens.get("total"))

    return {
        "zone": output_basename,
        "status": "done",
        "verdict": "laius",
        "routed_to": "outputs",
        "total_cost_usd": round(stats.get("total_usd", 0.0), 4),
        "duration_s": round(total_zone_sec, 2),
        "tokens": tokens,
        "timing": {
            "laius_llm_sec": stats.get("elapsed_sec", 0.0),
            "total_zone_sec": round(total_zone_sec, 2),
        },
        "finished_at": _utc_now_iso(),
    }
