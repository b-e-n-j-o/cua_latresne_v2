import json
import sys
import time
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field, ValidationError
from google.genai import types

from .shared import MODEL, compute_cost, get_client, parse_usage_metadata

ROOT = Path(__file__).resolve().parent

# Rapports d'audit JSON volumineux (nombreuses issues) : 8000 tokens coupe souvent la réponse au milieu du JSON.
MAX_OUTPUT_TOKENS = 32_768

# Erreurs API souvent transitoires (charge / disponibilité)
AUDIT_MAX_ATTEMPTS = 6
AUDIT_RETRY_BASE_DELAY_SEC = 8.0
AUDIT_RETRY_MAX_DELAY_SEC = 120.0


def _retryable_audit_error(exc: BaseException) -> bool:
    """True si l'échec peut mériter un nouvel essai (503, surcharge, limite temporaire)."""
    text = str(exc).upper()
    if any(x in text for x in ("503", "UNAVAILABLE", "429", "RESOURCE_EXHAUSTED", "RATE LIMIT", "HIGH DEMAND")):
        return True
    if any(x in text for x in ("502", "504", "DEADLINE", "TIMEOUT", "TEMPORARILY")):
        return True
    for attr in ("status_code", "code", "http_status"):
        v = getattr(exc, attr, None)
        if v is None:
            continue
        try:
            if int(v) in (429, 502, 503, 504):
                return True
        except (TypeError, ValueError):
            continue
    return False


# --- Schéma de sortie ---

class Issue(BaseModel):
    severity: Literal["high", "medium", "low"]
    category: Literal["omission", "addition", "alteration", "displacement"]
    description: str = Field(description="Description précise de la divergence")
    source_excerpt: str | None = Field(
        default=None,
        description="Extrait du texte source concerné (1-2 phrases). Null si addition pure."
    )
    md_excerpt: str | None = Field(
        default=None,
        description="Extrait du Markdown concerné (1-2 phrases). Null si omission pure."
    )

class AuditReport(BaseModel):
    verdict: Literal["ok", "warning", "critical"]
    summary: str = Field(description="Synthèse en 2-3 phrases du résultat de l'audit.")
    issues: list[Issue] = Field(default_factory=list)


def _generate_audit_with_retries(full_prompt: str) -> tuple[object, float]:
    """Appelle ``generate_content`` avec backoff sur erreurs transitoires. Retourne (response, elapsed_sec)."""
    last: BaseException | None = None
    for attempt in range(AUDIT_MAX_ATTEMPTS):
        t0 = time.perf_counter()
        try:
            response = get_client().models.generate_content(
                model=MODEL,
                contents=full_prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    max_output_tokens=MAX_OUTPUT_TOKENS,
                    response_mime_type="application/json",
                    response_schema=AuditReport,
                ),
            )
            elapsed_sec = time.perf_counter() - t0
            return response, elapsed_sec
        except Exception as e:
            last = e
            if attempt + 1 >= AUDIT_MAX_ATTEMPTS or not _retryable_audit_error(e):
                raise
            delay = min(
                AUDIT_RETRY_BASE_DELAY_SEC * (2**attempt),
                AUDIT_RETRY_MAX_DELAY_SEC,
            )
            print(
                f"⚠️  Audit LLM — erreur transitoire (tentative {attempt + 1}/{AUDIT_MAX_ATTEMPTS}) : "
                f"{e!s}\n   Nouvel essai dans {delay:.0f}s…",
                flush=True,
            )
            time.sleep(delay)
    assert last is not None
    raise last


# --- Appel au juge ---

def audit(
    raw_clean: str,
    md: str,
    prompt_path: str | Path | None = None,
) -> tuple[AuditReport, dict]:
    path = Path(prompt_path) if prompt_path is not None else ROOT / "prompts" / "judge.txt"
    if not path.is_file():
        path = ROOT / path
    prompt_template = path.read_text(encoding="utf-8")
    full_prompt = prompt_template.format(raw=raw_clean, md=md)

    response, elapsed_sec = _generate_audit_with_retries(full_prompt)

    raw_text = (response.text or "").strip()
    finish = None
    if getattr(response, "candidates", None):
        c0 = response.candidates[0]
        finish = getattr(c0, "finish_reason", None)

    try:
        report = AuditReport.model_validate_json(raw_text)
    except ValidationError as e:
        finish_s = str(finish) if finish is not None else ""
        hint = ""
        if "MAX_TOKENS" in finish_s or "LENGTH" in finish_s:
            hint = " La réponse semble avoir atteint la limite de sortie (finish_reason)."
        raise RuntimeError(
            f"JSON d'audit invalide ou tronqué ({len(raw_text)} car.).{hint}"
            " Augmente MAX_OUTPUT_TOKENS dans judge.py ou réduis la taille du prompt / des documents."
        ) from e

    tokens = parse_usage_metadata(response.usage_metadata)
    input_tok = tokens["prompt_token_count"]
    output_tok = tokens["candidates_token_count"]
    thinking_tok = tokens["thoughts_token_count"]
    cached_tok = tokens["cached_content_token_count"]
    billable_output = output_tok + thinking_tok

    cost = compute_cost(input_tok, billable_output, cached_tok)

    stats = {
        "input_tokens": input_tok,
        "output_tokens": output_tok,
        "thinking_tokens": thinking_tok,
        "cached_tokens": cached_tok,
        "tokens": tokens,
        "elapsed_sec": elapsed_sec,
        "cost_usd": cost["total_usd"],
        **cost,
    }
    return report, stats


# --- Compteurs utilitaires ---

def summarize_report(report: AuditReport) -> dict:
    counts = {"high": 0, "medium": 0, "low": 0}
    by_category = {"omission": 0, "addition": 0, "alteration": 0, "displacement": 0}
    for issue in report.issues:
        counts[issue.severity] += 1
        by_category[issue.category] += 1
    return {
        "verdict": report.verdict,
        "total_issues": len(report.issues),
        "by_severity": counts,
        "by_category": by_category,
    }


def print_report(report: AuditReport):
    icon = {"ok": "✅", "warning": "⚠️ ", "critical": "🔴"}[report.verdict]
    print(f"\n{icon} VERDICT : {report.verdict.upper()}")
    print(f"📝 {report.summary}\n")

    if not report.issues:
        print("Aucune divergence détectée.")
        return

    by_severity = {"high": [], "medium": [], "low": []}
    for issue in report.issues:
        by_severity[issue.severity].append(issue)

    for sev in ["high", "medium", "low"]:
        if not by_severity[sev]:
            continue
        sev_icon = {"high": "🔴", "medium": "🟡", "low": "🔵"}[sev]
        print(f"\n{sev_icon} {sev.upper()} ({len(by_severity[sev])})")
        print("─" * 60)
        for i, issue in enumerate(by_severity[sev], 1):
            print(f"\n  [{i}] [{issue.category}] {issue.description}")
            if issue.source_excerpt:
                print(f"      📄 Source : « {issue.source_excerpt[:200]} »")
            if issue.md_excerpt:
                print(f"      📝 MD    : « {issue.md_excerpt[:200]} »")


# --- Usage en standalone ---

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage : python judge.py <raw.txt> <output.md> [report.json] [prompt_judge.txt]\n"
            f"  Prompt juge par défaut : {ROOT / 'prompts' / 'judge.txt'}"
        )
        sys.exit(1)

    raw_path = Path(sys.argv[1])
    md_path = Path(sys.argv[2])
    report_path = Path(sys.argv[3]) if len(sys.argv) > 3 else md_path.with_suffix(".audit.json")
    judge_prompt: Path | None = Path(sys.argv[4]) if len(sys.argv) > 4 else None

    raw = raw_path.read_text(encoding="utf-8")
    md = md_path.read_text(encoding="utf-8")

    print(f"🔍 Audit en cours : {md_path.name} vs {raw_path.name}")
    report, stats = audit(raw, md, prompt_path=judge_prompt)

    print(
        f"\n📊 Tokens audit : in={stats['input_tokens']}  out={stats['output_tokens']}  "
        f"thinking={stats['thinking_tokens']}  cached={stats['cached_tokens']}"
    )
    print(
        f"💰 Coût audit   : input ${stats['cost_input_usd']:.4f}  output ${stats['cost_output_usd']:.4f}  "
        f"cache ${stats['cost_cache_usd']:.4f}  total ${stats['total_usd']:.4f} "
        f"(~{stats['total_eur_approx'] * 100:.2f}¢ EUR)"
    )
    print(f"⏱️  Durée LLM : {stats['elapsed_sec']:.2f}s")

    print_report(report)

    # Sauvegarde JSON
    report_path.write_text(
        json.dumps(report.model_dump(), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"\n💾 Rapport sauvegardé : {report_path}")

    # Exit code non-zéro si critique → utilisable en CI
    if report.verdict == "critical":
        sys.exit(2)
    elif report.verdict == "warning":
        sys.exit(1)