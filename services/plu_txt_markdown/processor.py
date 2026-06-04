"""
Traitement d'un fichier TXT (contenu en mémoire) — logique alignée sur pipeline_texte_a_mardown/pipeline.py.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from .extractor import clean_plu_text, extract_zone
from .judge import AuditReport, audit, summarize_report
from .shared import empty_token_usage, log_gemini_tokens, merge_token_usages, validate_gemini_model


def needs_review(report: AuditReport) -> bool:
    if report.verdict in ("warning", "critical"):
        return True
    if any(i.severity == "high" for i in report.issues):
        return True
    return False

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
PROMPTS_DIR = ROOT / "prompts"
EXTRACTOR_PROMPT_PATH = PROMPTS_DIR / "extractor.txt"
JUDGE_PROMPT_PATH = PROMPTS_DIR / "judge.txt"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_tokens_summary(
    extract_stats: dict | None,
    judge_stats: dict | None,
) -> dict:
    extract_tokens = (extract_stats or {}).get("tokens") or empty_token_usage()
    # Si judge_stats est absent, on utilise empty_token_usage() au lieu de None
    judge_tokens = (judge_stats or {}).get("tokens") if judge_stats else empty_token_usage()
    
    total = merge_token_usages(extract_tokens, judge_tokens)
    return {
        "extract": extract_tokens,
        "judge": judge_tokens if judge_stats else None, # On peut laisser None dans le rapport final si souhaité
        "total": total,
    }


def process_txt_content(
    zone_stem: str,
    raw_text: str,
    out_dir: Path,
    *,
    skip_judge: bool = False,
    model: str | None = None,
) -> dict:
    model_id = validate_gemini_model(model)
    """
    Extrait le markdown, audite (optionnel), écrit ``<stem>.md`` (+ ``.audit.json`` si juge).
    Retourne un dict résumé pour l'API.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    validated_dir = out_dir / "validated"
    review_dir = out_dir / "review_needed"
    validated_dir.mkdir(parents=True, exist_ok=True)
    review_dir.mkdir(parents=True, exist_ok=True)

    prompt_extract = EXTRACTOR_PROMPT_PATH.read_text(encoding="utf-8")
    t_zone_start = time.perf_counter()

    try:
        md, extract_stats = extract_zone(
            prompt_extract,
            raw_text,
            model=model_id,
            log_context=zone_stem,
        )
    except Exception as e:
        logger.exception("Extraction échouée pour %s", zone_stem)
        return {
            "zone": zone_stem,
            "status": "extract_failed",
            "error": str(e),
        }

    report: AuditReport | None = None
    judge_stats: dict | None = None
    extract_llm_sec = extract_stats.get("elapsed_sec", 0.0)
    audit_llm_sec = 0.0

    if skip_judge:
        target_dir = validated_dir
        verdict = "no_judge"
        summary = None
        (target_dir / f"{zone_stem}.md").write_text(md, encoding="utf-8")
    else:
        raw_clean = clean_plu_text(raw_text)
        try:
            report, judge_stats = audit(
                raw_clean,
                md,
                prompt_path=JUDGE_PROMPT_PATH,
                model=model_id,
                log_context=zone_stem,
            )
            audit_llm_sec = judge_stats.get("elapsed_sec", 0.0)
        except Exception as e:
            logger.exception("Audit échoué pour %s", zone_stem)
            (review_dir / f"{zone_stem}.md").write_text(md, encoding="utf-8")
            tokens = _build_tokens_summary(extract_stats, None)
            return {
                "zone": zone_stem,
                "status": "audit_failed",
                "error": str(e),
                "extract_cost_usd": extract_stats.get("total_usd", 0.0),
                "tokens": tokens,
            }

        summary = summarize_report(report)
        verdict = report.verdict
        target_dir = review_dir if needs_review(report) else validated_dir
        (target_dir / f"{zone_stem}.md").write_text(md, encoding="utf-8")
        (target_dir / f"{zone_stem}.audit.json").write_text(
            report.model_dump_json(indent=2),
            encoding="utf-8",
        )

    judge_cost = judge_stats["total_usd"] if judge_stats else 0.0
    total_cost = extract_stats.get("total_usd", 0.0) + judge_cost
    total_zone_sec = time.perf_counter() - t_zone_start
    tokens = _build_tokens_summary(extract_stats, judge_stats)
    log_gemini_tokens(logger, context=zone_stem, phase="total_fichier", tokens=tokens.get("total"))

    return {
        "zone": zone_stem,
        "status": "done",
        "verdict": verdict,
        "routed_to": target_dir.name,
        "issues_summary": summary,
        "extract_cost_usd": extract_stats.get("total_usd", 0.0),
        "judge_cost_usd": judge_cost,
        "total_cost_usd": round(total_cost, 4),
        "duration_s": round(total_zone_sec, 2),
        "tokens": tokens,
        "timing": {
            "extract_llm_sec": extract_llm_sec,
            "audit_llm_sec": audit_llm_sec,
            "total_zone_sec": round(total_zone_sec, 2),
        },
        "finished_at": _utc_now_iso(),
    }
