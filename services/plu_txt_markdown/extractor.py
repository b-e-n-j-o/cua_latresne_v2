import re
import sys
import time
from pathlib import Path
from google.genai import types

from .shared import MODEL, compute_cost, get_client, parse_usage_metadata


def clean_plu_text(raw: str) -> str:
    text = raw
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)  # césures
    text = re.sub(r"\n\s*\d{1,4}\s*\n", "\n", text)  # n° de page isolés
    text = re.sub(r"^[\s]*[▪►•◦→o]\s+", "- ", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_zone(prompt: str, raw: str) -> tuple[str, dict]:
    raw_clean = clean_plu_text(raw)
    full_prompt = f"{prompt}\n\n---\nTEXTE SOURCE À RETRANSCRIRE :\n\n{raw_clean}"

    t0 = time.perf_counter()
    response = get_client().models.generate_content(
        model=MODEL,
        contents=full_prompt,
        config=types.GenerateContentConfig(
            temperature=0.0,
            max_output_tokens=32000,
        ),
    )
    elapsed_sec = time.perf_counter() - t0

    tokens = parse_usage_metadata(response.usage_metadata)
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
        "ratio_chars": len(response.text) / len(raw_clean),
        "elapsed_sec": elapsed_sec,
        **cost,
    }
    return response.text, stats


def validate(raw_clean: str, md: str) -> list[str]:
    issues = []

    ratio = len(md) / len(raw_clean)
    if ratio < 0.85:
        issues.append(f"Compression suspecte : {ratio:.0%}")

    for pat in [
        r"voir texte source",
        r"pour détails",
        r"cf\. règlement",
        r"détails dans",
    ]:
        if re.search(pat, md, re.I):
            issues.append(f"Évasion : '{pat}'")

    nums_re = re.compile(r"\b\d+(?:[.,]\d+)?\s*(?:m²|m\b|%|mètres?)\b", re.I)
    nums_raw = {re.sub(r"\s", "", m.group()).lower() for m in nums_re.finditer(raw_clean)}
    nums_md = {re.sub(r"\s", "", m.group()).lower() for m in nums_re.finditer(md)}
    missing = nums_raw - nums_md
    if missing:
        issues.append(f"Nombres manquants ({len(missing)}) : {sorted(missing)[:10]}")

    arts_re = re.compile(r"[LR]\.?\s?\d{3}-\d+", re.I)
    arts_raw = {re.sub(r"\s", "", a) for a in arts_re.findall(raw_clean)}
    arts_md = {re.sub(r"\s", "", a) for a in arts_re.findall(md)}
    missing_arts = arts_raw - arts_md
    if missing_arts:
        issues.append(f"Articles Code manquants : {sorted(missing_arts)}")

    return issues


if __name__ == "__main__":
    ROOT = Path(__file__).resolve().parent
    PROMPT_PATH = ROOT / "prompts" / "extractor.txt"

    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(
            "Usage : python extractor.py [texte_brut.txt] [sortie.md]\n"
            "  Défauts : texte_brut.txt = reglement_brut.txt ; "
            "sortie = output/extracted/<nom_du_brut>.md\n"
            f"  Prompt : {PROMPT_PATH.relative_to(ROOT)}"
        )
        sys.exit(0)

    raw_path = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "reglement_brut.txt"
    if len(sys.argv) > 2:
        out_path = Path(sys.argv[2])
    else:
        out_dir = ROOT / "output" / "extracted"
        out_path = out_dir / f"{raw_path.stem}.md"

    prompt = PROMPT_PATH.read_text(encoding="utf-8")
    raw = raw_path.read_text(encoding="utf-8")

    md, s = extract_zone(prompt, raw)
    print(
        f"📊 Tokens : in={s['input']}  out={s['output_visible']}  "
        f"thinking={s['thinking']}  cached={s['cached']}"
    )
    print(
        f"💰 Coût   : input ${s['cost_input_usd']:.4f}  output ${s['cost_output_usd']:.4f}  "
        f"cache ${s['cost_cache_usd']:.4f}  total ${s['total_usd']:.4f} "
        f"(~{s['total_eur_approx'] * 100:.2f}¢ EUR)"
    )
    print(f"⏱️  Durée LLM : {s['elapsed_sec']:.2f}s")
    print(f"📏 Ratio chars sortie/entrée : {s['ratio_chars']:.0%}")

    issues = validate(clean_plu_text(raw), md)
    if issues:
        print("⚠️  Audit :")
        for i in issues:
            print(f"   - {i}")
    else:
        print("✅ Audit OK")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")
    print(f"💾 Markdown : {out_path}")
