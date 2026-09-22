-- 005_metriques_tour.sql
-- Lot 0.2 — JSONB compact exploitable en SQL (indépendant de raw_llm_context).
-- Idempotent. Une commune = une instruction ; relancer n'échoue pas.

ALTER TABLE latresne.plu_messages
  ADD COLUMN IF NOT EXISTS metriques_tour jsonb;

ALTER TABLE argeles.plu_messages
  ADD COLUMN IF NOT EXISTS metriques_tour jsonb;

COMMENT ON COLUMN latresne.plu_messages.metriques_tour IS
  'Lot 0 : nb_rounds, tool_calls[], latence_ms, tokens_in, tokens_out, caracteres_injectes.';
COMMENT ON COLUMN argeles.plu_messages.metriques_tour IS
  'Lot 0 : nb_rounds, tool_calls[], latence_ms, tokens_in, tokens_out, caracteres_injectes.';

-- Rejeu des tours déjà capturés (raw_llm_context).
UPDATE latresne.plu_messages
SET metriques_tour = jsonb_strip_nulls(jsonb_build_object(
  'version', 1,
  'source', 'backfill_raw_llm_context',
  'nb_rounds', jsonb_array_length(coalesce(raw_llm_context->'gemini_rounds', '[]'::jsonb)),
  'tool_calls', (
    SELECT coalesce(jsonb_agg(t->>'name'), '[]'::jsonb)
    FROM jsonb_array_elements(coalesce(raw_llm_context->'tool_invocations', '[]'::jsonb)) t
  ),
  'latence_ms', latency_ms,
  'tokens_in', coalesce(
    (raw_llm_context->'gemini_usage_total'->>'prompt_token_count')::int,
    prompt_tokens
  ),
  'tokens_out', coalesce(
    (raw_llm_context->'gemini_usage_total'->>'candidates_token_count')::int,
    candidate_tokens
  ),
  'caracteres_injectes',
    length(coalesce(raw_llm_context->>'system_instruction', ''))
    + length(coalesce(raw_llm_context->>'user_message', ''))
))
WHERE role = 'model'
  AND raw_llm_context IS NOT NULL
  AND metriques_tour IS NULL;

UPDATE argeles.plu_messages
SET metriques_tour = jsonb_strip_nulls(jsonb_build_object(
  'version', 1,
  'source', 'backfill_raw_llm_context',
  'nb_rounds', jsonb_array_length(coalesce(raw_llm_context->'gemini_rounds', '[]'::jsonb)),
  'tool_calls', (
    SELECT coalesce(jsonb_agg(t->>'name'), '[]'::jsonb)
    FROM jsonb_array_elements(coalesce(raw_llm_context->'tool_invocations', '[]'::jsonb)) t
  ),
  'latence_ms', latency_ms,
  'tokens_in', coalesce(
    (raw_llm_context->'gemini_usage_total'->>'prompt_token_count')::int,
    prompt_tokens
  ),
  'tokens_out', coalesce(
    (raw_llm_context->'gemini_usage_total'->>'candidates_token_count')::int,
    candidate_tokens
  ),
  'caracteres_injectes',
    length(coalesce(raw_llm_context->>'system_instruction', ''))
    + length(coalesce(raw_llm_context->>'user_message', ''))
))
WHERE role = 'model'
  AND raw_llm_context IS NOT NULL
  AND metriques_tour IS NULL;
