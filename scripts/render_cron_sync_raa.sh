#!/usr/bin/env bash
# Cron Render — veille RAA quotidienne (scrape P-O + analyse LLM des nouveaux).
#
# Dashboard Render → Cron Job :
#   startCommand: bash scripts/render_cron_sync_raa.sh
#   schedule: "0 5 * * *"   # 5h UTC ≈ 6h/7h Paris selon heure d'été
#
# Variables d'environnement (copier depuis le service web) :
#   SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_DB, SUPABASE_PORT
#   GEMINI_API_KEY ou GOOGLE_API_KEY

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
exec python -m api.raa.cron_sync_raa
