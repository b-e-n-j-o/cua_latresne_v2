#!/usr/bin/env bash
# Cron Render — ETL quotidien pour toutes les communes de config/etl_communes.json
#
# Deux options dans le dashboard Render :
#
# A) Cron Job « one-off command » (recommandé) — startCommand de ce script :
#    bash services/ingestion_cadastre/scripts/render_cron_etl_latresne.sh
#
# B) Cron Job qui appelle l'API web (si le service web est toujours actif) :
#    bash services/ingestion_cadastre/scripts/render_cron_etl_latresne.sh --http
#
# Variables d'environnement requises sur le Cron Job (copier depuis le service web) :
#   SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD, SUPABASE_DB, SUPABASE_PORT
#   SLACK_WEBHOOK ou SLACK_DEPLOY_WEBHOOK
#   Pour --http : RENDER_EXTERNAL_URL ou BACKEND_URL, INTERNAL_TOKEN

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "${ROOT}"

MODE="${1:-}"

if [[ "${MODE}" == "--http" ]]; then
  echo "Mode --http : lancer un POST /admin/etl/commune par commune (voir config/etl_communes.json)" >&2
  echo "Sur Render, préférer l'exécution Python directe (sans --http)." >&2
  exit 1
fi

# Exécution directe Python — liste dans config/etl_communes.json
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
exec python services/ingestion_cadastre/run_etl_all_communes.py
