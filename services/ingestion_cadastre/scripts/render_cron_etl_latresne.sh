#!/usr/bin/env bash
# Cron Render — ETL quotidien Latresne (s'exécute sur l'infra Render, pas sur votre Mac).
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
  BASE="${RENDER_EXTERNAL_URL:-${BACKEND_URL:-https://api.kerelia.fr}}"
  BASE="${BASE%/}"
  if [[ -z "${INTERNAL_TOKEN:-}" ]]; then
    echo "INTERNAL_TOKEN requis pour POST ${BASE}/admin/etl/commune" >&2
    exit 1
  fi
  exec curl -sfS -X POST "${BASE}/admin/etl/commune" \
    -H "Content-Type: application/json" \
    -H "x-internal-token: ${INTERNAL_TOKEN}" \
    -d '{"schema":"latresne","insee":"33234","parcelles_mode":"etalab"}'
fi

# Exécution directe Python (même image / deps que le backend)
export PYTHONPATH="${ROOT}:${PYTHONPATH:-}"
exec python services/ingestion_cadastre/run_etl_commune.py \
  --schema latresne \
  --insee 33234 \
  --parcelles-mode etalab
