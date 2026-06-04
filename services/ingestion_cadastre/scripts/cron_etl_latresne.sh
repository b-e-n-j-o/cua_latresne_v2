#!/usr/bin/env bash
# ETL quotidien Latresne — crontab LOCAL (Mac). Pour Render, utiliser render_cron_etl_latresne.sh.
#
# Prérequis :
#   - Python venv avec deps du backend (geopandas, psycopg, …)
#   - cua_latresne_v4/.env (SUPABASE_*, SLACK_WEBHOOK ou SLACK_DEPLOY_WEBHOOK)
#
# Installation cron (ex. tous les jours à 3h) :
#   chmod +x scripts/cron_etl_latresne.sh
#   crontab -e
#   0 3 * * * /Volumes/T7/Travaux_Freelance/KERELIA/CUAs/BACKEND_PRINCIPAL/LATRESNE/cua_latresne_v4/services/ingestion_cadastre/scripts/cron_etl_latresne.sh >> /tmp/etl_latresne.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INGEST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKEND_ROOT="$(cd "${INGEST_DIR}/../.." && pwd)"

# Adapter le chemin du venv si besoin
VENV_PYTHON="${VENV_PYTHON:-/Users/benjaminbenoit/Documents/venvs/keralia_venv/bin/python}"

export PYTHONPATH="${BACKEND_ROOT}:${PYTHONPATH:-}"

cd "${INGEST_DIR}"

exec "${VENV_PYTHON}" run_etl_commune.py \
  --schema latresne \
  --insee 33234 \
  --parcelles-mode etalab
