#!/usr/bin/env bash
# Test manuel : POST /api/identite-fonciere/publier
# Usage :
#   export API_BASE_URL=https://votre-api.onrender.com   # ou http://127.0.0.1:8000
#   bash scripts/curl_identite_publier.sh
#
# Attendu : JSON avec "success": true et "carte_url" commençant par http(s).
# Ce lien est le même principe que celui injecté dans le PDF (carte_web_url côté serveur).

set -euo pipefail
API_BASE_URL="${API_BASE_URL:-http://127.0.0.1:8000}"
API_BASE_URL="${API_BASE_URL%/}"

echo "POST ${API_BASE_URL}/api/identite-fonciere/publier"
echo "---"

curl -sS -X POST "${API_BASE_URL}/api/identite-fonciere/publier" \
  -H "Content-Type: application/json" \
  -d @- <<'EOF' | python3 -m json.tool 2>/dev/null || cat
{
  "commune": "Latresne",
  "insee": "33234",
  "srid": 4326,
  "geometry": {
    "type": "Polygon",
    "coordinates": [[[-0.5073, 44.7895], [-0.5048, 44.7895], [-0.5048, 44.791], [-0.5073, 44.791], [-0.5073, 44.7895]]]
  },
  "intersections": [
    {
      "table": "plu_latresne",
      "display_name": "Zonage PLU",
      "elements": [{ "zonage_reglement": "A" }]
    }
  ]
}
EOF

echo ""
echo "---"
echo "Si carte_url est vide ou erreur 5xx : vérifier les logs Render, SUPABASE_*, IDENTITE_FONCIERE_STORAGE_UPLOAD."
echo "Si le PDF n’a toujours pas de lien : la ligne « Carte interactive » exige carte_web_url en https (voir header._map_url_from_result)."
