# Simuler un build Render en local

## Créer le venv de test
python3.13 -m venv /tmp/render_test_venv
source /tmp/render_test_venv/bin/activate

## Installer les dépendances
cd /chemin/vers/ton/projet
pip install -r requirements.txt

## Lancer l'app sans .env (comme Render)
env -i PATH="$PATH" \
  SUPABASE_HOST="..." \
  SUPABASE_USER="..." \
  SUPABASE_DB="postgres" \
  SUPABASE_PASSWORD="..." \
  SUPABASE_PORT="5432" \
  GEMINI_API_KEY="..." \
  uvicorn main:app --port 8001

## Supprimer le venv après
deactivate
rm -rf /tmp/render_test_venv