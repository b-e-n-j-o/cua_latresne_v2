
from supabase import create_client
import os

SUPABASE_URL = "https://odlkagfeqkbrruajlcxm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9kbGthZ2ZlcWticnJ1YWpsY3htIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0NzkwMzYyMywiZXhwIjoyMDYzNDc5NjIzfQ.kQWiASDB1693r_klN1LZ-oNul4tU1FirzjVuvPaLrd0"   # ⚠️ pas l’anon key


supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Ton nouveau CUA
local_path = "/Users/benjaminbenoit/Downloads/cua_exemple.docx"

# Chemin exact à remplacer
remote_path = "visualisation/7fniMsdtcVUFK6KNasJTsWmA2N/CUA_unite_fonciere.docx"

with open(local_path, "rb") as f:
    supabase.storage.from_("visualisation").upload(
        remote_path,
        f.read(),
        {
            "content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "cache-control": "3600",
            "upsert": "true"    # <<< IMPORTANT : doit être une STRING
        }
    )

print("🎉 Upload terminé !")

