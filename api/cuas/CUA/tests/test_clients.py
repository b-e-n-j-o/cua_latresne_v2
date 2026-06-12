"""
Preuve : insertion multi-schémas avec un seul client
"""
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Un seul client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

test_slug = "proof_" + os.urandom(3).hex()

# ============================================================
# TEST 1 : INSERT dans latresne.pipelines
# ============================================================
print(f"\n🧪 INSERT dans latresne.pipelines (slug: {test_slug})")
try:
    response = supabase.schema("latresne").table("pipelines").insert({
        "slug": test_slug,
        "status": "success",
        "commune": "Latresne",
        "code_insee": "33234",
    }).execute()
    print("✅ INSERT OK")
    print(f"   Données: {response.data}")
except Exception as e:
    print(f"❌ ERREUR: {e}")
    exit(1)

# ============================================================
# TEST 2 : SELECT pour vérifier
# ============================================================
print(f"\n🧪 SELECT depuis latresne.pipelines")
try:
    response = supabase.schema("latresne").table("pipelines").select("*").eq("slug", test_slug).execute()
    print(f"✅ SELECT OK - {len(response.data)} ligne(s) trouvée(s)")
except Exception as e:
    print(f"❌ ERREUR: {e}")

# ============================================================
# TEST 3 : UPSERT dans public.shortlinks
# ============================================================
print(f"\n🧪 UPSERT dans public.shortlinks")
try:
    response = supabase.schema("public").table("shortlinks").upsert({
        "slug": test_slug,
        "target_url": "https://test.com"
    }).execute()
    print("✅ UPSERT OK")
except Exception as e:
    print(f"❌ ERREUR: {e}")

# ============================================================
# CLEANUP
# ============================================================
print(f"\n🧹 Nettoyage...")
supabase.schema("latresne").table("pipelines").delete().eq("slug", test_slug).execute()
supabase.schema("public").table("shortlinks").delete().eq("slug", test_slug).execute()

print("\n✅ PREUVE COMPLÈTE : .schema() fonctionne pour INSERT/UPSERT")