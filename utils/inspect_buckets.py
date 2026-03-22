import os
from supabase import create_client
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()

# Init Supabase
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def inspect_bucket(bucket_name: str):
    """Liste les dossiers (1er niveau) avec leurs dates"""
    print(f"\n{'='*60}")
    print(f"📦 Bucket: {bucket_name}")
    print(f"{'='*60}\n")
    
    try:
        # Lister tous les fichiers
        files = supabase.storage.from_(bucket_name).list()
        
        folders = defaultdict(list)
        
        for item in files:
            name = item.get("name", "")
            created = item.get("created_at", "")
            updated = item.get("updated_at", "")
            
            # Si c'est un dossier (pas d'extension)
            if "/" not in name and "." not in name:
                folders[name].append({
                    "created": created,
                    "updated": updated
                })
            # Si c'est un chemin avec dossier
            elif "/" in name:
                folder = name.split("/")[0]
                folders[folder].append({
                    "created": created,
                    "updated": updated,
                    "file": name
                })
        
        # Afficher résumé par dossier
        for folder, items in sorted(folders.items()):
            dates = [datetime.fromisoformat(i["created"].replace("Z", "+00:00")) 
                     for i in items if i.get("created")]
            
            if dates:
                oldest = min(dates)
                newest = max(dates)
                print(f"📁 {folder}/")
                print(f"   Fichiers: {len(items)}")
                print(f"   Plus ancien: {oldest.strftime('%Y-%m-%d %H:%M')}")
                print(f"   Plus récent: {newest.strftime('%Y-%m-%d %H:%M')}")
                print()
        
        print(f"Total dossiers: {len(folders)}\n")
        
    except Exception as e:
        print(f"❌ Erreur: {e}\n")

# Inspecter les deux buckets
inspect_bucket("cua-artifacts")
inspect_bucket("visualisation")