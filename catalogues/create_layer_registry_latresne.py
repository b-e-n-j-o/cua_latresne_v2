# scripts/populate_latresne_registry.py
import json
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    with open("/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/catalogues/catalogue_couches_map.json", "r") as f:
        catalogue = json.load(f)
    
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
        port=5432
    )
    cur = conn.cursor()
    
    for layer_id, data in catalogue.items():
        attribut_map = data.get("attribut_map")
        if attribut_map == "None":
            attribut_map = None
            
        cur.execute("""
            INSERT INTO latresne.layer_registry 
            (layer_id, nom, type, table_name, article, attribut_map)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (layer_id) DO UPDATE SET
                nom = EXCLUDED.nom,
                type = EXCLUDED.type,
                article = EXCLUDED.article,
                attribut_map = EXCLUDED.attribut_map;
        """, (
            layer_id,
            data["nom"],
            data["type"],
            layer_id,
            data.get("article"),
            attribut_map
        ))
    
    conn.commit()
    print(f"✅ {len(catalogue)} couches insérées")
    
    cur.execute("SELECT layer_id, nom, attribut_map FROM latresne.layer_registry ORDER BY layer_id;")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} (attribut: {row[2]})")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()