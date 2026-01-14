# scripts/add_geom_3857_columns.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("SUPABASE_HOST"),
    dbname=os.getenv("SUPABASE_DB"),
    user=os.getenv("SUPABASE_USER"),
    password=os.getenv("SUPABASE_PASSWORD"),
    port=5432
)
cur = conn.cursor()

cur.execute("SELECT table_name FROM latresne.layer_registry")
tables = [r[0] for r in cur.fetchall()]

for table in tables:
    print(f"Processing {table}...")
    
    cur.execute(f"""
        ALTER TABLE latresne.{table} 
        ADD COLUMN IF NOT EXISTS geom_3857 geometry(Geometry, 3857)
    """)
    
    cur.execute(f"""
        UPDATE latresne.{table} 
        SET geom_3857 = ST_Force2D(ST_Transform(geom_2154, 3857))
        WHERE geom_3857 IS NULL
    """)
    
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_geom_3857 
        ON latresne.{table} USING GIST(geom_3857)
    """)
    
    conn.commit()
    print(f"✅ {table}")

cur.execute("UPDATE latresne.layer_registry SET geom_column = 'geom_3857'")
conn.commit()
print("\n✅ Registry updated")

cur.close()
conn.close()