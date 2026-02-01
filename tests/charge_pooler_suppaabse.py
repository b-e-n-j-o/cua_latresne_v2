import os
import time
import threading
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_DB = os.getenv("SUPABASE_DB")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PASSWORD = os.getenv("SUPABASE_PASSWORD")
SUPABASE_PORT = os.getenv("SUPABASE_PORT") or "5432"

DATABASE_URL = (
    f"postgresql+psycopg2://{SUPABASE_USER}:{SUPABASE_PASSWORD}"
    f"@{SUPABASE_HOST}:{SUPABASE_PORT}/{SUPABASE_DB}"
)

# Paramètres de test
NB_THREADS = 1     # nombre de connexions simultanées à tester
NB_ROUNDS = 3          # combien de “tours” de test
SLEEP_BETWEEN_ROUNDS = 2  # secondes

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

def worker(idx: int):
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT 1")).scalar()
            print(f"[OK] Connexion {idx}: SELECT 1 -> {res}")
            # Garder la connexion ouverte un peu pour simuler un travail réel
            time.sleep(1)
    except OperationalError as e:
        print(f"[ERR] Connexion {idx}: OperationalError -> {e}")
    except Exception as e:
        print(f"[ERR] Connexion {idx}: {type(e).__name__} -> {e}")

def run_round(round_idx: int):
    print(f"\n=== ROUND {round_idx+1}/{NB_ROUNDS} - {NB_THREADS} connexions simultanées ===")
    threads = []
    for i in range(NB_THREADS):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print(f"=== ROUND {round_idx+1} terminé ===")

def main():
    print("Test de charge Supabase Postgres")
    print(f"URL: {DATABASE_URL}")
    for r in range(NB_ROUNDS):
        run_round(r)
        time.sleep(SLEEP_BETWEEN_ROUNDS)

if __name__ == "__main__":
    main()