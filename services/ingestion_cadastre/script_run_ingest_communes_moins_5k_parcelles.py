"""
run_ingestion_massive.py

Orchestre l'ingestion massive de parcelles cadastrales via l'endpoint Render.
- Lit le CSV des communes (v_commune_2025.csv)
- Pour chaque commune, vérifie le nb de parcelles via Etalab
- N'ingère que les communes avec < 5000 parcelles
- S'arrête quand la base atteint 2 Go de parcelles ingérées
- Ton Mac orchestre, Render fait le boulot lourd

Usage :
    python run_ingestion_massive.py
    python run_ingestion_massive.py --dry-run        # sans insert
    python run_ingestion_massive.py --dep 33         # filtrer un dep
    python run_ingestion_massive.py --limit-communes 100  # tester sur 100

    lister les jobs en cours : curl -s https://api.kerelia.fr/admin/parcelles/jobs | python3 -m json.tool
"""

import os
import csv
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────

CSV_PATH      = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/CONFIG/v_commune_2025.csv"
BACKEND_URL   = "https://api.kerelia.fr"   # ou http://localhost:8000 pour test local
ETALAB_COM    = "https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles"

MAX_PARCELLES_PAR_COMMUNE = 5_000    # seuil de filtrage
MAX_DB_SIZE_GO            = 2.0      # s'arrêter à 2 Go ingérés
POLL_INTERVAL_S           = 3        # secondes entre chaque poll de status
MAX_WAIT_S                = 300      # timeout max par commune (5 min)
PAUSE_BETWEEN_S           = 0.5      # pause entre communes
PARALLELISME              = 3        # nb de communes traitées en parallèle

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY")
supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def load_communes(csv_path: str, dep_filter: str = None) -> list[dict]:
    """Charge les communes du CSV, filtre sur TYPECOM=COM et optionnellement sur DEP."""
    communes = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["TYPECOM"] != "COM":
                continue
            if dep_filter and row["DEP"] != dep_filter:
                continue
            communes.append({
                "insee":   row["COM"],
                "nom":     row["NCCENR"],
                "dep":     row["DEP"],
            })
    return communes


def count_parcelles_etalab(insee: str) -> int | None:
    """
    Compte les parcelles d'une commune via Etalab sans télécharger les géométries.
    On fetch juste le début du JSON pour lire le totalFeatures... mais Etalab
    ne supporte pas RESULTTYPE=hits donc on utilise le Content-Length approximatif.
    
    Alternative plus fiable : on fetch et on compte les features.
    On utilise stream=True pour interrompre dès qu'on a le count.
    """
    try:
        r = requests.get(
            ETALAB_COM.format(insee=insee),
            timeout=60,
            stream=True
        )
        r.raise_for_status()
        
        # Lire juste assez pour compter les occurrences de '"id":'
        # qui correspondent aux features — sans tout télécharger
        chunk_total = b""
        feature_count = 0
        for chunk in r.iter_content(chunk_size=32768):
            chunk_total += chunk
            # Compter les occurrences de '"id":"' dans ce qu'on a lu
            feature_count = chunk_total.count(b'"id":"')
            # Si on a dépassé le seuil, inutile de continuer
            if feature_count > MAX_PARCELLES_PAR_COMMUNE:
                r.close()
                return feature_count
            # Sécurité : on ne lit pas plus de 2 Mo
            if len(chunk_total) > 2_000_000:
                r.close()
                return feature_count
        
        r.close()
        return feature_count
    except Exception as e:
        return None


def ingest_commune(insee: str, dry_run: bool) -> dict | None:
    """Lance l'ingestion d'une commune via l'endpoint Render."""
    try:
        r = requests.post(
            f"{BACKEND_URL}/admin/parcelles/ingest",
            json={"communes": [insee], "dry_run": dry_run},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return None


def poll_job(job_id: str, max_wait: int = MAX_WAIT_S) -> dict | None:
    """Poll le status d'un job jusqu'à completion ou timeout."""
    start = time.time()
    while time.time() - start < max_wait:
        try:
            r = requests.get(
                f"{BACKEND_URL}/admin/parcelles/status/{job_id}",
                timeout=10,
            )
            r.raise_for_status()
            job = r.json()
            if job["status"] in ("done", "error"):
                return job
        except Exception:
            pass
        time.sleep(POLL_INTERVAL_S)
    return None


def get_db_size_go() -> float | None:
    """
    Interroge l'endpoint backend pour connaître la taille du schema parcelles.
    Si l'endpoint n'existe pas, retourne None (on ignore la limite).
    """
    try:
        r = requests.get(f"{BACKEND_URL}/admin/parcelles/db-size", timeout=10)
        if r.status_code == 200:
            return r.json().get("size_go")
    except Exception:
        pass
    return None


def estimate_db_size_go(total_upserted: int) -> float:
    """
    Estimation locale basée sur le ratio mesuré :
    ~0.5 Ko par parcelle en PostGIS (données + géométries compressées)
    """
    return (total_upserted * 500) / 1e9  # 500 octets par parcelle estimé


def load_communes_en_base() -> set[str]:
    """Charge tous les code_insee déjà présents en base (pagination)."""
    print("  Chargement des communes déjà en base...")
    try:
        en_base = set()
        page = 0
        page_size = 1000

        while True:
            res = (
                supabase_client
                .schema("parcelles")
                .table("parcelles")
                .select("code_insee")
                .range(page * page_size, (page + 1) * page_size - 1)
                .execute()
            )
            if not res.data:
                break
            for row in res.data:
                code = row.get("code_insee")
                if code:
                    en_base.add(code)
            if len(res.data) < page_size:
                break
            page += 1

        print(f"  → {len(en_base)} communes déjà en base\n")
        return en_base
    except Exception as e:
        print(f"  ⚠️  Impossible de charger les communes en base : {e}")
        return set()


def traiter_commune(com: dict, dry_run: bool, skip_count: bool) -> dict:
    """Traite une commune et retourne un résultat standardisé (thread-safe)."""
    insee = com["insee"]
    nom = com["nom"]

    if not skip_count:
        nb = count_parcelles_etalab(insee)
        if nb is None:
            return {"insee": insee, "nom": nom, "status": "count_failed", "upserted": 0, "nb": 0}
        if nb > MAX_PARCELLES_PAR_COMMUNE:
            return {"insee": insee, "nom": nom, "status": "ignoree", "upserted": 0, "nb": nb}
    else:
        nb = "?"

    job_info = ingest_commune(insee, dry_run=dry_run)
    if not job_info:
        return {"insee": insee, "nom": nom, "status": "endpoint_failed", "upserted": 0, "nb": nb}

    job = poll_job(job_info["job_id"])
    if not job:
        return {"insee": insee, "nom": nom, "status": "timeout", "upserted": 0, "nb": nb}
    if job["status"] == "error":
        return {"insee": insee, "nom": nom, "status": "error", "upserted": 0, "nb": nb}

    return {
        "insee": insee,
        "nom": nom,
        "status": "ok",
        "upserted": job.get("parcelles_upserted", 0),
        "nb": nb,
    }


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",         action="store_true")
    parser.add_argument("--dep",             type=str, default=None, help="Filtrer sur un département")
    parser.add_argument("--limit-communes",  type=int, default=None, help="Limiter le nb de communes traitées")
    parser.add_argument("--skip-count",      action="store_true",    help="Ne pas vérifier le nb de parcelles (plus rapide)")
    args = parser.parse_args()

    mode = "🔍 DRY RUN" if args.dry_run else "🚀 INGESTION RÉELLE"
    print(f"\n{'='*65}")
    print(f"  run_ingestion_massive.py — {mode}")
    print(f"  Backend : {BACKEND_URL}")
    print(f"  Seuil   : < {MAX_PARCELLES_PAR_COMMUNE:,} parcelles/commune")
    print(f"  Stop    : {MAX_DB_SIZE_GO} Go ingérés")
    if args.dep:
        print(f"  Filtre  : dep {args.dep} uniquement")
    print(f"{'='*65}\n")

    # Charger les communes
    communes = load_communes(CSV_PATH, dep_filter=args.dep)
    if args.limit_communes:
        communes = communes[:args.limit_communes]
    print(f"  {len(communes):,} communes chargées depuis le CSV\n")

    # Précharger les communes déjà en base
    communes_en_base = load_communes_en_base()
    communes_a_traiter = [c for c in communes if c["insee"] not in communes_en_base]
    print(f"  {len(communes_en_base)} communes déjà en base — skippées")
    print(f"  {len(communes_a_traiter)} communes restantes à traiter\n")

    # Stats globales
    total_traitees = 0
    total_ignorees = 0
    total_upsertees = 0
    total_erreurs = 0
    communes_ignorees = []
    communes_erreurs = []
    started_at = datetime.now()
    print_lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=PARALLELISME) as executor:
        futures = {
            executor.submit(traiter_commune, com, args.dry_run, args.skip_count): com
            for com in communes_a_traiter
        }

        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            res = future.result()
            insee = res["insee"]
            nom = res["nom"]
            label = f"[{done_count:05d}/{len(communes_a_traiter):05d}] {nom} ({insee})"

            with print_lock:
                if res["status"] == "ok":
                    total_upsertees += res["upserted"]
                    total_traitees += 1
                    print(f"  {label} — ✅ {res['upserted']:,} upsertées")
                elif res["status"] == "ignoree":
                    total_ignorees += 1
                    communes_ignorees.append(res)
                    print(f"  {label} — ⏭️  {res['nb']:,} parcelles > seuil")
                else:
                    total_erreurs += 1
                    communes_erreurs.append(res)
                    print(f"  {label} — ❌ {res['status']}")

                if done_count % 50 == 0:
                    size_go = estimate_db_size_go(total_upsertees)
                    elapsed = (datetime.now() - started_at).seconds // 60
                    print(
                        f"\n  ── {done_count}/{len(communes_a_traiter)} "
                        f"— {total_upsertees:,} parcelles "
                        f"— ~{size_go:.2f} Go "
                        f"— {elapsed} min\n"
                    )
                    if size_go >= MAX_DB_SIZE_GO:
                        print(f"\n  🛑 Seuil {MAX_DB_SIZE_GO} Go atteint — arrêt")
                        executor.shutdown(wait=False, cancel_futures=True)
                        break

    # Rapport final
    elapsed_total = (datetime.now() - started_at).seconds // 60
    size_go = estimate_db_size_go(total_upsertees)

    print(f"\n{'='*65}")
    print(f"  RAPPORT FINAL — {mode}")
    print(f"{'='*65}")
    print(f"  Durée              : {elapsed_total} min")
    print(f"  Communes traitées  : {total_traitees:,}")
    print(f"  Communes ignorées  : {total_ignorees:,} (> {MAX_PARCELLES_PAR_COMMUNE:,} parcelles)")
    print(f"  Communes en erreur : {total_erreurs:,}")
    print(f"  Parcelles upsertées: {total_upsertees:,}")
    print(f"  Taille DB estimée  : ~{size_go:.2f} Go")
    print(f"{'='*65}")

    if communes_erreurs:
        print(f"\n  ❌ Communes en erreur ({len(communes_erreurs)}) :")
        for e in communes_erreurs[:20]:
            print(f"      {e['nom']} ({e['insee']}) — {e.get('status', 'erreur')}")

    if communes_ignorees:
        print(f"\n  ⏭️  Top 10 communes ignorées (trop grandes) :")
        top = sorted(communes_ignorees, key=lambda x: x.get("nb", 0), reverse=True)[:10]
        for c in top:
            print(f"      {c['nom']} ({c['insee']}) — {c.get('nb', 0):,} parcelles")

    print()


if __name__ == "__main__":
    main()