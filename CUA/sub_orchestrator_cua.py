# -*- coding: utf-8 -*-
"""
sub_orchestrator_cua.py — Pipeline de génération des visualisations et du CUA
------------------------------------------------------
1️⃣ Génère la carte 2D Folium à partir du WKT de l’unité foncière
2️⃣ Génère la visualisation 3D Plotly à partir du même WKT
3️⃣ Upload sur Supabase Storage (bucket: visualisation)
4️⃣ Construit l’URL publique encodée pour affichage sur Vercel (/maps?t=...)
5️⃣ Crée un shortlink / QR dynamique
6️⃣ Lance la génération du CUA DOCX avec QR
7️⃣ Upload final des artifacts et insertion dans latresne.pipelines
------------------------------------------------------
"""

import os
import json
import base64
import logging
import subprocess
import secrets
import string
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

from map_2d import generate_map_from_wkt
from map_3d import exporter_visualisation_3d_plotly_from_wkt

# ============================================================
# 🔧 CONFIGURATION
# ============================================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "visualisation"
KERELIA_BASE_URL = "https://kerelia.fr/maps"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("sub_orchestrator_cua")

# ============================================================
# 🔗 UTILITAIRES
# ============================================================

def generate_short_slug(length=26):
    """Génère un slug aléatoire sécurisé (sans caractères ambigus O, 0, I, l)."""
    alphabet = ''.join(ch for ch in string.ascii_letters + string.digits if ch not in 'O0Il')
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def upload_to_supabase(local_path, remote_path):
    """Upload d’un fichier vers Supabase Storage et renvoie l’URL publique."""
    with open(local_path, "rb") as f:
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            remote_path,
            f.read(),
            {
                "content-type": "application/octet-stream",
                "cache-control": "no-cache",
                "upsert": "true",
            },
        )
    public_url = f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{remote_path}"
    return public_url


# ============================================================
# 🧩 PIPELINE PRINCIPAL
# ============================================================

def generer_visualisations_et_cua_depuis_wkt(wkt_path, out_dir, commune="latresne", code_insee="33234"):
    OUT_DIR = out_dir
    os.makedirs(OUT_DIR, exist_ok=True)
    
    logger.info(f"🚀 Lancement du pipeline global pour le WKT : {wkt_path}")
    if not os.path.exists(wkt_path):
        raise FileNotFoundError(f"❌ Fichier WKT introuvable : {wkt_path}")

    # --------------------------------------------------------
    # ÉTAPE 1 : CARTE 2D
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 1/5 : Génération de la carte 2D Folium")

    html_2d, metadata_2d = generate_map_from_wkt(
        wkt_path=wkt_path,
        code_insee=code_insee,
        inclure_ppri=True,
        ppri_table=f"{commune}.pm1_detaillee_gironde"
    )

    html_2d_path = os.path.join(OUT_DIR, "carte_2d_unite_fonciere.html")
    with open(html_2d_path, "w", encoding="utf-8") as f:
        f.write(html_2d)
    logger.info(f"✅ Carte 2D sauvegardée : {html_2d_path}")

    # --------------------------------------------------------
    # ÉTAPE 2 : CARTE 3D
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 2/5 : Génération de la visualisation 3D Plotly")
    res3d = exporter_visualisation_3d_plotly_from_wkt(
        wkt_path=wkt_path,
        output_dir=OUT_DIR,
        exaggeration=1.5
    )
    path_3d = res3d["path"]
    logger.info(f"✅ Carte 3D générée : {path_3d}")

    # --------------------------------------------------------
    # ÉTAPE 3 : UPLOAD SUPABASE (cartes)
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 3/5 : Upload des cartes sur Supabase...")
    remote_dir = os.path.basename(wkt_path).replace(".wkt", "")
    remote_2d = f"{remote_dir}/carte_2d.html"
    remote_3d = f"{remote_dir}/carte_3d.html"

    url_2d = upload_to_supabase(html_2d_path, remote_2d)
    url_3d = upload_to_supabase(path_3d, remote_3d)

    logger.info(f"🌐 URL publique 2D : {url_2d}")
    logger.info(f"🌐 URL publique 3D : {url_3d}")

    # --------------------------------------------------------
    # ÉTAPE 4 : CONSTRUCTION DU LIEN ENCODED + SHORTLINK
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 4/5 : Construction du lien encodé et du QR dynamique")
    payload = {"carte2d": url_2d, "carte3d": url_3d, "commune": commune}
    token = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    maps_page_url = f"{KERELIA_BASE_URL}?t={token}"

    slug = generate_short_slug()
    try:
        supabase.table("shortlinks").upsert({"slug": slug, "target_url": maps_page_url}).execute()
        qr_url = f"https://kerelia.fr/m/{slug}"
        logger.info(f"✅ Lien court créé : {qr_url}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur création shortlink : {e}")
        qr_url = maps_page_url

    # ============================================================
    # 📄 ÉTAPE 5 : Génération du CUA DOCX
    # ============================================================
    logger.info("\n📦 ÉTAPE 5/5 : Génération du CUA DOCX avec QR dynamique")

    BASE_DIR = os.path.dirname(__file__)
    builder_path = os.path.join(BASE_DIR, "cua_builder.py")

    cerfa_path = os.path.join(OUT_DIR, "cerfa_result.json")
    intersections_files = list(Path(OUT_DIR).glob("rapport_intersections_*.json"))
    if intersections_files:
        intersections_path = str(max(intersections_files, key=lambda p: p.stat().st_mtime))
        logger.info(f"📑 Rapport d'intersections utilisé : {intersections_path}")
    else:
        raise FileNotFoundError(f"❌ Aucun rapport d'intersections trouvé dans {OUT_DIR}")

    catalogue_path = os.path.join(BASE_DIR, "catalogue_avec_articles.json")
    output_docx_path = os.path.join(OUT_DIR, "CUA_unite_fonciere.docx")
    logo_latresne_path = os.path.join(BASE_DIR, "logos", "logo_latresne.png")
    logo_kerelia_path = os.path.join(BASE_DIR, "logos", "logo_kerelia.png")

    cmd = [
        "python3", builder_path,
        "--cerfa-json", cerfa_path,
        "--intersections-json", intersections_path,
        "--catalogue-json", catalogue_path,
        "--output", output_docx_path,
        "--logo-first-page", logo_latresne_path,
        "--signature-logo", logo_kerelia_path,
        "--qr-url", qr_url,
        "--plu-nom", "PLU de Latresne",
        "--plu-date-appro", "13/02/2017"
    ]

    logger.info(f"🛠️ Commande exécutée : {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        logger.info("✅ CUA DOCX généré avec succès.")
    except subprocess.CalledProcessError as e:
        logger.error(f"💥 Échec génération CUA DOCX : {e}")

    # ============================================================
    # 📤 UPLOAD FINAL DES ARTIFACTS + ENREGISTREMENT PIPELINE
    # ============================================================
    logger.info("\n📤 Upload final du CUA et du pipeline_result.json vers Supabase...")

    remote_cua = f"{remote_dir}/CUA_unite_fonciere.docx"
    cua_url = upload_to_supabase(output_docx_path, remote_cua)
    logger.info(f"📎 CUA uploadé : {cua_url}")

    pipeline_result_path = os.path.join(OUT_DIR, "pipeline_result.json")
    if os.path.exists(pipeline_result_path):
        remote_result = f"{remote_dir}/pipeline_result.json"
        result_url = upload_to_supabase(pipeline_result_path, remote_result)
        logger.info(f"🧾 Résumé pipeline uploadé : {result_url}")
    else:
        result_url = None
        logger.warning("⚠️ Aucun pipeline_result.json trouvé à uploader.")

    result = {
        "maps_page": maps_page_url,
        "shortlink": qr_url,
        "slug": slug,
        "2d_url": url_2d,
        "3d_url": url_3d,
        "output_cua": cua_url,
        "metadata_2d": metadata_2d,
        "metadata_3d": res3d["metadata"],
    }

    try:
        supabase.table("latresne.pipelines").upsert({
            "slug": slug,
            "code_insee": code_insee,
            "commune": commune,
            "status": "success",
            "bucket_path": remote_dir,
            "output_cua": cua_url,
            "carte_2d_url": url_2d,
            "carte_3d_url": url_3d,
            "qr_url": qr_url,
            "pipeline_result_url": result_url,
            "metadata": result,
        }).execute()
        logger.info("✅ Métadonnées pipeline enregistrées dans latresne.pipelines.")
    except Exception as e:
        logger.error(f"💥 Erreur d'insertion dans latresne.pipelines : {e}")

    # --------------------------------------------------------
    # FIN DU PIPELINE
    # --------------------------------------------------------
    logger.info("\n🎉 PIPELINE CUA TERMINÉ AVEC SUCCÈS 🎉")
    return result


# ============================================================
# ▶️ CLI
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sous-orchestrateur pour génération carte 2D, 3D et CUA.")
    parser.add_argument("--wkt", required=True, help="Chemin vers le fichier WKT (unité foncière)")
    parser.add_argument("--out-dir", required=True, help="Dossier de sortie timestampé")
    parser.add_argument("--code_insee", default="33234")
    parser.add_argument("--commune", default="latresne")
    args = parser.parse_args()

    result = generer_visualisations_et_cua_depuis_wkt(
        wkt_path=args.wkt,
        out_dir=args.out_dir,
        commune=args.commune,
        code_insee=args.code_insee
    )

    print("\n📦 RÉSULTAT FINAL :")
    print(json.dumps(result, indent=2, ensure_ascii=False))
