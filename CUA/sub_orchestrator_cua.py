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
import sys
import json
import base64
import logging
import gc
import secrets
import string
import tracemalloc
import shutil
from pathlib import Path
from dotenv import load_dotenv
try:
    import psutil
except ImportError:
    psutil = None
from supabase import create_client
from pyproj import Transformer
from shapely import wkt as shapely_wkt

from CUA.map2d.carte2d.carte2d_rendu import generer_carte_2d_depuis_wkt
from CUA.map3d.map_3d import exporter_visualisation_3d_plotly_from_wkt
from CUA.docx.cua_builder import run_builder
from INTERSECTIONS.export_gpkg_intersections import export_gpkg_from_wkt


# ============================================================
# 🔧 CONFIGURATION
# ============================================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = "visualisation"
KERELIA_BASE_URL = "https://kerelia.fr/maps"

# ✅ Un seul client global
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("sub_orchestrator_cua")

logger.info("🔗 Client Supabase initialisé avec succès.")
logger.info(f"🌍 URL Supabase : {SUPABASE_URL}")

# ============================================================
# 🔗 UTILITAIRES
# ============================================================

def log_memory(step: str) -> float:
    """Log la RAM utilisée à chaque étape (si psutil disponible)."""
    if psutil is None:
        return 0.0
    mem_mb = psutil.Process().memory_info().rss / 1024**2
    logger.info(f"🔹 [{step}] RAM: {mem_mb:.1f} MB")
    return mem_mb


def log_memory_detailed(step: str) -> None:
    """Log RAM process + tracemalloc pour debug fin."""
    if psutil:
        rss = psutil.Process().memory_info().rss / 1024**2
        logger.info(f"🔹 [{step}] RAM Process: {rss:.1f} MB")

    if tracemalloc.is_tracing():
        current, peak = tracemalloc.get_traced_memory()
        logger.info(
            f"   └─ Tracemalloc: current={current/1024**2:.1f}MB, "
            f"peak={peak/1024**2:.1f}MB"
        )


def generate_short_slug(length=26):
    """Génère un slug aléatoire sécurisé (sans caractères ambigus O, 0, I, l)."""
    alphabet = ''.join(ch for ch in string.ascii_letters + string.digits if ch not in 'O0Il')
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def upload_to_supabase(local_path, remote_path, bucket=None):
    """Upload d'un fichier vers Supabase Storage et renvoie l'URL publique.
    Lit le fichier, uploade, puis libère le contenu pour limiter la RAM."""
    bucket = bucket or SUPABASE_BUCKET
    with open(local_path, "rb") as f:
        content = f.read()
    supabase.storage.from_(bucket).upload(
        remote_path,
        content,
        {
            "content-type": "application/octet-stream",
            "cache-control": "no-cache",
            "upsert": "true",
        },
    )
    del content
    return f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{remote_path}"


def compute_centroid_from_wkt_path(wkt_path: str):
    """
    Centroïde (lon/lat WGS84) à partir du WKT Lambert-93 (EPSG:2154).

    Calcul local (Shapely + Pyproj) : aucune connexion Postgres, donc pas d’impact
    sur le pool Supabase (évite MaxClientsInSessionMode sur cette étape).
    """
    try:
        wkt_file = Path(wkt_path)
        if not wkt_file.exists():
            logger.warning(f"⚠️ WKT introuvable pour calcul du centroïde: {wkt_path}")
            return None

        geom_wkt = wkt_file.read_text(encoding="utf-8").strip()
        if not geom_wkt:
            logger.warning("⚠️ WKT vide pour calcul du centroïde")
            return None

        geom = shapely_wkt.loads(geom_wkt)
        if geom.is_empty:
            logger.warning("⚠️ Géométrie vide pour calcul du centroïde")
            return None

        c = geom.centroid
        transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
        lon, lat = transformer.transform(c.x, c.y)

        centroid = {"lon": float(lon), "lat": float(lat)}
        logger.info(f"📍 Centroïde UF calculé: {centroid}")
        return centroid

    except Exception as e:
        logger.warning(f"⚠️ Erreur lors du calcul du centroïde UF: {e}")
        return None


# ============================================================
# 🧩 PIPELINE PRINCIPAL
# ============================================================

def generer_visualisations_et_cua_depuis_wkt(
    wkt_path,
    out_dir,
    commune="latresne",
    code_insee="33234",
    skip_3d: bool = False,
    skip_gpkg: bool = False,
):
    OUT_DIR = out_dir
    os.makedirs(OUT_DIR, exist_ok=True)

    # ============================================================
    # 🧪 LOGS MÉMOIRE DÉTAILLÉS DANS UN FICHIER DÉDIÉ
    # ============================================================
    log_file = os.path.join(OUT_DIR, "memory_audit.log")
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    )
    # Éviter de dupliquer les FileHandler si la fonction est rappelée
    logger.handlers = [
        h for h in logger.handlers if not isinstance(h, logging.FileHandler)
    ]
    logger.addHandler(file_handler)
    logger.info(f"🧪 Memory audit log initialisé : {log_file}")
    # Tracemalloc pour audit détaillé
    if not tracemalloc.is_tracing():
        tracemalloc.start()
    log_memory_detailed("START")
    log_memory("CUA_START")

    # ============================================================
    # 🔑 GÉNÉRATION DU SLUG EN AMONT (source de vérité)
    # ============================================================
    slug = generate_short_slug()
    logger.info(f"🔑 Slug généré : {slug}")
    
    # ============================================================
    # 🩹 VÉRIFICATION ROBUSTE DU FICHIER WKT
    # ============================================================
    wkt_path = Path(wkt_path)
    if not wkt_path.exists():
        raise FileNotFoundError(f"❌ Fichier WKT introuvable : {wkt_path}")
    logger.info(f"🚀 Lancement du pipeline global pour le WKT : {wkt_path}")

    # --------------------------------------------------------
    # ÉTAPE 1 : CARTE 2D (NOUVEAU MOTEUR)
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 1/5 : Génération de la carte 2D (nouvelle version)")

    html_2d, metadata_2d = generer_carte_2d_depuis_wkt(
        wkt_path=str(wkt_path),
        code_insee=code_insee,
        inclure_ppri=True,
        ppri_table=f"{commune}.pm1_detaillee_gironde"
    )
    log_memory_detailed("CARTE_2D_GENERATED")

    html_2d_path = os.path.join(OUT_DIR, "carte_2d_unite_fonciere.html")
    with open(html_2d_path, "w", encoding="utf-8") as f:
        f.write(html_2d)
    log_memory_detailed("CARTE_2D_WRITTEN")
    del html_2d
    gc.collect()
    log_memory_detailed("CARTE_2D_FREED")
    log_memory("APRES_CARTE_2D")
    logger.info(f"✅ Carte 2D (nouvelle version) sauvegardée : {html_2d_path}")

    # --------------------------------------------------------
    # ÉTAPE 2 : CARTE 3D
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 2/5 : Génération de la visualisation 3D Plotly")
    path_3d = None
    metadata_3d = None
    url_3d = None
    if not skip_3d:
        res3d = exporter_visualisation_3d_plotly_from_wkt(
            wkt_path=wkt_path,
            output_dir=OUT_DIR,
            exaggeration=1.5
        )
        log_memory_detailed("CARTE_3D_GENERATED")
        path_3d = res3d["path"]
        metadata_3d = res3d.get("metadata")
        del res3d
        gc.collect()
        log_memory("APRES_CARTE_3D")
        logger.info(f"✅ Carte 3D générée : {path_3d}")
    else:
        logger.info("⏭️ Carte 3D désactivée (skip_3d=True)")

    # --------------------------------------------------------
    # ÉTAPE 3 : UPLOAD SUPABASE (cartes)
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 3/5 : Upload des cartes sur Supabase...")
    # ✅ Utiliser le slug comme répertoire distant unique
    remote_dir = slug
    remote_2d = f"{remote_dir}/carte_2d.html"
    remote_3d = f"{remote_dir}/carte_3d.html"

    url_2d = upload_to_supabase(html_2d_path, remote_2d)
    if path_3d and not skip_3d:
        url_3d = upload_to_supabase(path_3d, remote_3d)
    else:
        url_3d = None
    gc.collect()
    log_memory("APRES_UPLOADS")
    log_memory_detailed("UPLOADS_DONE")

    logger.info(f"🌐 URL publique 2D : {url_2d}")
    logger.info(f"🌐 URL publique 3D : {url_3d}")

    # --------------------------------------------------------
    # ÉTAPE 4 : CONSTRUCTION DU LIEN ENCODED + SHORTLINK
    # --------------------------------------------------------
    logger.info("\n📦 ÉTAPE 4/5 : Construction du lien encodé et du QR dynamique")
    payload = {"carte2d": url_2d, "carte3d": url_3d, "commune": commune}
    token = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    maps_page_url = f"{KERELIA_BASE_URL}?t={token}"

    # ✅ Utiliser le slug déjà généré (shortlinks dans public)
    logger.info(f"🧩 Insertion du shortlink dans public.shortlinks (slug={slug})...")
    try:
        response = supabase.schema("public").table("shortlinks").upsert({
            "slug": slug,
            "target_url": maps_page_url
        }).execute()
        logger.info(f"✅ Shortlink créé (status={getattr(response, 'status_code', '?')}) : https://kerelia.fr/m/{slug}")
        qr_url = f"https://kerelia.fr/m/{slug}"
    except Exception as e:
        logger.warning(f"⚠️ Erreur lors de la création du shortlink : {e}")
        qr_url = maps_page_url

    # ============================================================
    # 📄 ÉTAPE 5 : Génération du CUA DOCX
    # ============================================================
    logger.info("\n📦 ÉTAPE 5/5 : Génération du CUA DOCX avec QR dynamique")

    BASE_DIR = os.path.dirname(__file__)

    cerfa_path = os.path.join(OUT_DIR, "cerfa_result.json")
    intersections_files = list(Path(OUT_DIR).glob("rapport_intersections_*.json"))
    if intersections_files:
        intersections_path = str(max(intersections_files, key=lambda p: p.stat().st_mtime))
        logger.info(f"📑 Rapport d'intersections utilisé : {intersections_path}")
    else:
        raise FileNotFoundError(f"❌ Aucun rapport d'intersections trouvé dans {OUT_DIR}")

    # ------------------------------------------------------------
    # 📤 Upload du JSON d'intersections sur Supabase
    # ------------------------------------------------------------
    logger.info("📤 Upload du JSON d'intersections...")
    
    intersections_filename = Path(intersections_path).name
    remote_intersections_json = f"{remote_dir}/{intersections_filename}"
    
    intersections_json_url = upload_to_supabase(
        intersections_path,
        remote_intersections_json,
        bucket=SUPABASE_BUCKET
    )
    
    logger.info(f"🌐 URL publique JSON intersections : {intersections_json_url}")

    # Récupération des métadonnées CERFA
    cerfa_data = None
    cerfa_data_json = os.getenv("CERFA_DATA_JSON")
    if cerfa_data_json:
        try:
            cerfa_data = json.loads(cerfa_data_json)
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors du parsing de CERFA_DATA_JSON : {e}")

    # ============================================================
    # 📦 Export GPKG intersections
    # ============================================================
    logger.info("\n📦 Export GPKG intersections...")
    gpkg_path = os.path.join(OUT_DIR, "intersections.gpkg")

    intersections_gpkg_url = None
    if not skip_gpkg:
        export_gpkg_from_wkt(str(wkt_path), gpkg_path)
        log_memory_detailed("GPKG_GENERATED")
        gc.collect()
        log_memory("APRES_GPKG")

        remote_gpkg = f"{remote_dir}/intersections.gpkg"
        intersections_gpkg_url = upload_to_supabase(gpkg_path, remote_gpkg)
        logger.info(f"🌐 URL publique GPKG : {intersections_gpkg_url}")
    else:
        logger.info("⏭️ Export GPKG désactivé (skip_gpkg=True)")

    # Utiliser le catalogue avec geom_type
    catalogue_path = os.path.join(BASE_DIR, "..", "catalogues", "catalogue_intersections_tagged.json")
    output_docx_path = os.path.join(OUT_DIR, "CUA_unite_fonciere.docx")
    logo_latresne_path = os.path.join(BASE_DIR, "logos", "logo_latresne.png")
    logo_kerelia_path = os.path.join(BASE_DIR, "logos", "logo_kerelia.png")

    try:
        run_builder(
            cerfa_json=cerfa_path,
            intersections_json=intersections_path,
            catalogue_json=catalogue_path,
            output_path=output_docx_path,
            wkt_path=str(wkt_path),
            logo_first_page=logo_latresne_path,
            signature_logo=logo_kerelia_path,
            qr_url=qr_url,
            plu_nom="PLU de Latresne",
            plu_date_appro="13/02/2017"
        )
        logger.info("✅ CUA DOCX généré avec succès.")
    except Exception as e:
        logger.error(f"💥 Échec génération CUA DOCX : {e}")
        raise
    gc.collect()
    log_memory_detailed("DOCX_GENERATED")
    log_memory("APRES_DOCX")

    # ============================================================
    # 📤 UPLOAD FINAL DES ARTIFACTS (CUA uniquement)
    # ============================================================
    logger.info("\n📤 Upload final du CUA vers Supabase...")

    # Upload du CUA dans le bucket visualisation
    remote_cua = f"{remote_dir}/CUA_unite_fonciere.docx"
    cua_url = upload_to_supabase(output_docx_path, remote_cua, bucket=SUPABASE_BUCKET)
    logger.info(f"📎 CUA uploadé dans {SUPABASE_BUCKET} : {cua_url}")
    gc.collect()
    log_memory("APRES_UPLOAD_FINAL")
    log_memory_detailed("UPLOAD_FINAL_DONE")

    # ============================================================
    # 🔑 GÉNÉRATION DU TOKEN POUR L'URL /cua?t={token}
    # ============================================================
    logger.info("\n🔑 Génération du token pour l'URL /cua...")
    payload_cua = {
        "docx": remote_cua  # Chemin relatif dans le bucket visualisation
    }
    token_cua = base64.b64encode(json.dumps(payload_cua).encode()).decode()
    cua_viewer_url = f"https://kerelia.fr/cua?t={token_cua}"
    logger.info(f"✅ URL CUA générée : {cua_viewer_url}")

    # 🧠 L'upload du pipeline_result.json est désormais géré par CUA/orchestrator_global.py
    result_url = None

    # ============================================================
    # 📋 RÉSULTAT UNIFIÉ (pour l'API et le front)
    # ============================================================
    result = {
        "slug": slug,
        "commune": commune,
        "code_insee": code_insee,
        "maps_page": maps_page_url,
        "qr_url": qr_url,
        "carte_2d_url": url_2d,
        "carte_3d_url": url_3d,
        "intersections_gpkg_url": intersections_gpkg_url,
        "intersections_json": intersections_json_url,
        "output_cua": cua_url,
        "cua_viewer_url": cua_viewer_url,  # URL pour afficher le CUA en HTML
        "bucket_path": remote_dir,
        "pipeline_result_url": result_url,
        "metadata_2d": metadata_2d,
        "metadata_3d": metadata_3d,
        "cerfa_data": cerfa_data,  # Métadonnées CERFA
        "status": "success",
    }

    # ============================================================
    # 💾 ÉCRITURE DU FICHIER RÉSULTAT (lisible par l'API)
    # ============================================================
    sub_result_path = Path(OUT_DIR) / "sub_orchestrator_result.json"
    sub_result_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(f"💾 Résultat écrit dans : {sub_result_path}")

    # ============================================================
    # 🗄️ ENREGISTREMENT EN BASE (facultatif mais recommandé)
    # ============================================================
    # 🧑‍💼 Récupération des infos utilisateur depuis l'environnement
    user_id = os.getenv("USER_ID") or None
    user_email = os.getenv("USER_EMAIL") or None

    # 📌 Extraction des parcelles pour historisation (depuis cerfa_data si disponible)
    parcelles_for_history = None
    if cerfa_data:
        parcelles_for_history = (
            cerfa_data.get("references_cadastrales")
            or cerfa_data.get("parcelles")
            or None
        )

    # 📍 Calcul du centroïde de l'unité foncière pour l'historique carto
    centroid_for_history = compute_centroid_from_wkt_path(str(wkt_path))
    
    logger.info(f"🧩 Insertion pipeline dans latresne.pipelines (slug={slug})...")
    try:
        response = supabase.schema("latresne").table("pipelines").upsert({
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
            "user_id": user_id,
            "user_email": user_email,
            "cerfa_data": cerfa_data,
            "parcelles": parcelles_for_history,
            "centroid": centroid_for_history,
            "intersections_gpkg_url": intersections_gpkg_url,
            "intersections_json_url": intersections_json_url,
            "metadata": result,
            "suivi": 2,  # Dossier traité (étapes 1 et 2 validées automatiquement)
        }).execute()
        logger.info(f"✅ Pipeline enregistré dans latresne.pipelines (status={getattr(response, 'status_code', '?')})")
        if user_id:
            logger.info(f"👤 Pipeline associé à l'utilisateur : {user_email or user_id}")
    except Exception as e:
        logger.error(f"💥 Erreur d'insertion dans latresne.pipelines : {e}")

    # --------------------------------------------------------
    # FIN DU PIPELINE
    # --------------------------------------------------------
    logger.info("\n🎉 PIPELINE CUA TERMINÉ AVEC SUCCÈS 🎉")
    logger.info(f"📦 Slug unique : {slug}")
    logger.info(f"🔗 Lien court : {qr_url}")

    # Copie du fichier de logs mémoire dans /tmp pour Render / debug externe
    try:
        shutil.copy(log_file, "/tmp/memory_audit.log")
        logger.info("📁 memory_audit.log copié vers /tmp/memory_audit.log")
    except Exception as e:
        logger.warning(f"⚠️ Impossible de copier memory_audit.log vers /tmp : {e}")

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
