#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_precision_mnt_supabase.py
--------------------------------------
Audit de la précision horizontale et verticale du MNT Supabase :
- Résolution spatiale (mètres/pixel)
- Précision verticale (pas des valeurs NGF)
- Vérification du type de données (float16 / float32 / etc.)
"""

import os, tempfile, requests, numpy as np, rasterio, logging
from sqlalchemy import create_engine, text
from shapely import wkt
from shapely.geometry import mapping
from dotenv import load_dotenv

# ============================================================
# CONFIG
# ============================================================

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
log = logging.getLogger("audit_mnt")

DB_HOST = os.getenv("SUPABASE_HOST")
DB_USER = os.getenv("SUPABASE_USER")
DB_PASS = os.getenv("SUPABASE_PASSWORD")
DB_NAME = os.getenv("SUPABASE_DB")
DB_PORT = os.getenv("SUPABASE_PORT", "5432")
SUPABASE_KEY = os.getenv("SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
from sqlalchemy import create_engine
DB_ENGINE = create_engine(DATABASE_URL)

# ============================================================
# AUDIT
# ============================================================

def audit_mnt_precision(wkt_path):
    if not os.path.exists(wkt_path):
        raise FileNotFoundError("❌ Fichier WKT introuvable")

    geom = wkt.loads(open(wkt_path, "r", encoding="utf-8").read().strip())
    geom_wkt = geom.wkt

    # 🔍 Trouver les dalles correspondantes
    sql = """
    SELECT nom_fichier, storage_url
    FROM public.mnt_dalles
    WHERE ST_Intersects(emprise, ST_GeomFromText(:geom, 2154))
    ORDER BY nom_fichier;
    """
    with DB_ENGINE.connect() as conn:
        res = conn.execute(text(sql), {"geom": geom_wkt})
        dalles = [dict(row._mapping) for row in res]

    if not dalles:
        raise ValueError("❌ Aucune dalle MNT trouvée.")

    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}

    with tempfile.TemporaryDirectory() as tmpdir:
        for dalle in dalles:
            log.info(f"⬇️ Téléchargement : {dalle['nom_fichier']}")
            r = requests.get(dalle["storage_url"], headers=headers, timeout=60)
            r.raise_for_status()
            local_path = os.path.join(tmpdir, dalle["nom_fichier"])
            with open(local_path, "wb") as f:
                f.write(r.content)

            with rasterio.open(local_path) as src:
                rx, ry = src.res
                dtype = src.dtypes[0]
                nodata = src.nodata
                rows, cols = src.height, src.width
                crs = src.crs
                bounds = src.bounds

                data = src.read(1).astype("float32")
                if nodata is not None:
                    data = np.where(np.isclose(data, nodata), np.nan, data)
                valid = data[~np.isnan(data)]

                if valid.size == 0:
                    log.warning(f"⚠️ Dalle {dalle['nom_fichier']} : aucune valeur valide après filtrage")
                    continue

                # Quantification 1 cm pour estimer le pas vertical typique
                quant_cm = np.round(valid, 2)
                uniq = np.unique(quant_cm)
                if uniq.size > 1:
                    diffs_cm = np.diff(uniq)
                    pas_vertical_cm = float(np.quantile(diffs_cm, 0.5) * 100.0)
                else:
                    pas_vertical_cm = float("nan")

                # Taille du maillage 3D après sous-échantillonnage step
                step = max(1, min(rows, cols) // 200)
                npx_display = int((rows // step) * (cols // step))

                log.info(f"📍 Dalle : {dalle['nom_fichier']}")
                log.info(f"   ➡️ Résolution horizontale : {rx:.2f} m (X) × {ry:.2f} m (Y)")
                log.info(f"   ➡️ Pixels valides (analyse) : {valid.size:,}")
                log.info(f"   ➡️ Pas vertical typique (quantifié à 1 cm) : ~{pas_vertical_cm:.1f} cm")
                log.info(f"   ➡️ Maillage affichage 3D (step={step}) : ~{npx_display:,} points")
                log.info(f"   ➡️ Type de données : {dtype} | CRS : {crs}")
                log.info(f"   ➡️ Étendue : {bounds}")
                log.info(f"   ➡️ Altitudes : min={np.nanmin(valid):.2f} m | max={np.nanmax(valid):.2f} m\n")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Audit de précision du MNT Supabase (résolution horizontale + verticale)")
    parser.add_argument("--geom-wkt", required=True, help="Chemin du fichier WKT")
    args = parser.parse_args()

    audit_mnt_precision(args.geom_wkt)
