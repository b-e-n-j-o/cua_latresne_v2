#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import sys
from pathlib import Path

def replace_layer_styles(gpkg_path):
    p = Path(gpkg_path)
    if not p.exists():
        print(f"❌ Fichier introuvable : {gpkg_path}")
        return

    print(f"📦 Modification du GeoPackage : {p.resolve()}")

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Vérifier la présence de table layerstyle
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='layerstyle';")
    if not cur.fetchone():
        print("❌ Aucune table 'layerstyle' trouvée dans ce GPKG.")
        conn.close()
        return

    print("🔍 Table 'layerstyle' trouvée")

    # Vérifier s'il y a déjà layer_styles
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='layer_styles';")
    if cur.fetchone():
        print("⚠️ Table 'layer_styles' existante → suppression…")
        cur.execute("DROP TABLE layer_styles;")
        conn.commit()

    # Renommer table
    print("✏️ Renommage 'layerstyle' → 'layer_styles'")
    cur.execute("ALTER TABLE layerstyle RENAME TO layer_styles;")
    conn.commit()

    # Vérifier résultat
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='layer_styles';")
    if cur.fetchone():
        print("✅ Renommage réussi.")
    else:
        print("❌ Erreur lors du renommage.")

    conn.close()
    print("🎉 Terminé !")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage : python replace_style_table.py votre_geopackage.gpkg")
    else:
        replace_layer_styles(sys.argv[1])
