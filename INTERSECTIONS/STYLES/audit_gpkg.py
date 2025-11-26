#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
import sys
from pathlib import Path

def audit_gpkg(gpkg_path):
    p = Path(gpkg_path)
    if not p.exists():
        print(f"❌ Fichier introuvable : {gpkg_path}")
        return

    print(f"📦 Audit du GeoPackage : {p.resolve()}")

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Vérifier si la table existe
    cur.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name='layer_styles';
    """)
    row = cur.fetchone()

    if not row:
        print("⚠️ Aucune table 'layer_styles' trouvée dans ce GeoPackage.")
        conn.close()
        return

    print("✅ Table 'layer_styles' trouvée.")

    # Compter les lignes
    cur.execute("SELECT COUNT(*) FROM layer_styles;")
    count = cur.fetchone()[0]
    print(f"🔢 Nombre de styles enregistrés : {count}")

    # Afficher les noms de couches + style
    print("\n📋 Aperçu des styles (nom_couche, style_name) :")
    cur.execute("""
        SELECT f_table_name, styleName
        FROM layer_styles
        ORDER BY f_table_name;
    """)
    rows = cur.fetchall()

    for (layer, style) in rows:
        print(f"  • {layer} → {style}")

    conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage : python audit_layer_styles.py votre_geopackage.gpkg")
    else:
        audit_gpkg(sys.argv[1])
