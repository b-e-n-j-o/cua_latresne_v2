#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path

PREFIX = "modele_styles — "   # le préfixe exact à retirer

def clean_layer_styles(gpkg_path):
    gpkg_path = Path(gpkg_path)
    if not gpkg_path.exists():
        raise FileNotFoundError(gpkg_path)

    print(f"📦 GPKG : {gpkg_path}")

    conn = sqlite3.connect(gpkg_path)
    cur = conn.cursor()

    # Vérifier présence de layer_styles
    cur.execute("""
        SELECT name 
        FROM sqlite_master 
        WHERE type='table' AND name='layer_styles'
    """)
    if cur.fetchone() is None:
        print("❌ Pas de table layer_styles !")
        return

    print("🔧 Nettoyage automatique des noms de couches…")

    # Récupérer les lignes actuelles
    cur.execute("SELECT rowid, f_table_name FROM layer_styles")
    rows = cur.fetchall()

    updated = 0

    for rowid, f_table_name in rows:
        if f_table_name.startswith(PREFIX):
            new_name = f_table_name[len(PREFIX):]  # retirer le préfixe
            cur.execute("""
                UPDATE layer_styles 
                SET f_table_name = ?
                WHERE rowid = ?
            """, (new_name, rowid))
            print(f"  • {f_table_name} → {new_name}")
            updated += 1

    conn.commit()
    conn.close()

    print(f"✅ Terminé ! {updated} noms nettoyés.")

if __name__ == "__main__":
    clean_layer_styles("OUTPUT.gpkg")
