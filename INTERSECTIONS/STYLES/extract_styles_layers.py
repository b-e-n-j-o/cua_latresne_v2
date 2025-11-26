#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path
import shutil

def create_minimal_style_gpkg(source_gpkg, out_gpkg):
    source = Path(source_gpkg)
    dest = Path(out_gpkg)

    if not source.exists():
        raise FileNotFoundError(source)

    # Supprime l’ancien fichier minimal si existe
    if dest.exists():
        dest.unlink()

    # 1) Crée un GPKG vide en copiant la structure GeoPackage minimale
    # On utilise un GPKG neutre généré par ogr pour garantir les tables de base
    conn_dest = sqlite3.connect(dest)
    conn_dest.execute('''PRAGMA application_id=1196437808''')  # "GPKG"
    conn_dest.execute('''CREATE TABLE gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT
    );''')
    conn_dest.execute('''CREATE TABLE gpkg_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        data_type TEXT NOT NULL,
        identifier TEXT,
        description TEXT,
        last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
        min_x DOUBLE,
        min_y DOUBLE,
        max_x DOUBLE,
        max_y DOUBLE,
        srs_id INTEGER
    );''')
    conn_dest.commit()

    # 2) Ouvre le modèle complet
    conn_src = sqlite3.connect(source)

    # 3) Recréer la table layer_styles
    create_sql = conn_src.execute("""
        SELECT sql FROM sqlite_master
        WHERE type='table' AND name='layer_styles'
    """).fetchone()[0]

    conn_dest.execute(create_sql)

    # 4) Copier les styles
    rows = conn_src.execute("SELECT * FROM layer_styles").fetchall()
    cols = [c[1] for c in conn_src.execute("PRAGMA table_info(layer_styles)").fetchall()]
    qmarks = ", ".join("?" for _ in cols)

    conn_dest.executemany(
        f"INSERT INTO layer_styles ({', '.join(cols)}) VALUES ({qmarks})",
        rows
    )

    conn_dest.commit()

    conn_src.close()
    conn_dest.close()

    print(f"🎉 GPKG minimal créé : {dest} (styles = {len(rows)} lignes)")

if __name__ == "__main__":
    create_minimal_style_gpkg("OUTPUT_1.gpkg", "modele_styles_minimal.gpkg")
