# CUA/map_2d.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Entrée principale pour générer une carte 2D (CLI ou orchestrateur).
"""

import os
import json
import argparse
from CUA.carte2d.carte2d_rendu import generer_carte_2d_depuis_wkt


def generate_map_for_orchestrator(wkt_path, output_dir="./out_2d", **kwargs):
    os.makedirs(output_dir, exist_ok=True)
    html_string, metadata = generer_carte_2d_depuis_wkt(wkt_path=wkt_path, **kwargs)
    output_path = os.path.join(output_dir, "carte_2d_unite_fonciere.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_string)
    return {
        "path": output_path,
        "filename": os.path.basename(output_path),
        "metadata": metadata,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère une carte Folium 2D à partir d'un fichier WKT.")
    parser.add_argument("--wkt", required=True, help="Chemin du fichier WKT contenant la géométrie")
    parser.add_argument("--code_insee", default="33234")
    parser.add_argument("--output", default="./out_2d")
    parser.add_argument("--ppri", action="store_true", default=True)
    args = parser.parse_args()

    res = generate_map_for_orchestrator(
        wkt_path=args.wkt,
        output_dir=args.output,
        code_insee=args.code_insee,
        inclure_ppri=args.ppri,
    )
    print(f"\n✅ Carte générée : {res['path']}")
    print(json.dumps(res["metadata"], indent=2, ensure_ascii=False))
