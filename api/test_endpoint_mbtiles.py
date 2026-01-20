#!/usr/bin/env python
import requests
from pathlib import Path


def test_parcelle_tile(
    base_url: str,
    code_insee: str,
    z: int,
    x: int,
    y: int,
    output: Path | None = None,
):
    url = f"{base_url.rstrip('/')}/tiles/parcelles/{code_insee}/{z}/{x}/{y}.mvt"
    print(f"→ Requête : {url}")

    resp = requests.get(url)
    print(f"Statut HTTP : {resp.status_code}")

    if resp.status_code == 204:
        print("Aucune tuile pour ces paramètres (204 No Content).")
        return

    if not resp.ok:
        print(f"Erreur : {resp.status_code} - {resp.text[:200]}")
        return

    print("Headers :")
    for k, v in resp.headers.items():
        print(f"  {k}: {v}")

    data = resp.content
    print(f"Taille de la tuile : {len(data)} octets")

    if output:
        output = Path(output)
        output.write_bytes(data)
        print(f"Tuile enregistrée dans : {output.resolve()}")


if __name__ == "__main__":
    # Paramètres en dur comme demandé
    BASE_URL = "http://localhost:8000"
    CODE_INSEE = "33063"
    Z = 15
    X = 16331
    Y = 11808

    # Si tu veux sauvegarder la tuile, mets un chemin ici, sinon laisse None
    OUTPUT = None  # exemple: Path("tuile_33063_15_16331_11808.mvt")

    test_parcelle_tile(
        base_url=BASE_URL,
        code_insee=CODE_INSEE,
        z=Z,
        x=X,
        y=Y,
        output=OUTPUT,
    )