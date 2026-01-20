# cadastre_ign.py
import requests

WFS_URL = "https://data.geopf.fr/wfs"
CAD_LAYER = "CADASTRALPARCELS.PARCELLAIRE_EXPRESS:parcelle"

def fetch_parcelle_ign_2154(
    insee: str,
    section: str,
    numero: str,
    timeout: int = 20
) -> dict:
    """
    Retourne la feature GeoJSON IGN d'une parcelle
    - géométrie en EPSG:2154
    - lève une exception si non trouvée
    """
    section = section.upper().strip()
    numero = numero.zfill(4)

    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": CAD_LAYER,
        "outputFormat": "application/json",
        "srsName": "EPSG:2154",
        "cql_filter": (
            f"code_insee='{insee}' AND "
            f"section='{section}' AND "
            f"numero='{numero}'"
        )
    }

    r = requests.get(WFS_URL, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    if not data.get("features"):
        raise ValueError(
            f"Parcelle IGN introuvable : {insee} {section} {numero}"
        )

    return data["features"][0]

def test_zonage_plui_logic():
    insee = "33234"
    section = "AC"
    numero = "0242"
    parcelle = fetch_parcelle_ign_2154(insee, section, numero)
    print(parcelle)

test_zonage_plui_logic()