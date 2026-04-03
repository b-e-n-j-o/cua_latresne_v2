# -*- coding: utf-8 -*-
"""
Tests : lien « carte web » dans le PDF (page de garde).

Le rendu n’affiche le lien cliquable que si `carte_web_url` commence par http:// ou https://
(voir `header._map_url_from_result`).
"""
from __future__ import annotations

import os
import json
import urllib.request
import urllib.error

import pytest


def test_map_url_from_result_requires_absolute_http():
    from api.identite_fonciere.pdf.header import _map_url_from_result

    assert (
        _map_url_from_result(
            {"carte_web_url": "https://www.example.com/api/identite-fonciere/public/if/if_abcd/carte.html"}
        )
        == "https://www.example.com/api/identite-fonciere/public/if/if_abcd/carte.html"
    )
    assert _map_url_from_result({"carte_web_url": "http://127.0.0.1:8000/api/identite-fonciere/map/view/xyz"}) is not None
    # Relatif ou vide → pas de lien dans le PDF
    assert _map_url_from_result({"carte_web_url": "/api/foo"}) is None
    assert _map_url_from_result({}) is None


@pytest.mark.skipif(
    not (os.getenv("API_BASE_URL") or os.getenv("IDENTITE_FONCIERE_TEST_API")),
    reason="Définir API_BASE_URL (ou IDENTITE_FONCIERE_TEST_API) pour tester l’API déployée",
)
def test_publier_api_returns_carte_url_https():
    """
    Appel HTTP réel vers POST /api/identite-fonciere/publier.

    Usage :
      API_BASE_URL=https://ton-api.onrender.com python3 -m pytest tests/test_identite_fonciere_carte_lien.py -v

    Si le host ne résout pas (DNS) ou l’API est hors ligne, le test est **skipped**, pas en erreur.
    """
    base = (os.getenv("API_BASE_URL") or os.getenv("IDENTITE_FONCIERE_TEST_API") or "").strip().rstrip("/")

    payload = {
        "commune": "Latresne",
        "insee": "33234",
        "srid": 4326,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-0.5073, 44.7895],
                    [-0.5048, 44.7895],
                    [-0.5048, 44.7910],
                    [-0.5073, 44.7910],
                    [-0.5073, 44.7895],
                ]
            ],
        },
        "intersections": [
            {
                "table": "plu_latresne",
                "display_name": "Zonage PLU",
                "elements": [
                    {
                        "zonage_reglement": "A",
                    }
                ],
            }
        ],
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/api/identite-fonciere/publier",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        pytest.fail(f"HTTP {e.code}: {err}")
    except (urllib.error.URLError, OSError) as e:
        # DNS introuvable, hors ligne, VPN, etc. — ne pas faire échouer la suite CI locale
        pytest.skip(
            f"API injoignable ({base}): {e}. "
            f"Vérifier l’URL (ex. sous-domaine api.* existant, ou http://127.0.0.1:8000 en local)."
        )

    assert body.get("success") is True, body
    cu = body.get("carte_url") or ""
    assert isinstance(cu, str) and cu.startswith("http"), f"carte_url inattendu: {cu!r}"
    # Réponse JSON : URL technique ; le PDF doit contenir la même logique (carte_web_url côté serveur)
    print("OK carte_url:", cu[:120], "..." if len(cu) > 120 else "")
