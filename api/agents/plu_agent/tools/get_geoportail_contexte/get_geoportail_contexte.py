"""
Tool get_geoportail_contexte — contexte PLU/SUP live (France entière) pour le LLM.

Pendant "France entière" de get_contexte_parcelle : au lieu de lire en BDD
(communes pré-traitées), fetch live le WFS Géoplateforme. Même format de sortie
pour compatibilité avec l'interface et les prompts existants.

Orchestration :
  1. resolve_unite_fonciere via WFS Parcellaire Express
  2. fetch des 8 couches EN PARALLÈLE (ThreadPoolExecutor, ~1s total)
  3. assemblage au format zones/prescriptions/servitudes/informations + counts
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import tiktoken
try:
    from google.genai import types
except Exception:  # pragma: no cover
    types = None

from .catalog import LAYERS
from .geoportail_core import build_layer_items, resolve_unite_fonciere

logger = logging.getLogger("geoportail")

MAX_WORKERS = 8
TIKTOKEN_ENCODING = "cl100k_base"


def _count_tokens_tiktoken(text: str, encoding_name: str = TIKTOKEN_ENCODING) -> int:
    """Compte exact des tokens via tiktoken pour le texte fourni."""
    if not text:
        return 0
    enc = tiktoken.get_encoding(encoding_name)
    return len(enc.encode(text))


def _context_metrics(payload: dict) -> dict:
    """
    Métriques utiles de taille contexte envoyé au LLM.
    """
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return {
        "payload_chars": len(raw),
        "payload_bytes_utf8": len(raw.encode("utf-8")),
        "payload_tokens_tiktoken": _count_tokens_tiktoken(raw),
        "payload_tokens_encoding": TIKTOKEN_ENCODING,
    }


def _to_builtin(value):
    """Convertit récursivement les types numpy/pandas en types Python natifs JSON-safe."""
    if isinstance(value, dict):
        return {k: _to_builtin(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_builtin(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_to_builtin(v) for v in value)
    # np scalar / pandas scalar: int32, int64, float32, bool_, etc.
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    # pandas Timestamp et objets similaires
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return value


def _empty_payload(error: str | None) -> dict:
    return {
        "zones": [], "zones_count": 0,
        "surfaciques": [], "lineaires": [], "ponctuelles": [], "prescriptions_count": 0,
        "servitudes": [], "servitudes_count": 0,
        "informations": {
            "surfaciques": [], "lineaires": [], "ponctuelles": [],
            "count": 0, "count_surfaciques": 0, "count_lineaires": 0, "count_ponctuelles": 0,
        },
        "informations_count": 0,
        "couches_supplementaires": {}, "couches_supplementaires_count": 0,
        "parcelles": [], "nb_parcelles": None, "superficie_unite_m2": None,
        "error": error,
    }


def get_geoportail_contexte(
    db_config: dict | None = None,   # ignoré (pas de BDD) — gardé pour cohérence de signature
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str = None,
    numero: str = None,
    idu: str = None,
    insee: str = None,
    buffer_m: float = 0.0,           # ignoré (intersection stricte) — gardé pour compat
) -> dict:
    """
    Contexte GPU live pour une parcelle / unité foncière, n'importe où en France.
    Format identique à get_contexte_parcelle.
    """
    try:
        session = requests.Session()

        resolved = resolve_unite_fonciere(
            parcelles=parcelles, idus=idus,
            section=section, numero=numero, idu=idu,
            insee=insee, session=session,
        )
        if resolved.get("error"):
            logger.warning("get_geoportail_contexte — %s", resolved["error"])
            return _empty_payload(resolved["error"])

        parcel_geom = resolved["geom_2154"]

        # Fetch des couches en parallèle
        results: dict[str, dict] = {}
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(build_layer_items, key, parcel_geom, session): key
                for key in LAYERS
            }
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    results[key] = fut.result()
                except Exception as e:
                    cfg = LAYERS[key]
                    if cfg.get("optional"):
                        results[key] = {"items": [], "count": 0, "error": None,
                                        "group": cfg["group"], "subgroup": cfg.get("subgroup")}
                    else:
                        results[key] = {"items": [], "count": 0, "error": str(e),
                                        "group": cfg["group"], "subgroup": cfg.get("subgroup")}

        # Si une couche non-optionnelle a une erreur dure, on la remonte
        hard_error = next(
            (r["error"] for k, r in results.items()
             if r.get("error") and not LAYERS[k].get("optional")),
            None,
        )

        # Assemblage au format get_contexte_parcelle
        zonage = results.get("zonage", {})
        zones = zonage.get("items", [])

        presc_surf = results.get("prescriptions_surf", {}).get("items", [])
        presc_lin = results.get("prescriptions_lin", {}).get("items", [])
        presc_pct = results.get("prescriptions_pct", {}).get("items", [])
        # kind ajouté pour cohérence avec l'ancien payload
        for it in presc_surf: it["kind"] = "surfacique"
        for it in presc_lin: it["kind"] = "lineaire"
        for it in presc_pct: it["kind"] = "ponctuelle"
        presc_count = len(presc_surf) + len(presc_lin) + len(presc_pct)

        servitudes = results.get("servitudes", {}).get("items", [])

        info_surf = results.get("infos_surf", {}).get("items", [])
        info_lin = results.get("infos_lin", {}).get("items", [])
        info_pct = results.get("infos_pct", {}).get("items", [])
        for it in info_surf: it["kind"] = "surfacique"
        for it in info_lin: it["kind"] = "lineaire"
        for it in info_pct: it["kind"] = "ponctuelle"
        info_count = len(info_surf) + len(info_lin) + len(info_pct)

        informations_block = {
            "surfaciques": info_surf,
            "lineaires": info_lin,
            "ponctuelles": info_pct,
            "count": info_count,
            "count_surfaciques": len(info_surf),
            "count_lineaires": len(info_lin),
            "count_ponctuelles": len(info_pct),
        }

        payload = {
            "zones": zones,
            "zones_count": len(zones),
            "surfaciques": presc_surf,
            "lineaires": presc_lin,
            "ponctuelles": presc_pct,
            "prescriptions_count": presc_count,
            "servitudes": servitudes,
            "servitudes_count": len(servitudes),
            "informations": informations_block,
            "informations_count": info_count,
            "couches_supplementaires": {},
            "couches_supplementaires_count": 0,
            "parcelles": resolved.get("parcelles") or [],
            "nb_parcelles": resolved.get("nb_parcelles"),
            "superficie_unite_m2": resolved.get("superficie_m2"),
            "error": hard_error,
        }
        payload = _to_builtin(payload)
        payload["llm_context_metrics"] = _context_metrics(payload)
        return payload

    except Exception as e:
        logger.exception("get_geoportail_contexte — erreur inattendue")
        payload = _to_builtin(_empty_payload(str(e)))
        payload["llm_context_metrics"] = _context_metrics(payload)
        return payload


# ---------- Déclaration Gemini ----------

def _parcel_tool_properties() -> dict:
    """Propriétés communes refs parcelles (identique à parcel_tool_properties existant)."""
    if types is None:
        return {}
    return {
        "parcelles": types.Schema(
            type=types.Type.ARRAY,
            description=(
                "Liste de parcelles cadastrales (section + numéro). "
                "Pour une unité foncière : toutes les parcelles contiguës du même ensemble."
            ),
            items=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "section": types.Schema(type=types.Type.STRING, description="Section cadastrale (ex: 'AC')."),
                    "numero": types.Schema(type=types.Type.STRING, description="Numéro de parcelle (ex: '8770')."),
                },
                required=["section", "numero"],
            ),
        ),
        "idus": types.Schema(
            type=types.Type.ARRAY,
            description="Liste d'IDU cadastraux (identifiant unique parcelle, ex: '660080000AB0001').",
            items=types.Schema(type=types.Type.STRING),
        ),
        "section": types.Schema(type=types.Type.STRING, description="Section cadastrale — une seule parcelle."),
        "numero": types.Schema(type=types.Type.STRING, description="Numéro de parcelle — une seule parcelle."),
        "idu": types.Schema(type=types.Type.STRING, description="IDU — une seule parcelle."),
        "insee": types.Schema(
            type=types.Type.STRING,
            description=(
                "Code INSEE de la commune (5 chiffres, ex: '66008'). FORTEMENT RECOMMANDÉ "
                "quand on fournit section+numero, pour éviter les collisions entre communes "
                "(une même section/numéro peut exister dans plusieurs communes)."
            ),
        ),
    }


if types is not None:
    DECL_GEOPORTAIL_CONTEXTE = types.FunctionDeclaration(
        name="get_geoportail_contexte",
        description=(
            "Retourne le contexte d'urbanisme (PLU/PLUi) et servitudes intersectant une parcelle "
            "ou une unité foncière, pour N'IMPORTE QUELLE commune de France métropolitaine, en "
            "interrogeant en direct le Géoportail de l'Urbanisme (données officielles à jour). "
            "Couvre : zonage PLU (avec % de couverture de la parcelle), prescriptions "
            "(surfaciques/linéaires/ponctuelles), servitudes d'utilité publique, et informations. "
            "À utiliser quand la commune N'EST PAS une commune pré-intégrée en base "
            "(sinon préférer get_contexte_parcelle qui est plus rapide et inclut les règlements). "
            "Recommandé de fournir 'insee' avec section+numero pour désambiguïser."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties=_parcel_tool_properties(),
        ),
    )
else:
    DECL_GEOPORTAIL_CONTEXTE = None