# -*- coding: utf-8 -*-
"""
Service d'analyse RAA — multi-commune (schéma SQL + prompts via raa_config).

Fonction publique : analyser_raa(conn, raa_id, commune_slug, client=None)
"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile

import requests
from google import genai
from google.genai import types
from psycopg2.extras import Json

from .._env import GEMINI_API_KEY, GEMINI_MODEL
from .raa_config import RaaCommuneConfig, get_raa_config, normalise_arrete_nature

HEADERS = {"User-Agent": "Mozilla/5.0 (Kerelia veille RAA)"}
DOWNLOAD_TIMEOUT = 120
INLINE_PDF_MAX_BYTES = 50 * 1024 * 1024  # limite Gemini inline (Vertex + Developer)

PRIX_IN = 0.25
PRIX_OUT = 1.50


def get_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(vertexai=True, api_key=GEMINI_API_KEY)
    return genai.Client(vertexai=True)


def _download_pdf(url: str) -> pathlib.Path:
    r = requests.get(url, headers=HEADERS, timeout=DOWNLOAD_TIMEOUT, stream=True)
    r.raise_for_status()
    fd, tmp = tempfile.mkstemp(suffix=".pdf")
    with os.fdopen(fd, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)
    return pathlib.Path(tmp)


def _parse_gemini_json(raw: str) -> dict:
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.lower().startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


def _cout(tokens_in: int, tokens_out: int) -> float:
    return round((tokens_in / 1_000_000) * PRIX_IN + (tokens_out / 1_000_000) * PRIX_OUT, 6)


def _call_gemini(client: genai.Client, pdf_path: pathlib.Path, cfg: RaaCommuneConfig) -> dict:
    """
    Analyse un PDF via generate_content (compatible Vertex AI).
    L'API Files (upload) n'est disponible qu'avec le client Gemini Developer.
    """
    pdf_bytes = pdf_path.read_bytes()
    if len(pdf_bytes) > INLINE_PDF_MAX_BYTES:
        mo = len(pdf_bytes) / (1024 * 1024)
        raise ValueError(f"PDF trop volumineux ({mo:.1f} Mo, max 50 Mo en inline)")

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=[
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
            cfg.analyse_prompt,
        ],
        config=types.GenerateContentConfig(
            system_instruction=cfg.system_prompt,
            temperature=0.1,
        ),
    )

    usage = response.usage_metadata
    tokens_in = usage.prompt_token_count if usage else 0
    tokens_out = usage.candidates_token_count if usage else 0
    analyse = _parse_gemini_json(response.text or "")
    analyse = _normalise_analyse(analyse)

    return {"analyse": analyse, "tokens_in": tokens_in, "tokens_out": tokens_out}


def _normalise_analyse(analyse: dict) -> dict:
    """Harmonise les clés Gemini (ex. latresne_mentionnee → commune_mentionnee, nature)."""
    if "commune_mentionnee" not in analyse:
        for key in ("latresne_mentionnee", "argeles_mentionnee"):
            if key in analyse:
                analyse["commune_mentionnee"] = analyse[key]
                break
    arretes = analyse.get("arretes")
    if isinstance(arretes, list):
        for a in arretes:
            if isinstance(a, dict):
                raw = a.get("nature") or a.get("classification") or a.get("categorie")
                a["nature"] = normalise_arrete_nature(raw)
    return analyse


def _insert_analyse_sql(schema: str) -> str:
    return f"""
    INSERT INTO {schema}.raa_analyse
        (raa_id, modele, niveau_alerte, nb_arretes_total, nb_arretes_pertinents,
         commune_mentionnee, resume_global, arretes, tokens_in, tokens_out,
         cout_estime, erreur)
    VALUES
        (%(raa_id)s, %(modele)s, %(niveau_alerte)s, %(nb_total)s, %(nb_pert)s,
         %(commune_mentionnee)s, %(resume_global)s, %(arretes)s, %(tokens_in)s,
         %(tokens_out)s, %(cout)s, %(erreur)s)
    RETURNING id, created_at;
"""


def _update_statut_sql(schema: str, *, avec_vu: bool) -> str:
    if avec_vu:
        return (
            f"UPDATE {schema}.raa SET statut=%s, vu=false, updated_at=now() WHERE id=%s;"
        )
    return f"UPDATE {schema}.raa SET statut=%s, updated_at=now() WHERE id=%s;"


def _enregistrer(conn, result: dict, schema: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            _insert_analyse_sql(schema),
            {
                "raa_id": result["raa_id"],
                "modele": result["modele"],
                "niveau_alerte": result.get("niveau_alerte"),
                "nb_total": result.get("nb_arretes_total"),
                "nb_pert": result.get("nb_arretes_pertinents"),
                "commune_mentionnee": result.get("commune_mentionnee"),
                "resume_global": result.get("resume_global"),
                "arretes": Json(result.get("arretes") or []),
                "tokens_in": result["tokens_in"],
                "tokens_out": result["tokens_out"],
                "cout": result["cout_estime"],
                "erreur": result.get("erreur"),
            },
        )
        analyse_id, created_at = cur.fetchone()
        cur.execute(
            _update_statut_sql(schema, avec_vu=(result["statut"] == "analyse")),
            (result["statut"], result["raa_id"]),
        )
    conn.commit()
    result["analyse_id"] = analyse_id
    result["created_at"] = created_at
    return result


def _build_result(
    raa_id, pdf_url, titre, date_publication,
    analyse, tokens_in, tokens_out, erreur, statut,
) -> dict:
    cout = _cout(tokens_in, tokens_out)
    return {
        "raa_id": raa_id,
        "pdf_url": pdf_url,
        "titre": titre,
        "date_publication": date_publication,
        "modele": GEMINI_MODEL,
        "statut": statut,
        "niveau_alerte": (analyse or {}).get("niveau_alerte"),
        "nb_arretes_total": (analyse or {}).get("nb_arretes_total"),
        "nb_arretes_pertinents": (analyse or {}).get("nb_arretes_pertinents"),
        "commune_mentionnee": (analyse or {}).get("commune_mentionnee"),
        "resume_global": (analyse or {}).get("resume_global"),
        "arretes": (analyse or {}).get("arretes", []),
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "cout_estime": cout,
        "erreur": erreur,
    }


def analyser_raa(
    conn,
    raa_id: int,
    commune_slug: str,
    client: genai.Client | None = None,
    persist: bool = True,
) -> dict:
    """
    Analyse le RAA `raa_id` pour la commune `commune_slug`.
    Ne lève jamais : en cas d'échec, retourne un dict avec `erreur` et statut='erreur'.
    """
    cfg = get_raa_config(commune_slug)
    if not cfg:
        raise ValueError(f"Commune RAA inconnue : {commune_slug}")

    client = client or get_client()
    schema = cfg.schema

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id, pdf_url, titre, date_publication FROM {schema}.raa WHERE id=%s;",
            (raa_id,),
        )
        row = cur.fetchone()
    if not row:
        raise ValueError(f"RAA #{raa_id} introuvable ({schema}.raa)")

    _, pdf_url, titre, date_publication = row
    pdf_path = None
    try:
        pdf_path = _download_pdf(pdf_url)
        res = _call_gemini(client, pdf_path, cfg)
        result = _build_result(
            raa_id=raa_id,
            pdf_url=pdf_url,
            titre=titre,
            date_publication=date_publication,
            analyse=res["analyse"],
            tokens_in=res["tokens_in"],
            tokens_out=res["tokens_out"],
            erreur=None,
            statut="analyse",
        )
        if persist:
            result = _enregistrer(conn, result, schema)
        return result
    except Exception as e:
        result = _build_result(
            raa_id=raa_id,
            pdf_url=pdf_url,
            titre=titre,
            date_publication=date_publication,
            analyse=None,
            tokens_in=0,
            tokens_out=0,
            erreur=f"{type(e).__name__}: {e}",
            statut="erreur",
        )
        if persist:
            result = _enregistrer(conn, result, schema)
        return result
    finally:
        if pdf_path and pdf_path.exists():
            pdf_path.unlink()
