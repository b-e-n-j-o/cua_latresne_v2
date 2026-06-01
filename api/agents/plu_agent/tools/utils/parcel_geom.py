"""Résolution géométrie — une ou plusieurs parcelles cadastrales contigues (unité foncière)."""

from __future__ import annotations

import json
import re
from typing import Any

import psycopg2
import psycopg2.extras
from google.genai import types

from ...commune_context import q


def _db_connect(db_config: dict):
    return psycopg2.connect(**db_config)


def _query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    conn = _db_connect(db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def normalize_parcel_refs(
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
) -> list[dict]:
    """Liste de refs {type: 'sn'|'idu', ...} sans doublon."""
    refs: list[dict] = []
    seen: set[str] = set()

    for p in parcelles or []:
        if not isinstance(p, dict):
            continue
        s = (p.get("section") or "").upper().strip()
        n = str(p.get("numero") or "").strip()
        if not s or not n:
            continue
        key = f"sn:{s}:{n.zfill(4)}"
        if key in seen:
            continue
        seen.add(key)
        refs.append({"type": "sn", "section": s, "numero": n})

    for i in idus or []:
        i_norm = str(i).strip().upper()
        if not i_norm:
            continue
        key = f"idu:{i_norm}"
        if key in seen:
            continue
        seen.add(key)
        refs.append({"type": "idu", "idu": i_norm})

    if section and numero:
        s = section.upper().strip()
        n = str(numero).strip()
        key = f"sn:{s}:{n.zfill(4)}"
        if key not in seen:
            seen.add(key)
            refs.append({"type": "sn", "section": s, "numero": n})

    if idu and not idus:
        i_norm = str(idu).strip().upper()
        key = f"idu:{i_norm}"
        if key not in seen:
            refs.append({"type": "idu", "idu": i_norm})

    return refs


def resolve_unite_fonciere(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
) -> dict:
    """
    Charge une ou plusieurs parcelles et retourne l'union EPSG:2154 si contiguës.

    Retour : geom_wkb, geojson_wgs84, superficie_m2, parcelles (métadonnées), error.
    """
    refs = normalize_parcel_refs(parcelles, idus, section, numero, idu)
    if not refs:
        return {"error": "Fournir parcelles, idus, ou section+numero."}

    idu_list = [r["idu"] for r in refs if r["type"] == "idu"]
    sn_sections = [r["section"] for r in refs if r["type"] == "sn"]
    sn_numeros = [r["numero"] for r in refs if r["type"] == "sn"]
    has_idu = bool(idu_list)
    has_sn = bool(sn_sections)

    sql_fetch = f"""
        SELECT idu, section, numero, contenance,
               ST_MakeValid(geom_2154) AS geom,
               ST_AsGeoJSON(ST_Transform(ST_MakeValid(geom_2154), 4326)) AS geojson_wgs84
        FROM {q("parcelles")}
        WHERE (%s AND idu = ANY(%s))
           OR (%s AND (section, lpad(numero, 4, '0')) IN (
                SELECT w.sec, lpad(w.num, 4, '0')
                FROM unnest(%s::text[], %s::text[]) AS w(sec, num)
           ))
    """
    rows = _query(
        db_config,
        sql_fetch,
        (
            has_idu,
            idu_list if has_idu else [""],
            has_sn,
            sn_sections if has_sn else [""],
            sn_numeros if has_sn else [""],
        ),
    )

    if not rows:
        return {"error": "Aucune parcelle trouvée pour les références fournies."}

    by_idu = {row["idu"]: row for row in rows}
    rows = list(by_idu.values())

    found_keys: set[str] = set()
    for row in rows:
        found_keys.add(f"idu:{row['idu']}")
        found_keys.add(
            f"sn:{row['section']}:{str(row['numero']).zfill(4)}"
        )

    missing = []
    for r in refs:
        if r["type"] == "idu" and f"idu:{r['idu']}" not in found_keys:
            missing.append(r["idu"])
        elif r["type"] == "sn":
            key = f"sn:{r['section']}:{r['numero'].zfill(4)}"
            if key not in found_keys:
                missing.append(f"{r['section']} {r['numero']}")

    if missing:
        return {
            "error": f"Parcelle(s) introuvable(s) : {', '.join(missing)}.",
        }

    if len(rows) != len(refs):
        return {
            "error": (
                "Certaines références pointent vers la même parcelle "
                "ou des parcelles distinctes n'ont pas été trouvées."
            ),
        }

    sql_union = f"""
        WITH geoms AS (
            SELECT ST_MakeValid(geom_2154) AS geom
            FROM {q("parcelles")}
            WHERE idu = ANY(%s)
        ),
        united AS (
            SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom FROM geoms
        )
        SELECT
            ST_AsEWKB(ST_MakeValid(geom)) AS geom_wkb,
            ST_AsGeoJSON(ST_Transform(ST_MakeValid(geom), 4326)) AS geojson_wgs84,
            ST_Area(ST_MakeValid(geom)) AS superficie_m2,
            ST_NumGeometries(ST_MakeValid(geom)) AS union_parts
        FROM united;
    """
    idus_found = [row["idu"] for row in rows]
    union_rows = _query(db_config, sql_union, (idus_found,))
    if not union_rows or union_rows[0]["geom_wkb"] is None:
        return {"error": "Impossible de construire la géométrie de l'unité foncière."}

    u = union_rows[0]
    parts = int(u.get("union_parts") or 0)
    if len(refs) > 1 and parts > 1:
        return {
            "error": (
                f"Les {len(refs)} parcelles ne sont pas contiguës "
                f"({parts} parties disjointes après union)."
            ),
        }

    parcelles_meta = [
        {
            "idu": row["idu"],
            "section": row["section"],
            "numero": row["numero"],
            "contenance": row.get("contenance"),
            "geojson_wgs84": row.get("geojson_wgs84"),
        }
        for row in rows
    ]

    return {
        "geom_wkb": u["geom_wkb"],
        "geojson_wgs84": u["geojson_wgs84"],
        "superficie_m2": float(u["superficie_m2"]) if u.get("superficie_m2") else None,
        "parcelles": parcelles_meta,
        "nb_parcelles": len(parcelles_meta),
        "error": None,
    }


_GEOJSON_GEOM_TYPES = frozenset({
    "Feature",
    "FeatureCollection",
    "Polygon",
    "MultiPolygon",
    "Point",
    "LineString",
    "MultiLineString",
    "MultiPoint",
    "GeometryCollection",
})

_PARCEL_ARG_KEYS = ("parcelles", "idus", "section", "numero", "idu")


def _refs_kwargs_from_normalized(refs: list[dict]) -> dict[str, Any]:
    """Arguments build_carto_payload / tools à partir de refs normalisées."""
    if not refs:
        return {}
    parcelles = [
        {"section": r["section"], "numero": r["numero"]}
        for r in refs
        if r["type"] == "sn"
    ]
    idus = [r["idu"] for r in refs if r["type"] == "idu"]
    out: dict[str, Any] = {}
    if len(refs) == 1:
        if refs[0]["type"] == "sn":
            out["section"] = refs[0]["section"]
            out["numero"] = refs[0]["numero"]
        else:
            out["idu"] = refs[0]["idu"]
    if parcelles:
        out["parcelles"] = parcelles
    if idus:
        out["idus"] = idus
    return out


def refs_kwargs_from_tool_args(args: dict | None) -> dict[str, Any]:
    """Extrait section/numero/parcelles/idus des args d'un tool Gemini."""
    if not isinstance(args, dict):
        return {}
    kw = {k: args[k] for k in _PARCEL_ARG_KEYS if args.get(k) is not None}
    refs = normalize_parcel_refs(
        kw.get("parcelles"),
        kw.get("idus"),
        kw.get("section"),
        kw.get("numero"),
        kw.get("idu"),
    )
    return _refs_kwargs_from_normalized(refs)


def refs_from_tool_calls(tool_calls: list[dict] | None) -> dict[str, Any]:
    """Dernier appel tool contenant des refs parcellaires (tour le plus récent en priorité)."""
    for tc in reversed(tool_calls or []):
        kw = refs_kwargs_from_tool_args(tc.get("args") if isinstance(tc, dict) else None)
        if kw:
            return kw
    return {}


def refs_from_user_text(text: str) -> dict[str, Any]:
    """
    Heuristique section/numéro ou IDU dans un message utilisateur
    (alignée sur le parseur frontend PluChat).
    """
    if not text or not str(text).strip():
        return {}

    idu_matches = re.findall(r"\b([0-9A-Z]{10}\d{4})\b", text, re.IGNORECASE)
    if len(idu_matches) > 1:
        return {"idus": [u.upper() for u in idu_matches]}
    if len(idu_matches) == 1:
        return {"idu": idu_matches[0].upper()}

    pairs: list[dict] = []
    seen: set[str] = set()
    for m in re.finditer(r"\b([A-Za-z]{1,2})\s+(\d{1,4})\b", text):
        section = m.group(1).upper()
        numero = m.group(2)
        key = f"{section}:{numero.zfill(4)}"
        if key in seen:
            continue
        seen.add(key)
        pairs.append({"section": section, "numero": numero})

    if len(pairs) >= 2:
        return {"parcelles": pairs}
    if len(pairs) == 1:
        return {"section": pairs[0]["section"], "numero": pairs[0]["numero"]}

    section_num = re.search(
        r"section\s+([A-Za-z]{1,2})\s+(?:n[°o]?\s*|num[ée]ro\s+)?(\d{1,4})\b",
        text,
        re.IGNORECASE,
    )
    if section_num:
        return {
            "section": section_num.group(1).upper(),
            "numero": section_num.group(2),
        }
    return {}


def refs_from_messages(messages: list[dict]) -> dict[str, Any]:
    """
    Refs depuis tool_calls persistés, sinon depuis le premier message utilisateur.
    """
    for msg in reversed(messages or []):
        if msg.get("role") != "model":
            continue
        kw = refs_from_tool_calls(msg.get("tool_calls"))
        if kw:
            return kw

    for msg in messages or []:
        if msg.get("role") != "user":
            continue
        content = msg.get("content")
        if not content:
            continue
        kw = refs_from_user_text(str(content))
        if kw:
            return kw
    return {}


def refs_from_session(session: dict) -> dict[str, Any]:
    """Reconstruit les arguments tools depuis une ligne plu_sessions."""
    raw = session.get("geojson")
    if raw:
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, dict):
                if data.get("parcelles") or data.get("idus"):
                    return {
                        "parcelles": data.get("parcelles") or None,
                        "idus": data.get("idus") or None,
                    }
                # Ancien format : vraie géométrie GeoJSON dans geojson — ignorer
                if data.get("type") in _GEOJSON_GEOM_TYPES:
                    pass
        except (json.JSONDecodeError, TypeError):
            pass

    if session.get("idu"):
        return {"idu": session["idu"]}
    if session.get("section") and session.get("numero"):
        return {
            "section": session["section"],
            "numero": session["numero"],
        }
    return {}


def resolve_session_refs(
    session: dict,
    messages: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Refs parcellaires pour la carto : colonnes session, puis historique (tools / texte).
    """
    refs = refs_from_session(session)
    if refs:
        return refs
    if messages:
        return refs_from_messages(messages)
    return {}


def parcelles_refs_to_json(
    parcelles: list[dict] | None,
    idus: list[str] | None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
) -> str | None:
    """Sérialise les refs pour la colonne geojson de plu_sessions (métadonnées, pas de géométrie)."""
    refs = normalize_parcel_refs(parcelles, idus, section, numero, idu)
    if not refs:
        return None
    payload: dict = {"parcelles": [], "idus": []}
    for r in refs:
        if r["type"] == "sn":
            payload["parcelles"].append({"section": r["section"], "numero": r["numero"]})
        else:
            payload["idus"].append(r["idu"])
    if not payload["parcelles"] and not payload["idus"]:
        return None
    return json.dumps(payload, ensure_ascii=False)


def parcel_tool_properties() -> dict[str, types.Schema]:
    """Propriétés communes des déclarations Gemini pour les refs parcelles."""
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
                    "section": types.Schema(
                        type=types.Type.STRING,
                        description="Section cadastrale (ex: 'AC').",
                    ),
                    "numero": types.Schema(
                        type=types.Type.STRING,
                        description="Numéro de parcelle (ex: '8770').",
                    ),
                },
                required=["section", "numero"],
            ),
        ),
        "idus": types.Schema(
            type=types.Type.ARRAY,
            description="Liste d'IDU cadastraux.",
            items=types.Schema(type=types.Type.STRING),
        ),
        "section": types.Schema(
            type=types.Type.STRING,
            description="Section cadastrale — une seule parcelle (équivalent à parcelles[0]).",
        ),
        "numero": types.Schema(
            type=types.Type.STRING,
            description="Numéro de parcelle — une seule parcelle.",
        ),
        "idu": types.Schema(
            type=types.Type.STRING,
            description="IDU — une seule parcelle.",
        ),
    }
