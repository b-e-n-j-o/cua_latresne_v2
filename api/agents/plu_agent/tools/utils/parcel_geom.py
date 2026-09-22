"""Résolution géométrie — une ou plusieurs parcelles cadastrales contigues (unité foncière)."""

from __future__ import annotations

import json
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
        refs.append({"type": "sn", "section": s, "numero": n.zfill(4)})

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
            refs.append({"type": "sn", "section": s, "numero": n.zfill(4)})

    if idu and not idus:
        i_norm = str(idu).strip().upper()
        key = f"idu:{i_norm}"
        if key not in seen:
            refs.append({"type": "idu", "idu": i_norm})

    return refs


def _fetch_parcel_rows(db_config: dict, refs: list[dict]) -> list[dict]:
    """Charge les feuilles cadastrales : section + numéro toujours comparés en SS NNNN."""
    if not refs:
        return []
    idu_list = [r["idu"] for r in refs if r["type"] == "idu"]
    sn_sections = [r["section"] for r in refs if r["type"] == "sn"]
    sn_numeros = [r["numero"].zfill(4) for r in refs if r["type"] == "sn"]
    has_idu = bool(idu_list)
    has_sn = bool(sn_sections)
    sql_fetch = f"""
        SELECT idu, section, numero, contenance,
               ST_MakeValid(geom_2154) AS geom,
               ST_AsGeoJSON(ST_Transform(ST_MakeValid(geom_2154), 4326)) AS geojson_wgs84
        FROM {q("parcelles")}
        WHERE (%s AND upper(trim(idu)) = ANY(%s))
           OR (%s AND (upper(trim(section)), lpad(trim(numero::text), 4, '0')) IN (
                SELECT upper(trim(w.sec)), lpad(trim(w.num), 4, '0')
                FROM unnest(%s::text[], %s::text[]) AS w(sec, num)
           ))
    """
    return _query(
        db_config,
        sql_fetch,
        (
            has_idu,
            [i.upper() for i in idu_list] if has_idu else [""],
            has_sn,
            sn_sections if has_sn else [""],
            sn_numeros if has_sn else [""],
        ),
    )


def lookup_parcel_refs(db_config: dict, refs: list[dict]) -> dict:
    """
    Résout chaque ref indépendamment (unité foncière multi-feuilles).

    Retour : found (lignes SQL), missing (refs), items (statut par feuille).
    """
    from .parcel_ref_parse import official_label

    items: list[dict] = []
    found: list[dict] = []
    missing: list[dict] = []
    if not refs:
        return {"found": found, "missing": missing, "items": items}

    rows = _fetch_parcel_rows(db_config, refs)
    by_key: dict[str, dict] = {}
    for row in rows:
        by_key[f"idu:{str(row['idu']).upper()}"] = row
        by_key[f"sn:{str(row['section']).upper().strip()}:{str(row['numero']).zfill(4)}"] = row

    seen_idu: set[str] = set()
    for r in refs:
        if r["type"] == "idu":
            row = by_key.get(f"idu:{r['idu']}")
            official = r["idu"]
            entry = {"type": "idu", "idu": r["idu"], "official": official, "found": bool(row)}
        else:
            official = official_label(r["section"], r["numero"])
            row = by_key.get(f"sn:{r['section']}:{r['numero'].zfill(4)}")
            entry = {
                "type": "sn",
                "section": r["section"],
                "numero": r["numero"].zfill(4),
                "official": official,
                "found": bool(row),
            }
        if row:
            if row["idu"] not in seen_idu:
                seen_idu.add(row["idu"])
                found.append(row)
            entry["idu"] = row["idu"]
            entry["contenance"] = row.get("contenance")
            items.append(entry)
        else:
            missing.append(r)
            items.append(entry)
    return {"found": found, "missing": missing, "items": items}


def found_rows_to_refs_kwargs(found: list[dict]) -> dict:
    parcelles = [
        {"section": row["section"], "numero": str(row["numero"]).zfill(4)}
        for row in found
    ]
    return _refs_kwargs_from_normalized(
        normalize_parcel_refs(parcelles=parcelles)
    )


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

    rows = _fetch_parcel_rows(db_config, refs)

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
    """Références cadastrales dans un message (parseur déterministe)."""
    from .parcel_ref_parse import parse_parcel_refs_from_text

    return parse_parcel_refs_from_text(text)


def merge_parcel_ref_sources(
    *,
    text: str | None = None,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
) -> dict[str, Any]:
    """Fusionne refs explicites et texte libre, puis normalise."""
    parsed = refs_from_user_text(text or "") if text else {}
    parsed_parcelles = list(parsed.get("parcelles") or [])
    if parsed.get("section") and parsed.get("numero"):
        parsed_parcelles.append(
            {"section": parsed["section"], "numero": parsed["numero"]}
        )
    parsed_idus = list(parsed.get("idus") or [])
    if parsed.get("idu"):
        parsed_idus.append(parsed["idu"])

    refs = normalize_parcel_refs(
        parcelles=(parcelles or []) + parsed_parcelles,
        idus=(idus or []) + parsed_idus,
        section=section,
        numero=numero,
        idu=idu,
    )
    return _refs_kwargs_from_normalized(refs)


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
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
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
