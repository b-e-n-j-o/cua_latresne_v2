"""
Lectures corpus.textes — projection réglementaire transverse (lot 0).

Ne pas appeler ``textes_pour_zonages()`` depuis get_reglement_zone :
cette fonction agrège tous les types de documents et toutes les DG.
Ici chaque appel est filtré par ``document_type``.
Le zonage de session / get_contexte_parcelle n'attache plus le markdown PLU.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
import psycopg2.extras

try:
    from .commune_context import current_schema, get_current_profile_optional
except ImportError:
    from commune_context import current_schema, get_current_profile_optional

logger = logging.getLogger("plu_tools")

INSEE_PAR_SCHEMA = {
    "argeles": "66008",
    "latresne": "33234",
}

DOCUMENT_PLU = ("PLU", "PLUI")


def db_query(db_config: dict, sql: str, params: tuple) -> list[dict]:
    """Connexion courte — évite d'importer tools (import circulaire)."""
    conn = psycopg2.connect(**db_config)
    with conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def insee_courant() -> str | None:
    """INSEE du profil HTTP, sinon déduit du schéma SQL actif."""
    profile = get_current_profile_optional()
    if profile and profile.insee:
        return str(profile.insee).strip()
    schema = current_schema()
    return INSEE_PAR_SCHEMA.get(schema)


def _norm(code: str | None) -> str:
    return str(code or "").strip()


def resoudre_codes_zonage(
    db_config: dict,
    codes: list[str],
    *,
    document_type: str,
    insee: str | None = None,
) -> list[str]:
    """Codes spatiaux → codes texte (table alias). Sans alias, le code est conservé."""
    insee = insee or insee_courant()
    demandes = [_norm(c) for c in codes if _norm(c)]
    if not demandes or not insee:
        return demandes
    try:
        rows = db_query(
            db_config,
            "SELECT corpus.resoudre_codes_zonage(%s, %s, %s::text[]) AS codes",
            (insee, document_type, demandes),
        )
    except Exception as e:
        logger.error("resoudre_codes_zonage — SQL échoué : %s", e)
        return demandes
    if not rows:
        return demandes
    out = [c for c in (rows[0].get("codes") or []) if c]
    return out or demandes


def fetch_textes(
    db_config: dict,
    *,
    document_type: str,
    codes: list[str] | None = None,
    include_globale: bool = False,
    insee: str | None = None,
) -> list[dict[str, Any]]:
    """
    Lignes ``corpus.textes`` d'un type de document.

    ``include_globale`` : ajoute portee='globale' / zone_code NULL du même type
    (DG PPRI, DG1–3 PPRMVT, etc.) — jamais les DG des autres documents.
    """
    insee = insee or insee_courant()
    if not insee:
        return []

    codes_norm = [_norm(c) for c in (codes or []) if _norm(c)]
    if not codes_norm and not include_globale:
        return []

    clauses = [
        "(t.commune_insee = %s OR t.commune_insee IS NULL)",
        "t.document_type = %s",
    ]
    params: list[Any] = [insee, document_type]
    or_parts: list[str] = []
    if codes_norm:
        or_parts.append("t.zone_code = ANY(%s)")
        params.append(codes_norm)
    if include_globale:
        or_parts.append("(t.portee = 'globale' OR t.zone_code IS NULL)")
    if not or_parts:
        return []
    clauses.append("(" + " OR ".join(or_parts) + ")")

    sql = f"""
        SELECT DISTINCT ON (
            t.document_type,
            coalesce(t.zone_code, ''),
            coalesce(t.chapitre, '')
        )
            t.id,
            t.commune_insee,
            t.document_type,
            t.zone_code,
            t.chapitre,
            t.titre,
            t.type_piece,
            t.contenu_markdown,
            t.resume,
            t.portee,
            t.rang_normatif,
            t.localisation_source,
            t.date_debut_applicabilite
        FROM corpus.textes t
        WHERE {' AND '.join(clauses)}
        ORDER BY t.document_type,
                 coalesce(t.zone_code, ''),
                 coalesce(t.chapitre, ''),
                 t.commune_insee NULLS LAST,
                 t.date_debut_applicabilite DESC NULLS LAST
    """
    try:
        rows = db_query(db_config, sql, tuple(params))
    except Exception as e:
        logger.error("fetch_textes(%s) — SQL échoué : %s", document_type, e)
        raise

    return [_row_public(r) for r in rows]


def _row_public(row: dict[str, Any]) -> dict[str, Any]:
    loc = row.get("localisation_source")
    if not isinstance(loc, dict):
        loc = {}
    date_deb = row.get("date_debut_applicabilite")
    return {
        "texte_id": str(row["id"]) if row.get("id") else None,
        "commune_insee": row.get("commune_insee"),
        "document_type": row.get("document_type"),
        "zone_code": row.get("zone_code"),
        "chapitre": row.get("chapitre"),
        "titre": row.get("titre"),
        "type_piece": row.get("type_piece"),
        "reglementation": (row.get("contenu_markdown") or "").strip() or None,
        "resume": row.get("resume"),
        "portee": row.get("portee"),
        "rang_normatif": row.get("rang_normatif"),
        "page_start": loc.get("page_start"),
        "page_end": loc.get("page_end"),
        "date_debut_applicabilite": date_deb.isoformat() if date_deb else None,
    }


def concatener_reglements(textes: list[dict[str, Any]]) -> str | None:
    parts: list[str] = []
    for t in textes:
        body = (t.get("reglementation") or "").strip()
        if not body:
            continue
        code = t.get("zone_code") or ""
        titre = t.get("titre") or t.get("chapitre") or ""
        if len(textes) > 1:
            header = " — ".join(p for p in (code, titre) if p)
            parts.append(f"### {header}\n\n{body}" if header else body)
        else:
            parts.append(body)
    if not parts:
        return None
    return "\n\n---\n\n".join(parts)


def _alias_par_code(
    db_config: dict,
    codes: list[str],
    *,
    document_type: str,
    insee: str,
) -> dict[str, list[str]]:
    """Un seul SELECT pour tout le lot de codes spatiaux (évite N allers-retours)."""
    mapping: dict[str, list[str]] = {c: [] for c in codes}
    if not codes:
        return mapping
    rows = db_query(
        db_config,
        """
        SELECT a.code_spatial, a.code_texte
        FROM corpus.alias_zonage a
        WHERE a.commune_insee = %s
          AND a.document_type = %s
          AND upper(a.code_spatial) = ANY(%s)
        """,
        (insee, document_type, [c.upper() for c in codes]),
    )
    by_upper: dict[str, list[str]] = {}
    for row in rows:
        key = str(row["code_spatial"] or "").upper()
        texte = _norm(row.get("code_texte"))
        if texte:
            by_upper.setdefault(key, []).append(texte)
    for spatial in codes:
        alias = by_upper.get(spatial.upper())
        mapping[spatial] = list(alias) if alias else [spatial]
    return mapping


def attacher_textes_plu(
    db_config: dict,
    rows: list[dict[str, Any]],
    *,
    insee: str | None = None,
) -> list[dict[str, Any]]:
    """Enrichit les lignes de zonage spatial (code GPU) avec corpus PLU + alias."""
    insee = insee or insee_courant()
    codes = []
    seen: set[str] = set()
    for row in rows:
        c = _norm(row.get("code_zone"))
        if c and c not in seen:
            seen.add(c)
            codes.append(c)
    if not codes or not insee:
        for row in rows:
            row.setdefault("texte_id", None)
            row.setdefault("texte_ids", [])
        return rows

    resolus_par_spatial = _alias_par_code(
        db_config, codes, document_type="PLU", insee=insee
    )
    tous_resolus: list[str] = []
    for resolus in resolus_par_spatial.values():
        for r in resolus:
            if r not in tous_resolus:
                tous_resolus.append(r)

    fetched = fetch_textes(
        db_config,
        document_type="PLU",
        codes=tous_resolus,
        include_globale=False,
        insee=insee,
    )
    by_code: dict[str, list[dict[str, Any]]] = {}
    for t in fetched:
        zc = _norm(t.get("zone_code"))
        by_code.setdefault(zc, []).append(t)

    for row in rows:
        spatial = _norm(row.get("code_zone"))
        textes: list[dict[str, Any]] = []
        for code in resolus_par_spatial.get(spatial, [spatial]):
            textes.extend(by_code.get(code, []))
        ids = [t["texte_id"] for t in textes if t.get("texte_id")]
        row["reglementation"] = concatener_reglements(textes)
        row["texte_id"] = ids[0] if ids else None
        row["texte_ids"] = ids
        row["codes_texte"] = [_norm(t.get("zone_code")) for t in textes]
    return rows
