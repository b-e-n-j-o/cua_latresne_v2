#!/usr/bin/env python3
"""
Replay lot 0.6 — vrai avant/après : règlement injecté (prompt + tools PPRI/PPRMVT).

Ancien chemin : JOIN {schema}.plu_reglement / tables ppri_reglements, pprmvt_reglements.
Nouveau : get_zonage_et_reglements (corpus) + get_reglement_ppri / get_reglement_pprmvt.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[4]
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT))

from api.agents.plu_agent.commune_context import set_current_profile  # noqa: E402
from api.agents.plu_agent.communes.latresne import LATRESNE_PROFILE  # noqa: E402
from api.agents.plu_agent.tools.contexte_parcelle import get_contexte_parcelle  # noqa: E402
from api.agents.plu_agent.tools.reglement_ppri import get_reglement_ppri  # noqa: E402
from api.agents.plu_agent.tools.reglement_pprmvt import get_reglement_pprmvt  # noqa: E402
from api.agents.plu_agent.tools.utils.zonage import get_zonage_et_reglements  # noqa: E402
from api.agents.plu_agent._env import DB_CONFIG  # noqa: E402

OUT = ROOT / "sql" / "corpus" / "replay_avant_apres.md"

CIBLES = (
    "parcelle AI 287 AI 299",
    "zone agricole",
    "si je fais 4 terrains de paddle en interieur",
)


def _fp(text: str | None) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def _conn():
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        port=int(os.environ.get("SUPABASE_PORT", 5432)),
        dbname=os.environ["SUPABASE_DB"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
        sslmode="require",
        connect_timeout=15,
    )


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def trouver_cas(cur) -> list[dict]:
    cas = []
    for snippet in CIBLES:
        cur.execute(
            """
            SELECT m.id::text, m.session_id::text, m.content, m.created_at,
                   s.section, s.numero, s.idu, s.geojson, s.zones
            FROM latresne.plu_messages m
            JOIN latresne.plu_sessions s ON s.id = m.session_id
            WHERE m.role = 'user'
              AND m.content ILIKE %s
            ORDER BY m.created_at DESC
            LIMIT 1
            """,
            (f"%{snippet}%",),
        )
        row = cur.fetchone()
        if not row:
            cas.append({"snippet": snippet, "error": "message introuvable"})
            continue
        d = dict(row)
        d["snippet"] = snippet
        cas.append(d)
    return cas


def refs_session(row: dict) -> dict:
    geo = row.get("geojson")
    if isinstance(geo, str):
        try:
            geo = json.loads(geo)
        except json.JSONDecodeError:
            geo = None
    parcelles = (geo or {}).get("parcelles") if isinstance(geo, dict) else None
    idus = (geo or {}).get("idus") if isinstance(geo, dict) else None
    return {
        "section": row.get("section"),
        "numero": row.get("numero"),
        "idu": row.get("idu"),
        "parcelles": parcelles,
        "idus": idus,
    }


def ancien_zonage_plu(cur, refs: dict) -> list[dict]:
    """JOIN historique zonage_plu ↔ plu_reglement (chemin d'avant lot 0.6)."""
    cur.execute(
        """
        WITH cible AS (
            SELECT ST_Union(ST_MakeValid(geom_2154)) AS geom
            FROM latresne.parcelles
            WHERE (%(has_sn)s AND (section, lpad(numero, 4, '0')) IN (
                    SELECT w.sec, lpad(w.num, 4, '0')
                    FROM unnest(%(secs)s::text[], %(nums)s::text[]) AS w(sec, num)
                 ))
               OR (%(has_idu)s AND idu = ANY(%(idus)s))
        )
        SELECT
            z.zonage_reglement AS code_zone,
            z.libelle,
            ROUND(ST_Area(ST_Intersection(ST_MakeValid(z.geom_2154), c.geom))::numeric, 1)
                AS superficie_intersection_m2,
            ROUND(
                (ST_Area(ST_Intersection(ST_MakeValid(z.geom_2154), c.geom))
                 / NULLIF(ST_Area(c.geom), 0) * 100)::numeric,
                1
            ) AS pct_parcelle_couverte,
            r.reglementation
        FROM latresne.zonage_plu z
        CROSS JOIN cible c
        LEFT JOIN latresne.plu_reglement r ON r.code_zone = z.zonage_reglement
        WHERE ST_Intersects(ST_MakeValid(z.geom_2154), c.geom)
          AND ST_Area(ST_Intersection(ST_MakeValid(z.geom_2154), c.geom)) > 1.0
        ORDER BY superficie_intersection_m2 DESC
        """,
        {
            "has_sn": bool(refs.get("parcelles") or (refs.get("section") and refs.get("numero"))),
            "secs": [p["section"] for p in (refs.get("parcelles") or [])]
            or ([refs["section"]] if refs.get("section") else [""]),
            "nums": [str(p["numero"]) for p in (refs.get("parcelles") or [])]
            or ([str(refs["numero"])] if refs.get("numero") else [""]),
            "has_idu": bool(refs.get("idus") or refs.get("idu")),
            "idus": refs.get("idus") or ([refs["idu"]] if refs.get("idu") else [""]),
        },
    )
    return [dict(r) for r in cur.fetchall()]


def ancien_ppri(cur, codes: list[str]) -> dict[str, str]:
    wanted = ["DG"] + [c for c in codes if c and c != "DG"]
    cur.execute(
        """
        SELECT zone_code, reglementation
        FROM latresne.ppri_reglements
        WHERE upper(trim(zone_code)) = ANY(%s)
        """,
        ([c.upper() for c in wanted],),
    )
    return {
        (r["zone_code"] or "").upper(): (r["reglementation"] or "")
        for r in cur.fetchall()
    }


def ancien_pprmvt(cur, codes: list[str]) -> dict[str, str]:
    wanted = ["DG1", "DG2", "DG3"] + [c for c in codes if c and not str(c).upper().startswith("DG")]
    cur.execute(
        """
        SELECT code_zone, reglementation
        FROM latresne.pprmvt_reglements
        WHERE upper(trim(code_zone)) = ANY(%s)
        """,
        ([c.upper() for c in wanted],),
    )
    return {
        (r["code_zone"] or "").upper(): (r["reglementation"] or "")
        for r in cur.fetchall()
    }


def codes_extra(contexte: dict, layer_id_substr: str, attr: str) -> list[str]:
    extra = contexte.get("couches_supplementaires") or {}
    out: list[str] = []
    for group, items in extra.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            lid = str(item.get("layer_id") or item.get("couche") or "")
            if layer_id_substr.lower() not in lid.lower() and layer_id_substr.lower() not in str(group).lower():
                continue
            val = item.get(attr)
            if val and str(val) not in out:
                out.append(str(val))
    return out


def markdown_diff(titre: str, avant: str | None, apres: str | None) -> list[str]:
    a, b = avant or "", apres or ""
    lines = [f"#### {titre}\n"]
    if a == b:
        lines.append(f"identique — {_fp(a)} — {len(a)} caractères\n")
        return lines
    lines.append(
        f"**DIVERGE** avant={len(a)} car. `{_fp(a)}` / après={len(b)} car. `{_fp(b)}`\n"
    )
    if not a and b:
        lines.append("ancien vide, nouveau renseigné (cas alias UA typique).\n")
    elif a and not b:
        lines.append("**régression : nouveau vide**\n")
    return lines


def main() -> int:
    set_current_profile(LATRESNE_PROFILE)
    conn = _conn()
    lines = ["# Replay avant/après — règlement réellement injecté (lot 0.6)\n"]
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cas = trouver_cas(cur)
        for item in cas:
            lines.append(f"\n## « {item['snippet']} »\n")
            if item.get("error"):
                lines.append(f"{item['error']}\n")
                continue
            refs = refs_session(item)
            lines.append(
                f"session `{item['session_id']}` — refs `{json.dumps(refs, default=str)}`\n"
            )
            if not (refs.get("parcelles") or refs.get("idus") or (refs.get("section") and refs.get("numero"))):
                lines.append("Pas de parcelle en session : pas de préchargement à comparer.\n")
                continue

            ancien = ancien_zonage_plu(cur, refs)
            nouveau = get_zonage_et_reglements(
                DB_CONFIG,
                include_reglement=True,
                **{k: v for k, v in refs.items() if v},
            )
            if nouveau.get("error"):
                lines.append(f"Nouveau chemin erreur : {nouveau['error']}\n")
                continue
            nzones = nouveau.get("zones") or []
            lines.append(
                f"Zones PLU : ancien {len(ancien)} / nouveau {len(nzones)} "
                f"— {[z.get('code_zone') for z in nzones]}\n"
            )
            by_new = {}
            for z in nzones:
                by_new.setdefault(z.get("code_zone"), []).append(z)
            for z in ancien:
                code = z.get("code_zone")
                cand = (by_new.get(code) or [{}])[0]
                lines.extend(
                    markdown_diff(
                        f"PLU {code} ({z.get('pct_parcelle_couverte')}%)",
                        z.get("reglementation"),
                        cand.get("reglementation"),
                    )
                )

            ctx = get_contexte_parcelle(DB_CONFIG, **{k: v for k, v in refs.items() if v})
            ppri_codes = codes_extra(ctx, "pm1", "Code zone") or codes_extra(ctx, "pm1", "codezone")
            if not ppri_codes:
                ppri_codes = codes_extra(ctx, "ppri", "codezone")
            pprmvt_codes = codes_extra(ctx, "pprmvt", "Code zone") or codes_extra(
                ctx, "pprmvt", "codezone"
            )

            lines.append(f"\nCodes PPRI extra : {ppri_codes or '—'}\n")
            ppri_new = get_reglement_ppri(DB_CONFIG, ppri_codes or None)
            dg_ppri = (ppri_new.get("dispositions_generales") or [{}])[0]
            lines.append(
                f"get_reglement_ppri DG found={dg_ppri.get('found')} "
                f"texte_id={dg_ppri.get('texte_id')} "
                f"chars={len(dg_ppri.get('reglementation') or '')}\n"
            )
            ancien_p = ancien_ppri(cur, ppri_codes)
            lines.extend(
                markdown_diff(
                    "PPRI DG",
                    ancien_p.get("DG"),
                    dg_ppri.get("reglementation"),
                )
            )
            for z in ppri_new.get("zones") or []:
                code = (z.get("zone_code") or "").upper()
                lines.extend(
                    markdown_diff(
                        f"PPRI {code}",
                        ancien_p.get(code),
                        z.get("reglementation"),
                    )
                )

            lines.append(f"\nCodes PPRMVT extra : {pprmvt_codes or '—'}\n")
            mvt_new = get_reglement_pprmvt(DB_CONFIG, pprmvt_codes or None)
            ancien_m = ancien_pprmvt(cur, pprmvt_codes)
            for dg in mvt_new.get("dispositions_generales") or []:
                code = (dg.get("code_zone") or "").upper()
                lines.append(
                    f"get_reglement_pprmvt {code} found={dg.get('found')} "
                    f"texte_id={dg.get('texte_id')} "
                    f"chars={len(dg.get('reglementation') or '')}\n"
                )
                lines.extend(
                    markdown_diff(f"PPRMVT {code}", ancien_m.get(code), dg.get("reglementation"))
                )
            for z in mvt_new.get("zones") or []:
                code = (z.get("code_zone") or "").upper()
                lines.extend(
                    markdown_diff(
                        f"PPRMVT zone {code}",
                        ancien_m.get(code),
                        z.get("reglementation"),
                    )
                )

        OUT.write_text("".join(lines), encoding="utf-8")
        print("".join(lines))
        print(f"\nÉcrit : {OUT}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
