# -*- coding: utf-8 -*-
"""Suivi interne CUA + chat LLM — agrégats multi-communes (superadmin)."""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import text

from api.agents.plu_agent.communes import COMMUNE_PROFILES
from api.cuas.argeles.db import PIPELINES_SCHEMA, get_engine, logger

router = APIRouter(prefix="/admin/monitoring", tags=["admin-monitoring"])

_PARIS = ZoneInfo("Europe/Paris")
_PROJECT_URL_BASE = "https://www.kerelia.fr"
_RECENT_LIMIT = 40
_DAY_WINDOW = 30

_CUA_LABELS = {
    "argeles": "Argelès-sur-Mer",
    "latresne": "Latresne",
    "mios": "Mios",
    "france": "France",
}

_ROLE_LABELS = {
    "user": "Utilisateur",
    "admin_commune": "Admin commune",
    "superadmin": "Superadmin",
}


def _project_url(commune_slug: str | None, slug: str | None) -> str | None:
    commune = (commune_slug or "").strip().lower()
    project = (slug or "").strip()
    if not commune or not project or commune == "?":
        return None
    return f"{_PROJECT_URL_BASE}/{commune}/cua/projects/{project}"


def _chat_url(commune_slug: str | None) -> str | None:
    commune = (commune_slug or "").strip().lower()
    if not commune:
        return None
    return f"{_PROJECT_URL_BASE}/{commune}/chat"


def _safe_ident(name: str) -> str:
    cleaned = (name or "").strip().lower()
    if not cleaned.replace("_", "").isalnum():
        raise ValueError(f"Identifiant SQL invalide : {name!r}")
    return cleaned


def _rollback(conn) -> None:
    """Postgres refuse toute requête après une erreur SQL tant que la tx n'est pas rollback."""
    try:
        conn.rollback()
    except Exception:
        pass


def _table_exists(conn, schema: str, table: str) -> bool:
    try:
        return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = :schema AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table},
            ).scalar()
        )
    except Exception as exc:
        logger.warning("Monitoring table_exists %s.%s : %s", schema, table, exc)
        _rollback(conn)
        return False


def _fetch_rows(conn, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]] | None:
    try:
        result = conn.execute(text(sql), params or {})
        return [dict(r) for r in result.mappings().all()]
    except Exception as exc:
        logger.warning("Monitoring SQL : %s — %s", exc, " ".join(sql.split()))
        _rollback(conn)
        return None


def _fetch_one(conn, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        row = conn.execute(text(sql), params or {}).mappings().first()
        return dict(row) if row else None
    except Exception as exc:
        logger.warning("Monitoring SQL : %s — %s", exc, " ".join(sql.split()))
        _rollback(conn)
        return None


def _format_parcelles(parcelles: Any) -> tuple[int, str]:
    if not parcelles:
        return 0, "—"
    if isinstance(parcelles, str):
        return 0, parcelles[:120]
    if not isinstance(parcelles, list):
        return 0, "—"
    labels = []
    for item in parcelles:
        if isinstance(item, dict):
            section = str(item.get("section") or "").strip().upper()
            numero = str(item.get("numero") or "").strip()
            if section and numero:
                labels.append(f"{section} {numero}")
        elif isinstance(item, str) and item.strip():
            labels.append(item.strip())
    return len(labels), " · ".join(labels) if labels else "—"


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _day_key(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(_PARIS).date().isoformat()
    text_value = str(value)
    return text_value[:10] if len(text_value) >= 10 else None


def _empty_days(n: int = _DAY_WINDOW) -> list[dict[str, Any]]:
    today = datetime.now(_PARIS).date()
    return [
        {"day": (today - timedelta(days=n - 1 - i)).isoformat(), "count": 0}
        for i in range(n)
    ]


def _fill_days(counts: dict[str, int]) -> list[dict[str, Any]]:
    series = _empty_days()
    for row in series:
        row["count"] = int(counts.get(row["day"], 0))
    return series


def _pipelines_ident(conn) -> str | None:
    schema = _safe_ident(PIPELINES_SCHEMA or "public")
    if _table_exists(conn, schema, "pipelines"):
        return f"{schema}.pipelines"
    if schema != "latresne" and _table_exists(conn, "latresne", "pipelines"):
        return "latresne.pipelines"
    return None


def _load_cua(conn) -> dict[str, Any]:
    ident = _pipelines_ident(conn)
    empty = {
        "total": 0,
        "success": 0,
        "error": 0,
        "last_7d": 0,
        "last_30d": 0,
        "by_commune": [
            {"slug": slug, "label": label, "total": 0, "success": 0, "error": 0, "last_7d": 0, "last_30d": 0}
            for slug, label in _CUA_LABELS.items()
        ],
        "by_day": _fill_days({}),
        "recent": [],
    }
    if not ident:
        return empty

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=_DAY_WINDOW)
    params = {"week": week_ago, "month": month_ago, "since": month_ago, "lim": _RECENT_LIMIT}

    totals = _fetch_one(
        conn,
        f"""
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (
                WHERE COALESCE(lower(status), 'success') NOT IN ('error', 'failed', 'timeout')
            )::int AS success,
            COUNT(*) FILTER (
                WHERE COALESCE(lower(status), 'success') IN ('error', 'failed', 'timeout')
            )::int AS error,
            COUNT(*) FILTER (WHERE created_at >= :week)::int AS last_7d,
            COUNT(*) FILTER (WHERE created_at >= :month)::int AS last_30d
        FROM {ident}
        """,
        params,
    ) or empty

    by_commune_rows = _fetch_rows(
        conn,
        f"""
        SELECT
            COALESCE(
                NULLIF(lower(trim(commune_slug)), ''),
                NULLIF(lower(trim(commune)), ''),
                'inconnu'
            ) AS slug,
            COUNT(*)::int AS total,
            COUNT(*) FILTER (
                WHERE COALESCE(lower(status), 'success') NOT IN ('error', 'failed', 'timeout')
            )::int AS success,
            COUNT(*) FILTER (
                WHERE COALESCE(lower(status), 'success') IN ('error', 'failed', 'timeout')
            )::int AS error,
            COUNT(*) FILTER (WHERE created_at >= :week)::int AS last_7d,
            COUNT(*) FILTER (WHERE created_at >= :month)::int AS last_30d
        FROM {ident}
        GROUP BY 1
        """,
        params,
    )
    if by_commune_rows is None:
        by_commune_rows = _fetch_rows(
            conn,
            f"""
            SELECT
                COALESCE(NULLIF(lower(trim(commune)), ''), 'inconnu') AS slug,
                COUNT(*)::int AS total,
                COUNT(*) FILTER (
                    WHERE COALESCE(lower(status), 'success') NOT IN ('error', 'failed', 'timeout')
                )::int AS success,
                COUNT(*) FILTER (
                    WHERE COALESCE(lower(status), 'success') IN ('error', 'failed', 'timeout')
                )::int AS error,
                COUNT(*) FILTER (WHERE created_at >= :week)::int AS last_7d,
                COUNT(*) FILTER (WHERE created_at >= :month)::int AS last_30d
            FROM {ident}
            GROUP BY 1
            """,
            params,
        ) or []

    by_commune: dict[str, dict[str, int]] = {}
    for row in by_commune_rows:
        slug = str(row.get("slug") or "inconnu")
        by_commune[slug] = {
            "total": int(row.get("total") or 0),
            "success": int(row.get("success") or 0),
            "error": int(row.get("error") or 0),
            "last_7d": int(row.get("last_7d") or 0),
            "last_30d": int(row.get("last_30d") or 0),
        }

    commune_rows = []
    for slug in sorted(set(list(_CUA_LABELS) + list(by_commune))):
        st = by_commune.get(slug) or {
            "total": 0, "success": 0, "error": 0, "last_7d": 0, "last_30d": 0,
        }
        commune_rows.append({"slug": slug, "label": _CUA_LABELS.get(slug, slug), **st})

    day_counts: dict[str, int] = defaultdict(int)
    for d in _fetch_rows(
        conn,
        f"""
        SELECT (created_at AT TIME ZONE 'Europe/Paris')::date AS day, COUNT(*)::int AS n
        FROM {ident}
        WHERE created_at >= :since
        GROUP BY 1
        """,
        params,
    ) or []:
        day_counts[str(d.get("day"))] += int(d.get("n") or 0)

    recent_sql = f"""
        SELECT
            p.slug,
            COALESCE(
                NULLIF(lower(trim(p.commune_slug)), ''),
                NULLIF(lower(trim(p.commune)), ''),
                'inconnu'
            ) AS commune_slug,
            p.created_at,
            COALESCE(NULLIF(trim(p.user_email), ''), au.email) AS user_email,
            p.user_id::text AS user_id,
            p.parcelles,
            COALESCE(p.status, 'success') AS status
        FROM {ident} p
        LEFT JOIN auth.users au ON au.id = p.user_id
        ORDER BY p.created_at DESC NULLS LAST
        LIMIT :lim
    """
    recent_rows = _fetch_rows(conn, recent_sql, params)
    if recent_rows is None:
        recent_rows = _fetch_rows(
            conn,
            f"""
            SELECT
                p.slug,
                COALESCE(NULLIF(lower(trim(p.commune)), ''), 'inconnu') AS commune_slug,
                p.created_at,
                COALESCE(NULLIF(trim(p.user_email), ''), au.email) AS user_email,
                p.user_id::text AS user_id,
                p.parcelles,
                COALESCE(p.status, 'success') AS status
            FROM {ident} p
            LEFT JOIN auth.users au ON au.id = p.user_id
            ORDER BY p.created_at DESC NULLS LAST
            LIMIT :lim
            """,
            params,
        ) or []

    recent: list[dict[str, Any]] = []
    for row in recent_rows:
        slug_com = str(row.get("commune_slug") or "inconnu")
        n_parc, parc_label = _format_parcelles(row.get("parcelles"))
        recent.append(
            {
                "slug": row.get("slug"),
                "commune_slug": slug_com,
                "created_at": _iso(row.get("created_at")),
                "user_email": row.get("user_email"),
                "user_id": str(row.get("user_id") or "").strip() or None,
                "n_parcelles": n_parc,
                "parcelles_label": parc_label,
                "status": str(row.get("status") or "success").strip().lower(),
                "project_url": _project_url(slug_com, row.get("slug")),
            }
        )

    return {
        "total": int(totals.get("total") or 0),
        "success": int(totals.get("success") or 0),
        "error": int(totals.get("error") or 0),
        "last_7d": int(totals.get("last_7d") or 0),
        "last_30d": int(totals.get("last_30d") or 0),
        "by_commune": commune_rows,
        "by_day": _fill_days(day_counts),
        "recent": recent,
    }


def _load_chat(conn) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=_DAY_WINDOW)

    by_commune: list[dict[str, Any]] = []
    recent: list[dict[str, Any]] = []
    day_counts: dict[str, int] = defaultdict(int)

    total_sessions = total_tokens = total_turns = 0
    unique_users: set[str] = set()

    for profile in COMMUNE_PROFILES.values():
        schema = _safe_ident(profile.schema)
        if not _table_exists(conn, schema, "plu_sessions"):
            by_commune.append(
                {
                    "slug": profile.slug,
                    "label": profile.label,
                    "sessions": 0,
                    "tokens": 0,
                    "turns": 0,
                    "users": 0,
                    "last_7d": 0,
                    "last_30d": 0,
                    "table_missing": True,
                }
            )
            continue

        agg = _fetch_one(
            conn,
            f"""
            SELECT
                COUNT(*)::int AS sessions,
                COALESCE(SUM(total_tokens), 0)::bigint AS tokens,
                COALESCE(SUM(total_turns), 0)::bigint AS turns,
                COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL)::int AS users
            FROM {schema}.plu_sessions
            """,
        )
        if agg is None:
            by_commune.append(
                {
                    "slug": profile.slug,
                    "label": profile.label,
                    "sessions": 0,
                    "tokens": 0,
                    "turns": 0,
                    "users": 0,
                    "last_7d": 0,
                    "last_30d": 0,
                    "table_missing": False,
                }
            )
            continue

        recent_counts = _fetch_one(
            conn,
            f"""
            SELECT
                COUNT(*) FILTER (WHERE updated_at >= :week)::int AS last_7d,
                COUNT(*) FILTER (WHERE updated_at >= :month)::int AS last_30d
            FROM {schema}.plu_sessions
            """,
            {"week": week_ago, "month": month_ago},
        ) or {"last_7d": 0, "last_30d": 0}

        daily = _fetch_rows(
            conn,
            f"""
            SELECT (updated_at AT TIME ZONE 'Europe/Paris')::date AS day,
                   COUNT(*)::int AS n
            FROM {schema}.plu_sessions
            WHERE updated_at >= :since
            GROUP BY 1
            """,
            {"since": month_ago},
        ) or []
        for d in daily:
            key = str(d["day"])
            day_counts[key] += int(d["n"] or 0)

        sessions_n = int(agg["sessions"] or 0)
        tokens_n = int(agg["tokens"] or 0)
        turns_n = int(agg["turns"] or 0)
        users_n = int(agg["users"] or 0)
        total_sessions += sessions_n
        total_tokens += tokens_n
        total_turns += turns_n

        by_commune.append(
            {
                "slug": profile.slug,
                "label": profile.label,
                "sessions": sessions_n,
                "tokens": tokens_n,
                "turns": turns_n,
                "users": users_n,
                "last_7d": int(recent_counts["last_7d"] or 0),
                "last_30d": int(recent_counts["last_30d"] or 0),
                "table_missing": False,
            }
        )

        recent_rows = _fetch_rows(
            conn,
            f"""
            SELECT s.id, s.user_id, au.email AS user_email,
                   s.created_at, s.updated_at,
                   COALESCE(s.total_tokens, 0) AS total_tokens,
                   COALESCE(s.total_turns, 0) AS total_turns,
                   s.section, s.numero
            FROM {schema}.plu_sessions s
            LEFT JOIN auth.users au ON au.id = s.user_id
            ORDER BY s.updated_at DESC NULLS LAST
            LIMIT :lim
            """,
            {"lim": _RECENT_LIMIT},
        ) or []

        for row in recent_rows:
            uid = str(row.get("user_id") or "").strip()
            if uid:
                unique_users.add(uid)
            section = str(row.get("section") or "").strip()
            numero = str(row.get("numero") or "").strip()
            preview = f"{section} {numero}".strip() or "Conversation"
            recent.append(
                {
                    "session_id": str(row["id"]),
                    "commune_slug": profile.slug,
                    "user_id": uid or None,
                    "user_email": row.get("user_email"),
                    "created_at": _iso(row.get("created_at")),
                    "updated_at": _iso(row.get("updated_at")),
                    "total_tokens": int(row.get("total_tokens") or 0),
                    "total_turns": int(row.get("total_turns") or 0),
                    "preview": preview,
                    "chat_url": _chat_url(profile.slug),
                }
            )

    recent.sort(key=lambda x: x.get("updated_at") or "", reverse=True)

    return {
        "total_sessions": total_sessions,
        "total_tokens": total_tokens,
        "total_turns": total_turns,
        "unique_users": len(unique_users),
        "by_commune": by_commune,
        "by_day": _fill_days(day_counts),
        "recent": recent[:_RECENT_LIMIT],
    }


def _user_row(row: dict[str, Any]) -> dict[str, Any]:
    slug = str(row.get("commune_slug") or "").strip().lower() or None
    role = str(row.get("role") or "").strip() or None
    return {
        "user_id": str(row.get("user_id") or ""),
        "email": row.get("email"),
        "commune_slug": slug,
        "commune_label": _CUA_LABELS.get(slug or "", slug or "Sans accès"),
        "code_insee": row.get("code_insee"),
        "role": role,
        "role_label": _ROLE_LABELS.get(role or "", role),
        "access_since": _iso(row.get("access_since")),
        "account_created": _iso(row.get("account_created")),
        "last_sign_in_at": _iso(row.get("last_sign_in_at")),
        "email_confirmed": bool(row.get("email_confirmed")),
    }


def _load_users(conn) -> dict[str, Any]:
    """Accès portail (`user_commune_access`) + email `auth.users`, une ligne par accès."""
    empty = {
        "total_accounts": 0,
        "total_accesses": 0,
        "by_commune": [],
        "sans_acces": [],
    }
    if not _table_exists(conn, "public", "user_commune_access"):
        return empty

    accesses = _fetch_rows(
        conn,
        """
        SELECT
            a.user_id::text AS user_id,
            u.email,
            a.commune_slug,
            a.code_insee,
            a.role,
            a.created_at AS access_since,
            u.created_at AS account_created,
            u.last_sign_in_at,
            (u.email_confirmed_at IS NOT NULL) AS email_confirmed
        FROM public.user_commune_access a
        LEFT JOIN auth.users u ON u.id = a.user_id
        ORDER BY a.commune_slug, u.email NULLS LAST, a.created_at DESC
        """,
    ) or []

    sans_acces = _fetch_rows(
        conn,
        """
        SELECT
            u.id::text AS user_id,
            u.email,
            NULL AS commune_slug,
            NULL AS code_insee,
            NULL AS role,
            NULL AS access_since,
            u.created_at AS account_created,
            u.last_sign_in_at,
            (u.email_confirmed_at IS NOT NULL) AS email_confirmed
        FROM auth.users u
        WHERE NOT EXISTS (
            SELECT 1 FROM public.user_commune_access a WHERE a.user_id = u.id
        )
        ORDER BY u.created_at DESC
        """,
    ) or []

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    user_ids: set[str] = set()
    for row in accesses:
        item = _user_row(row)
        slug = item["commune_slug"] or "inconnu"
        grouped[slug].append(item)
        if item["user_id"]:
            user_ids.add(item["user_id"])

    sans_rows = []
    for row in sans_acces:
        item = _user_row(row)
        item["commune_label"] = "Sans accès commune"
        sans_rows.append(item)
        if item["user_id"]:
            user_ids.add(item["user_id"])

    by_commune = []
    for slug in sorted(grouped, key=lambda s: _CUA_LABELS.get(s, s).lower()):
        users = grouped[slug]
        by_commune.append(
            {
                "slug": slug,
                "label": _CUA_LABELS.get(slug, slug),
                "count": len(users),
                "users": users,
            }
        )

    return {
        "total_accounts": len(user_ids),
        "total_accesses": len(accesses),
        "by_commune": by_commune,
        "sans_acces": sans_rows,
    }


def _require_superadmin(authorization: Optional[str] = Header(default=None)) -> str:
    """JWT superadmin, ou ADMIN_API_TOKEN — sans importer le module règlements (asyncpg)."""
    expected = os.getenv("ADMIN_API_TOKEN")
    if expected and authorization == f"Bearer {expected}":
        return "admin-token"
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Authentification requise (Bearer token).")
    token = authorization[7:].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token manquant.")
    from api.agents.plu_agent.routes.plu_auth import verify_supabase_access_token
    from services.auth.commune_access import assert_superadmin

    user_id = verify_supabase_access_token(token)
    assert_superadmin(user_id)
    return user_id


def _empty_cua() -> dict[str, Any]:
    return {
        "total": 0,
        "success": 0,
        "error": 0,
        "last_7d": 0,
        "last_30d": 0,
        "by_commune": [],
        "by_day": _fill_days({}),
        "recent": [],
    }


def _empty_chat() -> dict[str, Any]:
    return {
        "total_sessions": 0,
        "total_tokens": 0,
        "total_turns": 0,
        "unique_users": 0,
        "by_commune": [],
        "by_day": _fill_days({}),
        "recent": [],
    }


def _empty_users() -> dict[str, Any]:
    return {
        "total_accounts": 0,
        "total_accesses": 0,
        "by_commune": [],
        "sans_acces": [],
    }


@router.get("")
@router.get("/")
def get_monitoring(_user_id: str = Depends(_require_superadmin)) -> dict[str, Any]:
    """Agrégats CUA, chat, et accès utilisateurs (`user_commune_access` + `auth.users`)."""
    engine = get_engine()
    cua = _empty_cua()
    chat = _empty_chat()
    users = _empty_users()
    try:
        with engine.connect() as conn:
            cua = _load_cua(conn)
    except Exception:
        logger.exception("Monitoring : échec agrégats CUA")
    try:
        with engine.connect() as conn:
            chat = _load_chat(conn)
    except Exception:
        logger.exception("Monitoring : échec agrégats chat")
    try:
        with engine.connect() as conn:
            users = _load_users(conn)
    except Exception:
        logger.exception("Monitoring : échec liste utilisateurs")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cua": cua,
        "chat": chat,
        "users": users,
    }
