# -*- coding: utf-8 -*-
"""Notification Slack interne — génération CUA (canal dédié, jamais bloquant)."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

from api.cuas.argeles.db import logger

_PARIS = ZoneInfo("Europe/Paris")
_PROJECT_URL_BASE = "https://www.kerelia.fr"


def _webhook_url() -> str:
    return (os.getenv("SLACK_CUA_WEBHOOK") or "").strip()


def _notifications_allowed() -> bool:
    if os.getenv("SLACK_FORCE_NOTIFY", "").strip().lower() in ("1", "true", "yes", "on"):
        return True
    return (os.getenv("RENDER") or "").strip().lower() in ("true", "1", "yes")


def _project_url(commune_slug: str | None, slug: str | None) -> str:
    commune = (commune_slug or "").strip().lower()
    project = (slug or "").strip()
    if not commune or not project or commune == "?":
        return ""
    return f"{_PROJECT_URL_BASE}/{commune}/cua/projects/{project}"


def _format_parcelles(parcelles: list[dict] | None) -> str:
    labels = []
    for p in parcelles or []:
        section = str(p.get("section") or "").strip().upper()
        numero = str(p.get("numero") or "").strip()
        if section and numero:
            labels.append(f"{section} {numero}")
    return " · ".join(labels) if labels else "—"


def _post(text: str) -> None:
    if not _notifications_allowed():
        return
    url = _webhook_url()
    if not url:
        return
    try:
        requests.post(url, json={"text": text}, timeout=10)
    except Exception as exc:
        logger.warning("Slack CUA notification failed: %s", exc)


def notify_cua_generated(
    result: dict[str, Any],
    *,
    user_email: Optional[str] = None,
    user_id: Optional[str] = None,
) -> None:
    """Ping après génération réussie. N'échoue jamais vers l'appelant."""
    try:
        ts = datetime.now(_PARIS).strftime("%d/%m/%Y %H:%M")
        commune = result.get("commune_slug") or result.get("commune") or "?"
        n = result.get("n_parcelles") or len(result.get("parcelles") or [])
        surface = result.get("surface_m2")
        contenance = result.get("surface_indicative")
        dossier = (result.get("dossier") or {}).get("numero_cu") or "—"
        user = (user_email or "").strip() or (user_id or "").strip() or "inconnu"
        surface_txt = f"{surface:,.0f} m²".replace(",", " ") if surface is not None else "—"
        cad_txt = (
            f" (contenance {contenance:,.0f} m²)".replace(",", " ")
            if contenance is not None
            else ""
        )
        project_url = _project_url(str(commune), result.get("slug"))
        text = (
            f":white_check_mark: *CUA généré* — {commune} — {ts}\n"
            f"• User : {user}\n"
            f"• {n} parcelle(s) : {_format_parcelles(result.get('parcelles'))}\n"
            f"• Surface SIG : {surface_txt}{cad_txt}\n"
            f"• Dossier : {dossier}\n"
            f"• Projet : {project_url or '—'}"
        )
        _post(text)
    except Exception as exc:
        logger.warning("Slack CUA notification skipped: %s", exc)


def notify_cua_failed(
    *,
    commune_slug: str,
    refs: list[dict] | None = None,
    user_email: Optional[str] = None,
    error: str = "",
    status_code: int | None = None,
    error_type: str = "",
    traceback_text: str = "",
) -> None:
    """Ping interne en cas d'échec pipeline — n'affecte pas la réponse HTTP."""
    try:
        ts = datetime.now(_PARIS).strftime("%d/%m/%Y %H:%M")
        user = (user_email or "").strip() or "inconnu"
        err = (error or "").strip()
        if len(err) > 1200:
            err = err[:1197] + "…"
        status = f"HTTP {status_code}" if status_code else "échec"
        kind = (error_type or "").strip()
        header = f":x: *CUA échec* — {commune_slug or '?'} — {status}"
        if kind:
            header += f" ({kind})"
        lines = [
            f"{header} — {ts}",
            f"• User : {user}",
            f"• Parcelles : {_format_parcelles(refs)}",
            f"• Erreur : {err or '—'}",
        ]
        tb = (traceback_text or "").strip()
        if tb:
            tb_lines = [ln for ln in tb.splitlines() if ln.strip()][-8:]
            excerpt = "\n".join(tb_lines)
            if len(excerpt) > 1500:
                excerpt = excerpt[-1500:]
            lines.append(f"```{excerpt}```")
        _post("\n".join(lines))
    except Exception as exc:
        logger.warning("Slack CUA failure notification skipped: %s", exc)
