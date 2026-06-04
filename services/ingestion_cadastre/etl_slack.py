"""Notifications Slack pour l'ETL commune (parcelles + BAN + enrichissement)."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

import requests

from services.ingestion_cadastre.env_loader import load_project_env


def slack_webhook() -> str:
    load_project_env()
    return (
        os.getenv("SLACK_WEBHOOK")
        or os.getenv("SLACK_WEBHOOK_URL")
        or os.getenv("SLACK_DEPLOY_WEBHOOK")
        or ""
    ).strip()


def format_parcelles_block(result: dict) -> str:
    """Bloc texte diff parcelles (même format que sync_or_add)."""
    lines = [
        "=" * 60,
        f"  DIFF PARCELLES — {result.get('code_insee', '')} ({result.get('commune', '')})",
        "=" * 60,
        f"  Schéma          : {result.get('schema', '')}",
        f"  Table cible     : {result.get('table_cible', '')}",
        f"  Etalab          : {result.get('total_etalab', 0)} parcelles",
        f"  Base            : {result.get('total_db', 0)} parcelles",
        f"  Nouveaux        : {result.get('nouveaux', {}).get('count', 0)}",
        f"  Supprimés       : {result.get('supprimes', {}).get('count', 0)}",
        f"  Contenance diff : {result.get('contenance_diff', {}).get('count', 0)}",
        f"  Géométrie diff  : {result.get('geom_diff', {}).get('count', 0)}",
        "=" * 60,
    ]
    return "\n".join(lines)


def format_ban_block(ban: dict) -> str:
    lines = [
        "--- BAN ---",
        f"  Adresses en base     : {ban.get('nb_adresses', 0)}",
        f"  Liens parcelle↔adr   : {ban.get('nb_liens', 0)}",
    ]
    if ban.get("skipped"):
        lines.append("  (étape ignorée)")
    return "\n".join(lines)


def format_enrich_block(enrich: dict) -> str:
    total = enrich.get("total_parcelles", 0)
    avec = enrich.get("avec_adresse", 0)
    pct = round(100 * avec / total, 1) if total else 0.0
    lines = [
        "--- ENRICHISSEMENT parcelles ---",
        f"  Parcelles totales           : {total}",
        f"  Avec au moins 1 adresse     : {avec} ({pct} %)",
        f"  Avec autres parcelles liées : {enrich.get('avec_parcelles_liees', 0)}",
        f"  IDU avec lien BAN (liens)   : {enrich.get('parcelles_lien_ban', 0)}",
        f"  Adresses BAN distinctes     : {enrich.get('adresses_ban', 0)}",
    ]
    if enrich.get("skipped"):
        lines.append("  (étape ignorée)")
    elif enrich.get("dry_run"):
        lines.append("  (dry-run — pas de mise à jour)")
    return "\n".join(lines)


def notify_etl_complete(
    *,
    schema: str,
    insee: str,
    commune_label: str,
    parcelles_result: Optional[dict] = None,
    parcelles_note: Optional[str] = None,
    ban_stats: Optional[dict] = None,
    enrich_stats: Optional[dict] = None,
    elapsed_s: float = 0,
    dry_run: bool = False,
) -> None:
    """Envoie un message Slack récapitulatif ETL."""
    if dry_run:
        return

    webhook = slack_webhook()
    if not webhook:
        print("Info: webhook Slack absent, notification ETL ignorée.")
        return

    date_str = datetime.now().strftime("%d/%m/%Y à %Hh%M")
    blocks: list[str] = []

    if parcelles_result:
        blocks.append(format_parcelles_block(parcelles_result))
    elif parcelles_note:
        blocks.append(f"--- PARCELLES ---\n  {parcelles_note}")

    if ban_stats:
        blocks.append(format_ban_block(ban_stats))
    if enrich_stats:
        blocks.append(format_enrich_block(enrich_stats))

    body = "```\n" + "\n\n".join(blocks) + "\n```"
    if elapsed_s:
        body += f"\n_Durée ETL : {elapsed_s:.0f} s_"

    has_parcel_ecart = False
    if parcelles_result:
        has_parcel_ecart = any(
            parcelles_result.get(k, {}).get("count", 0)
            for k in ("nouveaux", "supprimes", "contenance_diff", "geom_diff")
        )

    title = (
        f"ETL commune — {commune_label} — INSEE {insee} — {date_str}"
        if has_parcel_ecart
        else f"OK ETL commune — {commune_label} — INSEE {insee} — {date_str}"
    )
    color = "warning" if has_parcel_ecart else "good"

    payload = {
        "text": title,
        "attachments": [
            {
                "color": color,
                "mrkdwn_in": ["text"],
                "text": body,
                "footer": f"{schema}.parcelles | ETL run_etl_commune",
            }
        ],
    }

    try:
        resp = requests.post(webhook, json=payload, timeout=10)
        resp.raise_for_status()
        print("Notification Slack ETL envoyée.")
    except Exception as e:
        print(f"Alerte: envoi Slack ETL échoué: {e}")
