# -*- coding: utf-8 -*-
"""
API veille réglementaire RAA — multi-commune.

Endpoints (par slug communal) :

    GET  /{commune_slug}/raa                 -> liste (RAA + dernière analyse jointe)
    GET  /{commune_slug}/raa/{raa_id}       -> détail (avec le tableau `arretes`)
    POST /{commune_slug}/raa/sync           -> scrape préfecture, diff, analyse des nouveaux (202)
    POST /{commune_slug}/raa/{raa_id}/marquer-vu -> marque un recueil comme lu
    POST /{commune_slug}/raa/{raa_id}/analyser -> lance l'analyse en tâche de fond (202)

Communes supportées : voir raa_config.RAA_COMMUNES (argeles, latresne, …).
"""

import json
import logging
from datetime import date, datetime

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from .._env import DB_CONFIG
from .raa_config import RaaCommuneConfig, get_raa_config
from .service_analyse_raa import analyser_raa
from .service_sync_raa import commune_a_scraper, sync_raa

logger = logging.getLogger("raa_api")

router = APIRouter(tags=["raa"])


def _db_conn():
    return psycopg2.connect(**DB_CONFIG)


def _parse_json_field(value):
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return value


def _require_config(commune_slug: str) -> RaaCommuneConfig:
    cfg = get_raa_config(commune_slug)
    if not cfg:
        raise HTTPException(
            status_code=404,
            detail=f"Veille RAA non disponible pour la commune « {commune_slug} ».",
        )
    return cfg


def _sql_list(schema: str) -> str:
    return f"""
    SELECT
        r.id, r.titre, r.date_publication, r.pdf_url, r.page_url,
        r.taille_mo, r.statut, r.vu, r.departement, r.updated_at,
        a.niveau_alerte, a.nb_arretes_total, a.nb_arretes_pertinents,
        a.commune_mentionnee, a.resume_global, a.arretes, a.cout_estime,
        a.tokens_in, a.tokens_out, a.erreur,
        a.created_at AS analyse_at
    FROM {schema}.raa r
    LEFT JOIN LATERAL (
        SELECT * FROM {schema}.raa_analyse aa
        WHERE aa.raa_id = r.id
        ORDER BY aa.created_at DESC
        LIMIT 1
    ) a ON TRUE
    WHERE (%(annee)s IS NULL OR EXTRACT(YEAR FROM r.date_publication) = %(annee)s)
    ORDER BY r.date_publication DESC NULLS LAST, r.id DESC
    LIMIT %(limit)s;
"""


def raa_list(cfg: RaaCommuneConfig, annee: int | None = None, limit: int = 500) -> list[dict]:
    conn = _db_conn()
    conn.autocommit = True
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_sql_list(cfg.schema), {"annee": annee, "limit": limit})
        rows = []
        for r in cur.fetchall():
            row = dict(r)
            row["arretes"] = _parse_json_field(row.get("arretes")) or []
            rows.append(row)
    conn.close()
    return rows


def _sql_detail(schema: str) -> str:
    return f"""
    SELECT
        r.id, r.titre, r.date_publication, r.pdf_url, r.page_url,
        r.taille_mo, r.statut, r.vu, r.departement, r.created_at, r.updated_at,
        a.id AS analyse_id, a.modele, a.niveau_alerte,
        a.nb_arretes_total, a.nb_arretes_pertinents, a.commune_mentionnee,
        a.resume_global, a.arretes, a.tokens_in, a.tokens_out,
        a.cout_estime, a.erreur, a.created_at AS analyse_at
    FROM {schema}.raa r
    LEFT JOIN LATERAL (
        SELECT * FROM {schema}.raa_analyse aa
        WHERE aa.raa_id = r.id
        ORDER BY aa.created_at DESC
        LIMIT 1
    ) a ON TRUE
    WHERE r.id = %s;
"""


def raa_get(cfg: RaaCommuneConfig, raa_id: int) -> dict | None:
    conn = _db_conn()
    conn.autocommit = True
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_sql_detail(cfg.schema), (raa_id,))
        row = cur.fetchone()
    conn.close()
    if not row:
        return None
    row = dict(row)
    row["arretes"] = _parse_json_field(row.get("arretes")) or []
    return row


def raa_marquer_vu(cfg: RaaCommuneConfig, raa_id: int) -> bool:
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {cfg.schema}.raa SET vu=true, updated_at=now() WHERE id=%s RETURNING id;",
                (raa_id,),
            )
            ok = cur.fetchone() is not None
    conn.close()
    return ok


def raa_set_statut(cfg: RaaCommuneConfig, raa_id: int, statut: str) -> None:
    conn = _db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {cfg.schema}.raa SET statut=%s, updated_at=now() WHERE id=%s;",
                (statut, raa_id),
            )
    conn.close()


def _run_analyse_bg(commune_slug: str, raa_id: int) -> None:
    conn = _db_conn()
    try:
        analyser_raa(conn, raa_id, commune_slug)
    except Exception as e:
        logger.error(
            "analyse RAA #%s (%s) échouée hors service : %s",
            raa_id, commune_slug, e, exc_info=True,
        )
        try:
            cfg = get_raa_config(commune_slug)
            if cfg:
                raa_set_statut(cfg, raa_id, "erreur")
        except Exception:
            pass
    finally:
        conn.close()


def _run_analyses_sequentielles_bg(commune_slug: str, raa_ids: list[int]) -> None:
    """Analyse les RAA un par un (évite la saturation Gemini)."""
    for raa_id in raa_ids:
        _run_analyse_bg(commune_slug, raa_id)


class RaaListItem(BaseModel):
    id: int
    titre: str
    date_publication: date | None = None
    pdf_url: str
    page_url: str
    taille_mo: float | None = None
    statut: str
    vu: bool = True
    departement: str | None = None
    niveau_alerte: str | None = None
    nb_arretes_total: int | None = None
    nb_arretes_pertinents: int | None = None
    commune_mentionnee: bool | None = None
    resume_global: str | None = None
    cout_estime: float | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    erreur: str | None = None
    analyse_at: datetime | None = None
    arretes: list[dict] = []


class RaaListResponse(BaseModel):
    commune_slug: str
    raa: list[RaaListItem]


class RaaDetail(RaaListItem):
    analyse_id: int | None = None
    modele: str | None = None
    arretes: list[dict] = []
    created_at: datetime | None = None
    updated_at: datetime | None = None


class AnalyseLancee(BaseModel):
    commune_slug: str
    raa_id: int
    statut: str
    message: str


class RaaNouveau(BaseModel):
    id: int
    titre: str | None = None
    date_publication: date | None = None
    pdf_url: str
    statut: str


class SyncRaaResponse(BaseModel):
    commune_slug: str
    annee: int
    nb_scrapes: int
    nb_nouveaux: int
    nouveaux: list[RaaNouveau]
    analyses_lancees: list[int]
    message: str


class MarquerVuResponse(BaseModel):
    commune_slug: str
    raa_id: int
    vu: bool


@router.get("/{commune_slug}/raa", response_model=RaaListResponse)
def list_raa(commune_slug: str, annee: int | None = None):
    """Liste des RAA (plus récents d'abord) avec leur dernière analyse."""
    cfg = _require_config(commune_slug)
    return RaaListResponse(commune_slug=commune_slug, raa=raa_list(cfg, annee=annee))


@router.get("/{commune_slug}/raa/{raa_id}", response_model=RaaDetail)
def get_raa(commune_slug: str, raa_id: int):
    """Détail d'un RAA, dernière analyse incluse (avec le détail des arrêtés)."""
    cfg = _require_config(commune_slug)
    row = raa_get(cfg, raa_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"RAA {raa_id} introuvable.")
    return row


@router.post("/{commune_slug}/raa/sync", response_model=SyncRaaResponse, status_code=202)
def sync_raa_endpoint(
    commune_slug: str,
    background: BackgroundTasks,
    annee: int | None = None,
):
    """
    Scrape la page préfecture, insère les recueils absents de la base (diff par pdf_url),
    puis lance l'analyse LLM en tâche de fond pour chaque nouveau recueil.
    Les RAA déjà en base (analysés ou non) ne sont jamais modifiés ni re-analysés.
    """
    cfg = _require_config(commune_slug)
    if not commune_a_scraper(commune_slug):
        raise HTTPException(
            status_code=501,
            detail=f"Scraping RAA non disponible pour « {commune_slug} ».",
        )

    conn = _db_conn()
    try:
        result = sync_raa(conn, commune_slug, annee=annee)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("sync RAA (%s) échouée : %s", commune_slug, e, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Échec du scraping : {e}") from e
    finally:
        conn.close()

    ids = [n["id"] for n in result["nouveaux"]]
    for raa_id in ids:
        raa_set_statut(cfg, raa_id, "en_cours")
    if ids:
        background.add_task(_run_analyses_sequentielles_bg, commune_slug, ids)

    nb = result["nb_nouveaux"]
    if nb == 0:
        msg = "Aucun nouveau recueil — la base est à jour."
    elif nb == 1:
        msg = "1 nouveau recueil détecté — analyse en cours."
    else:
        msg = f"{nb} nouveaux recueils détectés — analyses en cours."

    return SyncRaaResponse(
        commune_slug=commune_slug,
        annee=result["annee"],
        nb_scrapes=result["nb_scrapes"],
        nb_nouveaux=nb,
        nouveaux=result["nouveaux"],
        analyses_lancees=ids,
        message=msg,
    )


@router.post("/{commune_slug}/raa/{raa_id}/marquer-vu", response_model=MarquerVuResponse)
def marquer_vu(commune_slug: str, raa_id: int):
    """Marque un recueil analysé comme lu par un utilisateur (retire le badge « Nouveau »)."""
    cfg = _require_config(commune_slug)
    row = raa_get(cfg, raa_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"RAA {raa_id} introuvable.")
    if not raa_marquer_vu(cfg, raa_id):
        raise HTTPException(status_code=404, detail=f"RAA {raa_id} introuvable.")
    return MarquerVuResponse(commune_slug=commune_slug, raa_id=raa_id, vu=True)


@router.post("/{commune_slug}/raa/{raa_id}/analyser", response_model=AnalyseLancee, status_code=202)
def lancer_analyse(commune_slug: str, raa_id: int, background: BackgroundTasks):
    """
    Lance (ou relance) l'analyse Gemini en tâche de fond.
    Répond immédiatement ; la page poll GET /{id} jusqu'à statut 'analyse' ou 'erreur'.
    """
    cfg = _require_config(commune_slug)
    row = raa_get(cfg, raa_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"RAA {raa_id} introuvable.")

    if row["statut"] == "en_cours":
        return AnalyseLancee(
            commune_slug=commune_slug,
            raa_id=raa_id,
            statut="en_cours",
            message="Une analyse est déjà en cours.",
        )

    raa_set_statut(cfg, raa_id, "en_cours")
    background.add_task(_run_analyse_bg, commune_slug, raa_id)
    return AnalyseLancee(
        commune_slug=commune_slug,
        raa_id=raa_id,
        statut="en_cours",
        message="Analyse lancée.",
    )
