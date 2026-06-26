"""
route_identite_parcelle.py
Endpoint API pour l'identité parcellaire et l'identité foncière (UF) :
intersections, carte Folium, rapport PDF, et proxy des fichiers carte/PDF stockés
sur Supabase (liens « propres » sans domaine supabase dans le PDF).
"""
import logging
import os
import re
import secrets
import time

import requests
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, AliasChoices, field_validator, model_validator
from typing import List, Dict, Any, Optional
from pathlib import Path

from .identite_fonciere import (
    get_catalogue,
    analyser_identite_fonciere,
    analyser_identite_parcelle,
    get_identite_db_schema,
    identite_fonciere_request_context,
    resolve_identite_fonciere_geometry,
)
from .carte_identite_fonciere import generate_identite_fonciere_map_html
from .sse_identite_fonciere import iter_identite_fonciere_sse_chunks, sse_error_chunk
from .pdf.rapport_identite_fonciere import generate_rapport_pdf
from . import identite_fonciere_history as identite_fonciere_history_module
from services.auth.current_user import get_current_user_id
from .storage_et_urls import (
    new_project_id,
    object_path,
    public_object_url,
    upload_html_carte,
    upload_pdf_rapport,
)

router = APIRouter(prefix="/api/identite-parcelle", tags=["Identité Parcellaire"])
router_fonciere = APIRouter(prefix="/api/identite-fonciere", tags=["Identité Foncière"])

_logger = logging.getLogger(__name__)

# HTML Folium servi via GET /map/view/{token} (évite de ne renvoyer que du HTML inline)
_MAP_HTML_CACHE: dict[str, tuple[str, float]] = {}
_MAP_CACHE_TTL_SEC = int(os.getenv("IDENTITE_FONCIERE_MAP_CACHE_TTL_SEC", "3600"))

# PDF généré pour POST /publier si upload Storage échoue (lien temporaire GET /rapport/view/{token})
_RAPPORT_PDF_CACHE: dict[str, tuple[str, float]] = {}

# GET /public/if/... — identifiants projet générés par storage_et_urls.new_project_id()
_IF_PROJECT_ID_RE = re.compile(r"^if_[a-f0-9]{8,32}$")
_DB_SCHEMA_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def _identite_fichier_public_autorise(filename: str) -> bool:
    """Noms autorisés pour le proxy Storage (carte HTML ou rapport PDF)."""
    base = filename.split("/")[-1]
    if base == "carte.html":
        return True
    if base.endswith(".pdf") and ".." not in base and "/" not in base and "\\" not in base:
        return True
    return False


def _prune_map_html_cache() -> None:
    now = time.time()
    for k, (_, exp) in list(_MAP_HTML_CACHE.items()):
        if exp < now:
            del _MAP_HTML_CACHE[k]


def _map_html_cache_put(html: str) -> str:
    _prune_map_html_cache()
    token = secrets.token_urlsafe(24)
    _MAP_HTML_CACHE[token] = (html, time.time() + _MAP_CACHE_TTL_SEC)
    return token


def _map_html_cache_get(token: str) -> str | None:
    _prune_map_html_cache()
    item = _MAP_HTML_CACHE.get(token)
    if not item:
        return None
    html, exp = item
    if time.time() > exp:
        del _MAP_HTML_CACHE[token]
        return None
    return html


def _prune_rapport_pdf_cache() -> None:
    now = time.time()
    for k, (_, exp) in list(_RAPPORT_PDF_CACHE.items()):
        if exp < now:
            del _RAPPORT_PDF_CACHE[k]


def _rapport_pdf_cache_put(pdf_path: Path, base_url: str) -> str:
    _prune_rapport_pdf_cache()
    token = secrets.token_urlsafe(24)
    p = str(Path(pdf_path).resolve())
    _RAPPORT_PDF_CACHE[token] = (p, time.time() + _MAP_CACHE_TTL_SEC)
    return f"{base_url}/api/identite-fonciere/rapport/view/{token}"


def _public_api_base_url(request: Request) -> str:
    """URL publique de l’API (reverse proxy) ; sinon `request.base_url`."""
    base = (os.getenv("PUBLIC_API_BASE_URL") or "").strip().rstrip("/")
    if base:
        return base
    return str(request.base_url).rstrip("/")


# ------------------------------------------------------------
# Models
# ------------------------------------------------------------

class ParcelleRequest(BaseModel):
    commune: str
    section: str
    numero: str
    insee: str

class IntersectionResult(BaseModel):
    table: str
    display_name: str
    article: str | None = None
    attribut_discriminant: str | None = None
    elements: List[Dict[str, Any]] = []

class IdentiteResponse(BaseModel):
    success: bool
    parcelle: str
    commune: str
    insee: str
    nb_intersections: int
    intersections: List[IntersectionResult]
    error: str | None = None


class IdentiteFonciereRequest(BaseModel):
    commune: str
    insee: str | None = None
    srid: int | None = None
    geometry: dict | None = None
    idu: str | None = Field(
        default=None,
        description="IDU cadastral : charge geom_2154 depuis {schema}.parcelles si geometry est absent.",
        validation_alias=AliasChoices("idu", "IDU"),
    )
    parcelle_id: int | None = Field(
        default=None,
        description="Identifiant parcelles.id pour charger la géométrie si geometry et idu sont absents.",
        validation_alias=AliasChoices("parcelle_id", "parcelleId"),
    )
    db_schema: str | None = Field(
        default=None,
        description="Schéma PostGIS des couches (ex. argeles). Le champ commune reste libellé / PDF.",
        validation_alias=AliasChoices("db_schema", "dbSchema"),
    )

    @model_validator(mode="after")
    def _geometry_ou_reference_parcelle(self) -> "IdentiteFonciereRequest":
        if isinstance(self.geometry, dict) and self.geometry.get("type"):
            return self
        if self.parcelle_id is not None:
            return self
        if self.idu and str(self.idu).strip():
            return self
        raise ValueError(
            "Fournir geometry (GeoJSON de l'UF), ou idu (référence cadastrale), ou parcelle_id (clé parcelles.id)."
        )

    @field_validator("db_schema", mode="before")
    @classmethod
    def _normalize_db_schema(cls, v: Any) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if not _DB_SCHEMA_RE.fullmatch(s):
            raise ValueError(
                "db_schema invalide : utiliser un identifiant SQL minuscule [a-z_][a-z0-9_]* (ex. argeles)."
            )
        return s


class IdentiteFonciereMapRequest(IdentiteFonciereRequest):
    intersections: List[IntersectionResult] | None = None


class ParcelleCadRef(BaseModel):
    """Une parcelle cadastrale (section + numéro) pour l’affichage UF en page de garde du PDF."""

    model_config = ConfigDict(str_strip_whitespace=True)

    section: str = ""
    numero: str = ""

    @field_validator("section", "numero", mode="before")
    @classmethod
    def _coerce_str(cls, v: Any) -> str:
        if v is None:
            return ""
        return str(v)


class CoucheSyntheseRow(BaseModel):
    """Une ligne du tableau dynamique d’intersection (identique au front)."""

    model_config = ConfigDict(populate_by_name=True)

    table: str
    display_name: str = Field(
        default="",
        validation_alias=AliasChoices("display_name", "displayName"),
    )
    status: str
    elements_count: int = Field(
        default=0,
        validation_alias=AliasChoices("elements_count", "elementsCount"),
    )
    skip_reason: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("skip_reason", "skipReason"),
    )
    error: Optional[str] = None


class RapportFonciereRequest(BaseModel):
    """PDF : intersections déjà calculées (recommandé) ou géométrie seule pour relancer l’analyse."""

    model_config = ConfigDict(populate_by_name=True)

    commune: str
    insee: str | None = None
    srid: int | None = None
    geometry: dict | None = None
    idu: str | None = Field(
        default=None,
        description="IDU cadastral pour charger la géométrie depuis la table parcelles du schéma.",
        validation_alias=AliasChoices("idu", "IDU"),
    )
    parcelle_id: int | None = Field(
        default=None,
        validation_alias=AliasChoices("parcelle_id", "parcelleId"),
    )
    intersections: List[IntersectionResult] | None = None
    output_dir: str | None = None
    db_schema: str | None = Field(
        default=None,
        description="Schéma PostGIS des couches (ex. argeles), comme sur POST /identite-fonciere/intersect.",
        validation_alias=AliasChoices("db_schema", "dbSchema"),
    )
    # URL publique (HTTPS) affichée en lien cliquable sur la page 1 du PDF (ex. page carte du front).
    carte_web_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("carte_web_url", "carteWebUrl", "map_url", "mapUrl"),
    )
    # Référence cadastrale dans le PDF si pas d’UF (ex. section + numéro)
    parcelle: str | None = None
    # UF : liste des parcelles (section + numéro) pour la page 1 du rapport
    parcelles_cadastrales: Optional[List[ParcelleCadRef]] = Field(
        default=None,
        validation_alias=AliasChoices("parcelles_cadastrales", "parcellesCadastrales"),
    )
    # Lignes « Couche / Résultat » (même logique que le tableau SSE du frontend)
    couches_synthese: Optional[List[CoucheSyntheseRow]] = Field(
        default=None,
        validation_alias=AliasChoices("couches_synthese", "couchesSynthese"),
    )
    # Optionnel : pour rattacher la publication à l’utilisateur (historique CIF, même logique que les pipelines CUA).
    user_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("user_id", "userId"),
    )
    user_email: str | None = Field(
        default=None,
        validation_alias=AliasChoices("user_email", "userEmail"),
    )

    @field_validator("db_schema", mode="before")
    @classmethod
    def _normalize_db_schema_rapport(cls, v: Any) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if not _DB_SCHEMA_RE.fullmatch(s):
            raise ValueError(
                "db_schema invalide : utiliser un identifiant SQL minuscule [a-z_][a-z0-9_]* (ex. argeles)."
            )
        return s

    @model_validator(mode="after")
    def _rapport_geom_ou_intersections(self) -> "RapportFonciereRequest":
        if self.intersections:
            return self
        has_g = isinstance(self.geometry, dict) and self.geometry.get("type")
        has_idu = bool(self.idu and str(self.idu).strip())
        if has_g or has_idu or self.parcelle_id is not None:
            return self
        raise ValueError(
            "Fournir intersections, ou geometry, ou idu / parcelle_id pour générer le rapport."
        )


class IdentiteFonciereMapResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    success: bool
    html: str
    metadata: Dict[str, Any]
    intersections: List[IntersectionResult]
    error: str | None = None
    # Lien GET pour ouvrir la même carte dans le navigateur (sans coller le HTML)
    carte_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("carte_url", "carteUrl", "map_url", "mapUrl"),
    )
    # Présent si la carte a été déposée sur Supabase Storage (même dossier que le PDF prévu)
    carte_project_id: str | None = Field(
        default=None,
        validation_alias=AliasChoices("carte_project_id", "carteProjectId"),
    )


class PublierIdentiteResponse(BaseModel):
    """Réponse JSON après génération carte + PDF et dépôt Storage (ou URL temporaires)."""

    model_config = ConfigDict(populate_by_name=True)

    success: bool
    carte_project_id: str | None = None
    carte_url: str | None = None
    pdf_url: str | None = None
    error: str | None = None
    warnings: List[str] | None = None
    history_recorded: bool | None = Field(
        default=None,
        description="True si une ligne a été créée dans latresne.identite_fonciere_projects",
    )


def _build_result_dict_from_rapport_payload(payload: RapportFonciereRequest) -> Dict[str, Any]:
    """Construit le dict métier `result` pour la carte Folium et le PDF (identique à POST /rapport)."""
    if payload.intersections:
        ref_parcelle = payload.parcelle or "UNITE_FONCIERE"
        result: Dict[str, Any] = {
            "parcelle": ref_parcelle,
            "commune": payload.commune,
            "insee": payload.insee or "",
            "nb_intersections": len(payload.intersections),
            "intersections": [i.model_dump() for i in payload.intersections],
        }
        if payload.geometry is not None:
            result["geometry"] = payload.geometry
        if payload.srid is not None:
            result["srid"] = payload.srid
        pcs = payload.parcelles_cadastrales
        if pcs:
            result["parcelles_cadastrales"] = [p.model_dump() for p in pcs]
        cs = payload.couches_synthese
        if cs:
            result["couches_synthese"] = [c.model_dump() for c in cs]
        result["db_schema"] = (payload.db_schema or "").strip() or get_identite_db_schema()
        return result
    geom = resolve_identite_fonciere_geometry(
        payload.geometry,
        idu=payload.idu,
        parcelle_id=payload.parcelle_id,
    )
    result = analyser_identite_fonciere(
        geometry=geom,
        commune=payload.commune,
        insee=payload.insee,
        srid=payload.srid,
    )
    result["geometry"] = geom
    if payload.srid is not None:
        result["srid"] = payload.srid
    pcs = payload.parcelles_cadastrales
    if pcs:
        result["parcelles_cadastrales"] = [p.model_dump() for p in pcs]
    if payload.parcelle:
        result["parcelle"] = payload.parcelle
    cs = payload.couches_synthese
    if cs:
        result["couches_synthese"] = [c.model_dump() for c in cs]
    result["db_schema"] = (payload.db_schema or "").strip() or get_identite_db_schema()
    return result


# ------------------------------------------------------------
# Endpoint
# ------------------------------------------------------------

@router.post("/intersect", response_model=IdentiteResponse)
async def intersect_parcelle(payload: ParcelleRequest):
    """
    Calcule les intersections entre une parcelle et toutes les couches
    du schéma 'carto' en base de données avec leurs éléments discriminants.
    
    Workflow:
    1. Récupère la géométrie de la parcelle depuis l'IGN (EPSG:2154)
    2. Teste l'intersection avec chaque table du schéma 'carto'
    3. Extrait les valeurs des attributs discriminants pour chaque couche
    4. Retourne les couches et leurs éléments intersectés
    """
    try:
        result = analyser_identite_parcelle(
            section=payload.section,
            numero=payload.numero,
            insee=payload.insee,
            commune=payload.commune
        )
        
        return IdentiteResponse(
            success=True,
            **result,
            error=None
        )
    
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    except Exception as e:
        return IdentiteResponse(
            success=False,
            parcelle=f"{payload.section} {payload.numero}",
            commune=payload.commune,
            insee=payload.insee,
            nb_intersections=0,
            intersections=[],
            error=str(e)
        )


@router_fonciere.post("/intersect", response_model=IdentiteResponse)
async def intersect_fonciere(payload: IdentiteFonciereRequest):
    """
    Calcule les intersections à partir d'une géométrie GeoJSON (UF) et/ou d'une parcelle
    déjà stockée (`idu` ou `parcelle_id` dans `{db_schema}.parcelles`).
    """
    try:
        with identite_fonciere_request_context(payload.db_schema):
            geom = resolve_identite_fonciere_geometry(
                payload.geometry,
                idu=payload.idu,
                parcelle_id=payload.parcelle_id,
            )
            result = analyser_identite_fonciere(
                geometry=geom,
                commune=payload.commune,
                insee=payload.insee,
                srid=payload.srid
            )

        return IdentiteResponse(
            success=True,
            **result,
            error=None
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except Exception as e:
        return IdentiteResponse(
            success=False,
            parcelle="UNITE_FONCIERE",
            commune=payload.commune,
            insee=payload.insee or "",
            nb_intersections=0,
            intersections=[],
            error=str(e)
        )


@router_fonciere.post("/intersect/stream")
async def intersect_fonciere_stream(payload: IdentiteFonciereRequest):
    """
    Même analyse que POST /intersect, en SSE : événements `init`, `layer_done` par couche du catalogue,
    puis `complete` avec le même corps que la réponse JSON classique.
    """

    # Générateur **async** : évite `iterate_in_threadpool` (sync iterator) où chaque `next()`
    # peut s'exécuter dans un thread différent → ContextVar / reset token cassés et
    # `get_identite_db_schema()` incohérent entre les couches (ex. Argelès vs latresne).
    async def agen():
        with identite_fonciere_request_context(payload.db_schema):
            try:
                geom = resolve_identite_fonciere_geometry(
                    payload.geometry,
                    idu=payload.idu,
                    parcelle_id=payload.parcelle_id,
                )
                for chunk in iter_identite_fonciere_sse_chunks(
                    geom,
                    payload.commune,
                    payload.insee,
                    payload.srid,
                ):
                    yield chunk
            except ValueError as e:
                yield sse_error_chunk(str(e))
            except Exception as e:
                yield sse_error_chunk(str(e))

    return StreamingResponse(
        agen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router_fonciere.get("/history/by_user")
def identite_fonciere_history_by_user(
    limit: int = 50,
    user_id: str = Depends(get_current_user_id),
):
    """
    Liste les publications CIF d’un utilisateur (carte + PDF), avec centroïde pour la carte.
    Même usage que GET /pipelines/by_user pour le front (session Supabase → user.id).
    """
    return identite_fonciere_history_module.list_identite_fonciere_projects_by_user(
        user_id, limit=limit
    )


@router_fonciere.delete("/history/{project_id}")
def delete_identite_fonciere_history_entry(
    project_id: str,
    user_id: str = Depends(get_current_user_id),
):
    """
    Supprime une publication CIF pour l’utilisateur : ligne `latresne.identite_fonciere_projects`
    et fichiers du dossier Storage `{project_id}/` (carte HTML, PDF, etc.).
    """
    pid = project_id.strip()
    if not _IF_PROJECT_ID_RE.match(pid):
        raise HTTPException(status_code=404, detail="Ressource introuvable.")

    res = identite_fonciere_history_module.delete_identite_fonciere_project_for_user(pid, user_id)
    if not res.get("success"):
        err = str(res.get("error") or "Échec suppression")
        if "introuvable" in err.lower() or "refusé" in err.lower():
            raise HTTPException(status_code=404, detail=err)
        raise HTTPException(status_code=400, detail=err)
    return res


@router_fonciere.get("/map/view/{token}", response_class=HTMLResponse)
async def view_map_fonciere_html(token: str):
    """
    Affiche la carte Folium générée par POST /map (même contenu que le champ `html`).
    Le jeton expire après IDENTITE_FONCIERE_MAP_CACHE_TTL_SEC (défaut 1 h).
    """
    html = _map_html_cache_get(token)
    if not html:
        raise HTTPException(status_code=404, detail="Carte expirée ou introuvable.")
    return HTMLResponse(content=html)


@router_fonciere.get("/public/if/{project_id}/{filename}")
def proxy_identite_fonciere_depuis_storage(project_id: str, filename: str) -> Response:
    """
    Sert la carte HTML ou le PDF déposés dans le bucket identité foncière (Supabase Storage)
    sous une URL de cette API (`…/identite-fonciere/public/if/...`) :
    - liens « propres » sans domaine `*.supabase.co` dans le PDF ;
    - **Content-Type** fiable (`text/html; charset=utf-8` pour la carte), car l’URL publique
      Storage renvoie souvent un type incorrect et le navigateur affiche le HTML comme texte brut.
    Voir `storage_et_urls.py` et `_prefer_identite_proxy_url`.
    """
    if not _IF_PROJECT_ID_RE.match(project_id.strip()):
        raise HTTPException(status_code=404, detail="Ressource introuvable.")
    fn = filename.strip()
    if not _identite_fichier_public_autorise(fn):
        raise HTTPException(status_code=404, detail="Ressource introuvable.")

    path = object_path(project_id, fn)
    src = public_object_url(path)

    try:
        r = requests.get(src, timeout=90)
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Stockage indisponible: {e!s}") from e

    if r.status_code == 404:
        raise HTTPException(status_code=404, detail="Fichier introuvable.")
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Erreur stockage ({r.status_code}).")

    ct = r.headers.get("content-type", "").split(";")[0].strip()
    if fn.endswith(".html"):
        media = "text/html; charset=utf-8"
    elif fn.endswith(".pdf"):
        media = "application/pdf"
    else:
        media = ct or "application/octet-stream"

    return Response(
        content=r.content,
        media_type=media,
        headers={
            "Cache-Control": "public, max-age=300",
            "X-Content-Type-Options": "nosniff",
        },
    )


def _is_supabase_storage_public_url(url: str) -> bool:
    """URL publique directe du bucket (GET sans signature)."""
    u = url.lower()
    return "supabase" in u and "/storage/v1/object/public/" in u


def _proxy_identite_asset_url(request: Request, project_id: str, filename: str) -> str:
    base = _public_api_base_url(request)
    pid = project_id.strip()
    fn = filename.strip().split("/")[-1]
    return f"{base}/api/identite-fonciere/public/if/{pid}/{fn}"


def _is_non_shareable_carte_web_url(url: str) -> bool:
    """
    Liens qui ne fonctionnent que sur un navigateur / session (ex. /maps?ls=… + localStorage).
    Ne pas les mettre dans le PDF : utiliser l’URL proxy API ou Storage à la place.
    """
    u = (url or "").strip().lower()
    if "/maps" in u and ("ls=" in u or "ls%3d" in u):
        return True
    return False


def _prefer_identite_proxy_url(request: Request, project_id: str, filename: str, url: str) -> str:
    """
    Supabase Storage renvoie souvent un Content-Type inadapté pour le HTML (ex. text/plain) :
    le navigateur affiche le code source au lieu du rendu. L’URL proxy `/public/if/...` force
    `text/html; charset=utf-8` ou `application/pdf` (voir `proxy_identite_fonciere_depuis_storage`).
    Si l’URL renvoyée au client est encore l’URL directe Supabase, on la remplace par le proxy.
    """
    if not _is_supabase_storage_public_url(url):
        return url
    return _proxy_identite_asset_url(request, project_id, filename)


def _identite_storage_upload_enabled() -> bool:
    """Désactiver avec IDENTITE_FONCIERE_STORAGE_UPLOAD=0 (ex. dev sans clé service)."""
    return os.getenv("IDENTITE_FONCIERE_STORAGE_UPLOAD", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )


@router_fonciere.post("/map", response_model=IdentiteFonciereMapResponse)
async def map_fonciere(request: Request, payload: IdentiteFonciereMapRequest):
    """
    Génère une carte 2D HTML minimale (Folium) :
    - unité foncière analysée
    - couches intersectées
    - légende simplifiée

    Si Storage est configuré (`SUPABASE_URL` + clé service, bucket identité foncière),
    le HTML est uploadé et `carte_url` pointe vers l’URL persistante (proxy Kerelia ou
    URL directe Storage). Sinon, repli sur le lien temporaire `/map/view/{token}`.

    Le champ `html` reste disponible pour intégration inline.
    """
    try:
        with identite_fonciere_request_context(payload.db_schema):
            geom = resolve_identite_fonciere_geometry(
                payload.geometry,
                idu=payload.idu,
                parcelle_id=payload.parcelle_id,
            )
            res = generate_identite_fonciere_map_html(
                geometry=geom,
                commune=payload.commune,
                insee=payload.insee,
                srid=payload.srid,
                intersections=[i.model_dump() for i in payload.intersections] if payload.intersections else None,
            )
        html = res["html"]
        base = _public_api_base_url(request)
        token = _map_html_cache_put(html)
        carte_url_temp = f"{base}/api/identite-fonciere/map/view/{token}"

        carte_project_id: str | None = None
        carte_url = carte_url_temp

        if _identite_storage_upload_enabled():
            try:
                carte_project_id = new_project_id()
                carte_url = upload_html_carte(carte_project_id, html)
                carte_url = _prefer_identite_proxy_url(
                    request, carte_project_id, "carte.html", carte_url
                )
                _logger.info(
                    "Carte identité foncière enregistrée Storage (project_id=%s)",
                    carte_project_id,
                )
            except Exception as exc:
                _logger.warning(
                    "Upload Storage carte identité foncière indisponible, repli URL temporaire : %s",
                    exc,
                )
                carte_url = carte_url_temp
                carte_project_id = None

        return IdentiteFonciereMapResponse(
            success=True,
            html=html,
            metadata=res["metadata"],
            intersections=res["intersections"],
            error=None,
            carte_url=carte_url,
            carte_project_id=carte_project_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        return IdentiteFonciereMapResponse(
            success=False,
            html="",
            metadata={},
            intersections=[],
            error=str(e),
            carte_url=None,
            carte_project_id=None,
        )


@router_fonciere.get("/rapport/view/{token}")
async def rapport_identite_pdf_view_temp(token: str):
    """
    Sert un PDF généré par POST /publier lorsque l’upload Storage a échoué (lien temporaire, TTL identique à la carte).
    """
    _prune_rapport_pdf_cache()
    item = _RAPPORT_PDF_CACHE.get(token)
    if not item:
        raise HTTPException(status_code=404, detail="Rapport expiré ou introuvable.")
    path, exp = item
    if time.time() > exp:
        del _RAPPORT_PDF_CACHE[token]
        raise HTTPException(status_code=404, detail="Rapport expiré ou introuvable.")
    p = Path(path)
    if not p.is_file():
        del _RAPPORT_PDF_CACHE[token]
        raise HTTPException(status_code=404, detail="Rapport expiré ou introuvable.")
    return FileResponse(
        p,
        media_type="application/pdf",
        filename=p.name,
        headers={"Cache-Control": "no-store"},
    )


@router_fonciere.post("/publier", response_model=PublierIdentiteResponse)
async def publier_identite_fonciere(request: Request, payload: RapportFonciereRequest):
    """
    Génère la carte HTML et le rapport PDF, les dépose sur Storage (même `project_id`) et renvoie les URLs.
    À appeler après l’analyse SSE (même corps que POST /rapport) : les boutons front n’ont plus qu’à ouvrir ces liens.

    Le lien « carte web » dans le PDF est toujours une URL **partageable** (proxy `/public/if/…` ou équivalent),
    jamais un lien `/maps?ls=…` (localStorage, non valide pour un tiers).

    Si Storage est indisponible : `carte_url` / `pdf_url` pointent vers des URLs temporaires de cette API.
    """
    warnings: List[str] = []
    try:
        with identite_fonciere_request_context(payload.db_schema):
            result = _build_result_dict_from_rapport_payload(payload)
            geom = payload.geometry if payload.geometry is not None else result.get("geometry")
            if not isinstance(geom, dict) or "type" not in geom:
                raise HTTPException(
                    status_code=400,
                    detail="Géométrie GeoJSON requise pour publier la carte (envoyer `geometry` dans le corps).",
                )

            base = _public_api_base_url(request)
            res_map = generate_identite_fonciere_map_html(
                geometry=geom,
                commune=payload.commune,
                insee=payload.insee,
                srid=payload.srid or result.get("srid"),
                intersections=result.get("intersections"),
            )
        html = res_map["html"]
        token_map = _map_html_cache_put(html)
        carte_url_temp = f"{base}/api/identite-fonciere/map/view/{token_map}"

        project_id = new_project_id()
        carte_url = carte_url_temp
        if _identite_storage_upload_enabled():
            try:
                carte_url = upload_html_carte(project_id, html)
                carte_url = _prefer_identite_proxy_url(
                    request, project_id, "carte.html", carte_url
                )
            except Exception as exc:
                warnings.append(f"Carte Storage : {exc}")
                _logger.warning("Upload Storage carte (publier) : %s", exc)
                carte_url = carte_url_temp
        else:
            warnings.append("IDENTITE_FONCIERE_STORAGE_UPLOAD désactivé : URL carte temporaire.")

        result["carte_web_url"] = carte_url

        output_dir = payload.output_dir or "./rapports_identite"
        with identite_fonciere_request_context(payload.db_schema):
            pdf_path = generate_rapport_pdf(
                result,
                output_dir=output_dir,
                catalogue=get_catalogue(),
            )

        pdf_url: str | None = None
        if _identite_storage_upload_enabled():
            try:
                pdf_url = upload_pdf_rapport(project_id, pdf_path)
                pdf_url = _prefer_identite_proxy_url(
                    request,
                    project_id,
                    "rapport_identite_fonciere.pdf",
                    pdf_url,
                )
            except Exception as exc:
                warnings.append(f"PDF Storage : {exc}")
                _logger.warning("Upload Storage PDF (publier) : %s", exc)
                pdf_url = _rapport_pdf_cache_put(pdf_path, base)
        else:
            pdf_url = _rapport_pdf_cache_put(pdf_path, base)

        history_recorded = identite_fonciere_history_module.record_identite_fonciere_project(
            project_id=project_id,
            user_id=(payload.user_id or "").strip() or None,
            user_email=(payload.user_email or "").strip() or None,
            commune=str(payload.commune or ""),
            insee=str(payload.insee or ""),
            parcelle_label=(result.get("parcelle") or None),
            parcelles_cadastrales=result.get("parcelles_cadastrales"),
            geometry=geom,
            srid=payload.srid or result.get("srid"),
            carte_url=carte_url or "",
            pdf_url=pdf_url or "",
            nb_intersections=int(result.get("nb_intersections") or 0),
        )

        return PublierIdentiteResponse(
            success=True,
            carte_project_id=project_id,
            carte_url=carte_url,
            pdf_url=pdf_url,
            error=None,
            warnings=warnings or None,
            history_recorded=history_recorded,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        _logger.exception("Échec POST /publier identité foncière")
        raise HTTPException(status_code=500, detail=str(e))


@router_fonciere.post("/rapport")
async def rapport_fonciere(payload: RapportFonciereRequest):
    """
    Génère le rapport PDF d'identité foncière.
    Si intersections fournies dans le payload : utilisées directement (évite de recalculer).
    Sinon : lance l'analyse complète puis génère le PDF.
    """
    try:
        with identite_fonciere_request_context(payload.db_schema):
            result = _build_result_dict_from_rapport_payload(payload)

            if payload.carte_web_url and str(payload.carte_web_url).strip():
                cu = str(payload.carte_web_url).strip()
                if not _is_non_shareable_carte_web_url(cu):
                    result["carte_web_url"] = cu

            output_dir = payload.output_dir or "./rapports_identite"
            pdf_path = generate_rapport_pdf(
                result,
                output_dir=output_dir,
                catalogue=get_catalogue(),
            )

        return FileResponse(
            pdf_path,
            media_type="application/pdf",
            filename=Path(pdf_path).name,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        _logger.exception("Échec génération rapport PDF identité foncière")
        raise HTTPException(status_code=500, detail=str(e))