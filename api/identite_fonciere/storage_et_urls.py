"""
Stockage Supabase Storage + URLs publiques pour le rapport PDF d’identité foncière
et la carte HTML Folium associée (un « projet » = un dossier = PDF + HTML).

Configuration (variables d’environnement) :
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY  ou  SERVICE_KEY  (rôle service : upload)
  IDENTITE_FONCIERE_STORAGE_BUCKET  (défaut : identite-fonciere)

---------------------------------------------------------------------------
Création du bucket Supabase (à faire une fois dans le dashboard)
---------------------------------------------------------------------------

1. **Dashboard** → **Storage** → **New bucket**
2. **Name** : `identite-fonciere` (ou la valeur de `IDENTITE_FONCIERE_STORAGE_BUCKET`)
3. **Public bucket** : **Oui** — pour que les URLs publiques
   `{SUPABASE_URL}/storage/v1/object/public/{bucket}/...`
   fonctionnent sans signature (adapté aux liens dans un PDF partagé).
4. **File size limit** : ajuster si besoin (PDF + HTML Folium peuvent être volumineux).
5. **Allowed MIME types** : laisser vide pour tout autoriser, ou restreindre à
   `application/pdf`, `text/html` selon votre politique.

Si le bucket n’est pas public, il faudrait des **signed URLs** (TTL) — moins adapté
à un lien « à vie » dans un PDF.

**Policies (RLS)** : pour un bucket public, Supabase crée en général les règles
permettant la lecture publique des objets. En cas d’erreur 403 au GET :

- Storage → Policies → s’assurer que **lecture anonyme** (SELECT) est autorisée
  sur `storage.objects` pour ce bucket, ou utiliser la doc officielle
  « Public buckets ».

---------------------------------------------------------------------------
HTML affiché correctement dans le navigateur
---------------------------------------------------------------------------

Le navigateur doit recevoir `Content-Type: text/html; charset=utf-8` (pas
`application/octet-stream`), sinon téléchargement ou texte brut. Les uploads
ci-dessous fixent explicitement le type pour `.html` et `.pdf`.

---------------------------------------------------------------------------
URLs « propres » (sans domaine supabase.co dans le PDF)
---------------------------------------------------------------------------

Définir **`IDENTITE_FONCIERE_PUBLIC_BASE_URL`** (ex. `https://kerelia.fr` ou
l’URL publique qui pointe vers cette API derrière le reverse proxy).

Les fonctions d’upload renvoient alors une URL du type :
`{IDENTITE_FONCIERE_PUBLIC_BASE_URL}/api/identite-fonciere/public/if/{project_id}/{fichier}`

La route **GET** `/api/identite-fonciere/public/if/{project_id}/{filename}`
(dans `route_identite_parcelle.py`, fonction `proxy_identite_fonciere_depuis_storage`)
proxifie le fichier depuis Storage : l’utilisateur ne voit pas `supabase` dans la
barre d’adresse. Sans cette variable, repli sur l’URL Storage directe.

---------------------------------------------------------------------------
Intégration prévue (à brancher dans le routeur / rapport)
---------------------------------------------------------------------------

- Après génération du HTML carte : `upload_html_carte(...)` → URL dans
  `result["carte_web_url"]` pour le PDF et la réponse API.
- Après génération du PDF : `upload_pdf_rapport(...)` → ex. `result["identite_pdf_url"]`.
- Utiliser un **même** `project_id` (slug UUID) pour les deux fichiers afin de
  les retrouver côte à côte sous `{project_id}/`.
"""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path
from typing import Any, List, Optional

# Client lazy pour éviter l’import au chargement du module si non utilisé
_supabase_client: Any = None


def _bucket_name() -> str:
    return (os.getenv("IDENTITE_FONCIERE_STORAGE_BUCKET") or "identite-fonciere").strip()


def _supabase_url() -> str:
    return (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")


def _service_key() -> str:
    return (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SERVICE_KEY") or "").strip()


def get_supabase_client():
    """Client Supabase (service role) pour Storage ; singleton lazy."""
    global _supabase_client
    if _supabase_client is not None:
        return _supabase_client
    if not _supabase_url() or not _service_key():
        raise RuntimeError(
            "SUPABASE_URL et SUPABASE_SERVICE_ROLE_KEY (ou SERVICE_KEY) sont requis pour le stockage identité foncière."
        )
    from supabase import create_client

    _supabase_client = create_client(_supabase_url(), _service_key())
    return _supabase_client


def new_project_id() -> str:
    """Identifiant unique de dossier (préfixe lisible + UUID court)."""
    return f"if_{uuid.uuid4().hex[:16]}"


def _sanitize_segment(name: str) -> str:
    s = name.strip().replace("..", "").replace("\\", "/")
    return re.sub(r"[^a-zA-Z0-9._/-]+", "_", s) or "fichier"


def object_path(project_id: str, filename: str) -> str:
    """Chemin objet dans le bucket : `{project_id}/{filename}`."""
    pid = _sanitize_segment(project_id).strip("/")
    fn = _sanitize_segment(filename)
    return f"{pid}/{fn}"


def public_object_url(remote_path: str) -> str:
    """URL publique directe Supabase (bucket public uniquement)."""
    base = _supabase_url()
    bucket = _bucket_name()
    rp = remote_path.lstrip("/")
    return f"{base}/storage/v1/object/public/{bucket}/{rp}"


def _public_front_base() -> str:
    """Base affichée dans les PDF / mails (domaine Kerelia, pas Supabase)."""
    return (
        os.getenv("IDENTITE_FONCIERE_PUBLIC_BASE_URL")
        or os.getenv("PUBLIC_API_BASE_URL")
        or ""
    ).strip().rstrip("/")


def friendly_identite_asset_url(project_id: str, filename: str) -> Optional[str]:
    """
    URL « neutre » vers notre API (proxy), sans supabase.co.
    None si aucune base publique n’est configurée.
    """
    base = _public_front_base()
    if not base:
        return None
    pid = _sanitize_segment(project_id).strip("/")
    fn = _sanitize_segment(filename)
    return f"{base}/api/identite-fonciere/public/if/{pid}/{fn}"


def public_display_url(project_id: str, filename: str) -> str:
    """
    URL à utiliser dans le PDF : friendly si `IDENTITE_FONCIERE_PUBLIC_BASE_URL`
    (ou `PUBLIC_API_BASE_URL`) est défini, sinon URL Storage directe.

    Attention : l’URL Storage directe peut être servie avec un mauvais ``Content-Type`` pour
    le HTML (affichage « code source »). Les routes API réécrivent alors vers le proxy
    ``/public/if/...`` lorsque c’est possible (voir ``route_identite_parcelle``).
    """
    u = friendly_identite_asset_url(project_id, filename)
    if u:
        return u
    return public_object_url(object_path(project_id, filename))


def upload_bytes(
    remote_path: str,
    data: bytes,
    *,
    content_type: str,
    upsert: bool = True,
    display_project_id: Optional[str] = None,
    display_filename: Optional[str] = None,
) -> str:
    """
    Upload brut vers le bucket identité foncière.
    Si `display_project_id` + `display_filename` sont fournis, retourne
    `public_display_url` (lien domaine Kerelia si configuré), sinon l’URL Storage.
    """
    client = get_supabase_client()
    bucket = _bucket_name()
    opts: dict[str, Any] = {
        "content-type": content_type,
        "cache-control": "public, max-age=3600",
        "upsert": "true" if upsert else "false",
    }
    # API alignée sur fetch_plu / sub_orchestrator (supabase-py 2.x)
    client.storage.from_(bucket).upload(remote_path, data, file_options=opts)
    if display_project_id and display_filename:
        return public_display_url(display_project_id, display_filename)
    return public_object_url(remote_path)


def upload_html_carte(project_id: str, html: str, *, filename: str = "carte.html") -> str:
    """Enregistre la carte Folium (HTML) et renvoie l’URL à afficher (friendly si configuré)."""
    path = object_path(project_id, filename)
    raw = html.encode("utf-8")
    return upload_bytes(
        path,
        raw,
        content_type="text/html; charset=utf-8",
        display_project_id=project_id,
        display_filename=filename,
    )


def upload_pdf_rapport(project_id: str, pdf_path: str | Path, *, filename: str = "rapport_identite_fonciere.pdf") -> str:
    """Enregistre le PDF généré et renvoie l’URL à afficher (friendly si configuré)."""
    p = Path(pdf_path)
    data = p.read_bytes()
    path = object_path(project_id, filename)
    return upload_bytes(
        path,
        data,
        content_type="application/pdf",
        display_project_id=project_id,
        display_filename=filename,
    )


def upload_html_carte_from_file(project_id: str, html_path: str | Path, *, filename: Optional[str] = None) -> str:
    """Upload depuis un fichier HTML déjà écrit sur disque."""
    p = Path(html_path)
    name = filename or p.name
    return upload_html_carte(project_id, p.read_text(encoding="utf-8"), filename=name)


def delete_identite_fonciere_storage_prefix(project_id: str) -> List[str]:
    """
    Supprime les objets sous `{project_id}/` dans le bucket identité foncière
    (liste Storage + chemins connus carte.html / rapport PDF).
    Retourne les chemins effectivement supprimés (relatifs au bucket).
    """
    pid = _sanitize_segment(project_id).strip("/")
    if not pid:
        return []
    client = get_supabase_client()
    bucket = _bucket_name()
    storage = client.storage.from_(bucket)
    paths_set: set[str] = set()
    try:
        entries = storage.list(pid)
        for item in entries or []:
            if isinstance(item, dict):
                name = item.get("name")
            else:
                name = getattr(item, "name", None)
            if name and str(name).strip():
                paths_set.add(f"{pid}/{_sanitize_segment(str(name))}")
    except Exception:
        pass
    paths_set.add(object_path(pid, "carte.html"))
    paths_set.add(object_path(pid, "rapport_identite_fonciere.pdf"))
    paths = list(paths_set)
    removed: List[str] = []
    try:
        storage.remove(paths)
        return paths
    except Exception:
        for p in paths:
            try:
                storage.remove([p])
                removed.append(p)
            except Exception:
                continue
        return removed


__all__ = [
    "friendly_identite_asset_url",
    "get_supabase_client",
    "new_project_id",
    "object_path",
    "public_display_url",
    "public_object_url",
    "upload_bytes",
    "upload_html_carte",
    "upload_pdf_rapport",
    "upload_html_carte_from_file",
    "delete_identite_fonciere_storage_prefix",
]
