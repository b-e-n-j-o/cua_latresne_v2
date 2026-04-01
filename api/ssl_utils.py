"""
Vérification TLS pour les clients HTTP (ex. WFS IGN data.geopf.fr).

Contrainte terrain:
- certains environnements locaux macOS/Python échouent en
  CERTIFICATE_VERIFY_FAILED sur data.geopf.fr.

Par défaut on désactive donc la vérification SSL pour éviter de bloquer
les workflows UF en local. Pour la réactiver explicitement:
  export IGN_WFS_SSL_VERIFY=true
"""
import os


def ssl_verify_for_requests() -> bool:
    v = os.getenv("IGN_WFS_SSL_VERIFY", os.getenv("REQUESTS_SSL_VERIFY", "false")).strip().lower()
    return v in ("1", "true", "yes", "on")
