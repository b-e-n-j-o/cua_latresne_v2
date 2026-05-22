# Prompts système par commune

Fichiers Markdown chargés par `commune_profile.load_prompt()` dans chaque profil (`argeles.py`, `latresne.py`).

| Fichier | Profil | Rôle |
|---------|--------|------|
| `argeles_system.md` | `ARGELES_PROFILE` | Consignes LLM Argelès-sur-Mer (production) |
| `latresne_system.md` | `LATRESNE_PROFILE` | Consignes LLM Latresne (client séparé) |

Modifier le comportement conversationnel d'une commune **sans toucher** au code Python des routes : éditer le `.md` correspondant, redémarrer l'API si nécessaire.
