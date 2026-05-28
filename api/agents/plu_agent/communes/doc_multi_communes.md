## Multi-commune PLU — processus (résumé)

### Principe
**Un seul code métier**, plusieurs **clients** isolés par :
- URL : `/api/plu/argeles/…` vs `/api/plu/latresne/…`
- BDD : schéma PostgreSQL `argeles.*` vs `latresne.*`
- Config : prompt + catalogue de couches JSON

### Chaîne HTTP
1. `main.py` monte `argeles_router` + `latresne_router` (factory `create_plu_router(profile)`).
2. Chaque route est enregistrée via `register(router, profile, bind)` — **même handlers**, profil différent.
3. `profile_guard` / `bind` pose le `CommuneProfile` dans une **ContextVar** pour toute la requête.
4. Le code lit `get_current_profile()` ou `q("parcelles")` → `latresne.parcelles` (plus de schéma en dur).

### Fichiers clés
| Rôle | Où |
|------|-----|
| Définition profil | `communes/argeles.py`, `latresne.py` → `CommuneProfile` |
| Profil actif (requête) | `commune_context.py` |
| Routes partagées | `routes/*` + `api.py` |
| Couches (JSON) | `communes/catalogs/default.json` + `{slug}.json` |
| SQL spatial / tools LLM | `tools/utils/*` + `catalog_bridge.py` |
| Contexte LLM + carte | `cartography/spatial_context.py`, `cartography/carto.py` |

### Catalogue de couches
- **default.json** = socle GPU (toutes communes).
- **{slug}.json** = surcharges : `enabled: false`, autre `table`, couches extra (ex. PPRT).
- Au runtime : `profile.catalog` → prescriptions / servitudes / infos / carte / contexte parcelle.

### Hors HTTP (tests CLI)
Pas de ContextVar → `current_schema()` défaut **`argeles`** ; chemins « legacy » ou catalogue via ce défaut.

### Tools LLM par commune
Chaque `CommuneProfile` définit `llm_tool_names` : seuls ces tools sont déclarés à Gemini
et branchés dans le dispatch (`routes/chat.py`). Ex. `get_reglement_zone` uniquement si
la table `{schema}.plu_reglement` existe (Latresne) ; Argelès garde la liste par défaut.

### Ajouter une commune (checklist)
1. Schéma BDD + tables GPU / sessions (`plu_sessions`, `plu_messages`).
2. `communes/<slug>.py` + entrée registre `communes/__init__.py`.
3. `catalogs/<slug>.json` + prompt `prompts/<slug>_system.md`.
4. `llm_tool_names` dans le profil (optionnel : `get_reglement_zone` si `plu_reglement`).
5. `create_plu_router(...)` + `include_router` dans `main.py`.
6. Frontend : route `/api/plu/{slug}/…` (ex. `communeConfig.ts`).