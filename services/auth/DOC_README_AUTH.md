# Revue d'authentification & multitenant — Kerelia CUA

> Document de reprise. Décrit ce qui a changé dans l'authentification, comment ça
> se répercute sur les endpoints, les fichiers concernés, et les dettes restantes.
> Daté de la session de refonte. À relire avant de retravailler sur l'auth.

---

## 1. Le problème de départ

Deux besoins mélangés au début :

1. **Onboarding sans création manuelle** : permettre à tout agent d'une collectivité
   (ex. tout mail `@ville-argelessurmer.fr`) de se connecter et de partager le même
   périmètre commune que ses collègues, sans que tu crées chaque compte à la main.
2. **Sécuriser le multitenant** : découvert en cours de route, le système n'avait
   **aucune authentification réelle** côté API — l'identité était déclarative
   (`?user_id=...` en query, cru sur parole par le backend).

Le second point a pris la priorité : tout le contrôle d'accès par commune reposait
sur une identité falsifiable. On a donc posé une vraie authentification d'abord,
puis branché l'autorisation dessus.

---

## 2. Principe directeur : identité prouvée + fail-closed

Deux règles qui guident tout le reste :

- **L'identité vient du token, jamais du client.** Le front n'envoie plus `user_id`.
  Il envoie son JWT Supabase dans `Authorization: Bearer <token>`. Le backend vérifie
  le token et en extrait le `user_id` lui-même.
- **Fail-closed** : en cas d'absence d'information (pas de droits, panne, pas d'identité),
  le défaut est **refuser**, jamais autoriser. Un trou de vérification doit fermer la
  porte, pas l'ouvrir.

---

## 3. Brique d'authentification

### Fichier : `services/auth/current_user.py`  *(nouveau)*

Calqué sur la dépendance existante du PLU agent (`api/agents/plu_agent/routes/plu_auth.py`,
fonction `get_plu_user_id`), qui était déjà saine.

Contient :
- `AuthenticatedUser(id, email)` — objet identité.
- `verify_supabase_access_token(token)` — appelle `GET {SUPABASE_URL}/auth/v1/user`
  avec le token + la `SERVICE_KEY` en `apikey`. Supabase valide signature + expiration.
  Renvoie l'`id` **et** l'`email` lus depuis la réponse vérifiée.
- `get_current_user()` — dépendance FastAPI, renvoie `AuthenticatedUser`.
- `get_current_user_id()` — délègue à `get_current_user().id` (un seul appel réseau).

**Mode validation** : par appel réseau à Supabase (`/auth/v1/user`), pas par validation
locale du JWT. Choix assumé : homogène avec le PLU agent, pas de gestion de secret à
maintenir. Le `SUPABASE_JWT_SECRET` est présent dans le `.env` mais **non utilisé** ici —
il servira si un jour on passe en validation locale (PyJWT) pour réduire la latence.

**Variable d'env** : `REQUIRE_AUTH`
- `REQUIRE_AUTH=0` → bypass (dev local), renvoie un UUID nul. Calqué sur `PLU_REQUIRE_AUTH`.
- sinon (prod) → token obligatoire, 401 si absent/invalide.
- ⚠️ **En prod, `REQUIRE_AUTH` doit valoir 1** (ou être absent, le défaut est 1).

**Dépend de** : `SUPABASE_URL` + `SERVICE_KEY` (ou `SUPABASE_SERVICE_ROLE_KEY`) présents
dans les vars d'env Render. Sans elles → 503.

---

## 4. Autorisation par commune

### Fichier : `services/auth/commune_access.py`

C'est le cœur du multitenant. Source de vérité : la table `public.user_commune_access`
(une ligne par (user, commune), avec `code_insee` et `role`).

**Convention de retour de `get_authorized_insee_codes(user_id)`** :
- `None` → **superadmin uniquement** (accès toutes communes), décidé explicitement
  via `role = 'superadmin'`.
- `[]` → **aucun accès** (fail-closed).
- liste non vide → communes autorisées (codes INSEE).

**Corrections fail-closed appliquées (important)** :
- zéro ligne dans `user_commune_access` → renvoie `[]` (avant : `None` = accès global ⚠️).
- fallback metadata vide → `[]` (avant : `None`).
- `return codes or None` → `return codes` (une liste vide reste vide, pas global).
- `get_authorized_commune_slugs` : `return slugs or None` → `return slugs` (idem).
- `is_authorized_for_insee` : `if not user_id: return False` (avant : `return True` ⚠️).
- `assert_authorized_for_insee` / `assert_authorized_for_commune_slug` :
  si `REQUIRE_AUTH` actif et `user_id` absent → **401** (avant : early return silencieux
  qui laissait tout passer).

**Cloison superadmin** (fonctions du même fichier) :
- `get_superadmin_user_ids()` — UUID des comptes `role = 'superadmin'`.
- `is_pipeline_visible_to_viewer(pipeline, viewer)` — un CUA créé par un superadmin
  n'est visible que par les superadmins (sert à masquer les dossiers de test Kerelia
  aux agents). Les CUA créés par des agents normaux sont partagés dans la commune.
- `filter_pipelines_for_viewer(pipelines, viewer)` — applique ça sur une liste.
- `assert_pipeline_visible_to_viewer(pipeline, viewer)` — version bloquante (404).

> Note : un superadmin a `get_authorized_insee_codes = None` → voit toutes les communes,
> tous les créateurs. C'est voulu. Conséquence : **un superadmin ne doit jamais générer
> de CUA destiné à un agent**, car ce CUA serait masqué aux agents par la cloison.

---

## 5. Scope : de « par user » à « par commune »

Avant, chaque agent ne voyait que **ses propres** CUA (`.eq("user_id", user_id)`).
Objectif métier : un agent voit tous les CUA de **sa commune** (donc ceux de ses
collègues, ex. ceux de `d.winzer`), tout en gardant la traçabilité du créateur.

### Fichier : `services/auth/pipelines_query.py`
- `select_pipelines_for_user` : retrait du `.eq("user_id", ...)`, scope par `code_insee`
  via `get_authorized_insee_codes`. **Garde-fou** : si pas de communes autorisées → `[]`
  (ne renvoie pas tout).
- `apply_access_filters` : filtre `code_insee in (communes autorisées)`.
- Le résultat passe par `filter_pipelines_for_viewer` (cloison superadmin).

La traçabilité est préservée : `pipelines.user_id` / `user_email` restent écrits par
ligne, jamais modifiés. « Qui a généré quoi » reste exact ; seul le périmètre de
*visibilité* est passé à la commune.

### Autorisation écriture (édition / suppression / suivi)
Avant : barrière « propriétaire » (`owner_id == user_id`) qui cassait le partage —
un agent ne pouvait pas modifier le CUA d'un collègue.
Après : autorisation **par commune** de la pipeline ciblée.
- Fichier `services/history/project_management.py` : `_assert_can_modify` vérifie
  l'accès commune (via `code_insee` / `commune_slug`), plus l'appartenance.
- `services/history/suivi.py` : même logique, multi-schéma (Argelès/Latresne/Mios).

---

## 6. Endpoints migrés (token obligatoire)

Motif appliqué partout :
```python
from services.auth.current_user import get_current_user_id  # ou get_current_user
# AVANT : def endpoint(user_id: str, ...)        ← query param, déclaratif
# APRÈS : def endpoint(..., user_id: str = Depends(get_current_user_id))
```
Le corps de la fonction ne change pas : il reçoit enfin un `user_id` **prouvé**.

| Endpoint | Fichier | État |
|---|---|---|
| `GET /pipelines/by_user` | `services/history/centroid_history.py` (~29) | ✅ migré |
| `GET /pipelines/map-history` | `services/history/centroid_history.py` (~64) | ✅ migré (aucun appel front) |
| `GET /account/commune-access` | `app/routers/site_account.py` (~105) | ✅ migré (critique : décide les droits) |
| `PATCH /pipelines/{slug}` | `services/history/project_management.py` (~102) | ✅ migré |
| `DELETE /pipelines/{slug}` | `services/history/project_management.py` (~143) | ✅ migré |
| `PATCH /pipelines/{slug}/suivi` | `services/history/suivi.py` (~49) | ✅ migré |
| `GET /pipelines/by_slug` | `app/routers/pipelines_supabase.py` (~52) | ✅ migré + `assert_can_view_pipeline` |
| `GET /pipelines/{slug}/files` | `services/history/project_directory.py` (~124) | ✅ migré + `assert_can_view_pipeline` |
| `GET /api/identite-fonciere/history/by_user` | `api/identite_fonciere/route_identite_parcelle.py` (~526) | ✅ migré |
| `DELETE /api/identite-fonciere/history/{project_id}` | idem (~537) | ✅ migré |
| `POST /communes/{slug}/cua/generate` | `api/cuas/argeles/cua_router.py` (~41) | ✅ migré (voir §7) |

### `assert_can_view_pipeline` (fonction d'autorisation partagée)
Dans `services/history/project_management.py`. Utilisée par `by_slug` ET `/files`
(une seule porte, pour ne pas diverger). Vérifie dans l'ordre :
1. pipeline existe → sinon **404**
2. `is_pipeline_visible_to_viewer` (cloison superadmin) → refus = **404**
3. `get_authorized_insee_codes` : si périmètre restreint et INSEE hors périmètre → **404**

> **404 et non 403** : ne révèle pas l'existence d'un projet d'une autre commune.
> Ex : agent Latresne ouvrant `/latresne/cua/projects/<slug_argeles>` → 404.

---

## 7. Génération de CUA — fermeture de la faille d'écriture

C'était le trou le plus grave restant : `user_id` / `user_email` étaient dans le **body
Pydantic**, donc usurpables (générer un CUA « au nom de » n'importe qui, sans contrôle
d'accès car `assert_*` faisait early-return sur `user_id=None`).

### Fichier : `api/cuas/argeles/cua_router.py`
- `user_id` et `user_email` **retirés** du modèle `GenerateCuaRequest`.
- Endpoint passe `user: AuthenticatedUser = Depends(get_current_user)`.
- `assert_authorized_for_commune_slug(user.id, slug)` s'exécute **toujours**
  (plus de `None` possible).
- Propagation : `generate_cua_for_parcelles(user_id=user.id, user_email=user.email, ...)`
  → `user_id`/`user_email` viennent du token jusqu'à `persist_cua` (table `pipelines`).

Traçabilité désormais fondée sur une identité vérifiée de bout en bout.

---

## 8. Côté front

### Fichier : `src/api/apiFetch.ts`  *(nouveau)*
Helper central. Lit la session Supabase, injecte `Authorization: Bearer <access_token>`,
préfixe `API_BASE`. Tous les appels protégés passent par lui (plus de `?user_id=`).

```ts
export async function apiFetch(path, init = {}) {
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  const headers = new Headers(init.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(`${API_BASE}${path}`, { ...init, headers });
}
```

### Pages migrées (appels passés en `apiFetch`, `user_id` retiré des URLs/bodies)
- `ArgelesPage.tsx` : `refreshHistoryPipelines`, `handleUpdate/DeleteHistoryProject`,
  suivi, `refreshIdentiteFonciereHistory`, `handleDeleteIdentiteProject`.
- `LatresnePage.tsx` : mêmes appels (parité), `commune_slug=latresne`.
- `ProjectPage.tsx` (réexporté par Argelès) : `loadAll` → `by_slug` + `files` en `apiFetch`.
- Génération : `generateArgelesCua.ts`, `ParcelleCuaGenerateAction.tsx`,
  `UniteFonciereCard.tsx` (`runGenerateCua`) — body sans `user_id`/`user_email`.

### Fichier : `src/auth/communeAccess.ts` — fail-closed
`accessFromMetadata` corrigé : un échec d'appel à `commune-access` ne donne plus
l'accès global. Sans info fiable → `{ allowedSlugs: [], unrestricted: false }`.
Conséquence : 401/panne sur `commune-access` → redirection `/login`, pas accès global.

### Redirection post-login (inchangé, déjà correct)
`resolvePostLoginPath` : une seule commune → `/{slug}/cua` ; plusieurs → premier slug ;
superadmin (`unrestricted`) → `/`.

---

## 9. Onboarding self-service (objectif initial)

- **Self-signup activé** + **confirmation email activée** (dashboard Supabase).
  La confirmation email est le garde-fou : elle prouve la possession de l'adresse,
  ce qui rend sûr le filtrage par domaine.
- **Table `public.commune_domains`** : mapping `domain → commune_slug + code_insee`.
  Ex : `ville-argelessurmer.fr → argeles / 66008`.
- **Trigger `handle_new_user`** (`after insert on auth.users`, mode bloquant) :
  - domaine connu → crée la ligne `user_commune_access` automatiquement.
  - domaine inconnu → `raise exception` (signup refusé).
- **Vue `user_commune_access_view`** : `user_commune_access` + email (confort SQL).
- ⚠️ **`SignupForm` côté front non finalisé** : le motif a été fourni (appel
  `supabase.auth.signUp`, écran « vérifiez votre mail », traduction de l'erreur
  « Domaine non autorisé »), mais à brancher/router (`/signup`) quand tu reprends.
  À tester : qu'un domaine non autorisé ne crée pas de compte fantôme dans `auth.users`
  (si ça arrive, passer le filtrage domaine en `before insert`).

---

## 10. Dettes de sécurité restantes (À TRAITER)

| # | Dette | Gravité | Détail |
|---|---|---|---|
| 1 | **CERFA / `cua_pipeline.py` / `cerfa.py`** | 🔴 Élevée | Endpoints d'**écriture** (génération CUA, voie legacy) montés dans `main.py`, **sans auth**, appelables hors UI. Jumelle de la faille fermée sur Argelès. « Inutilisé par l'UI » ≠ « inaccessible ». **À fermer ou retirer du routeur**, surtout avant d'activer le signup public. |
| 2 | **`files/upload` (POST) & `files/{id}` DELETE** | ✅ Corrigé | `Depends(get_current_user_id)` + `assert_can_view_pipeline` avant tout traitement ; front `ProjectPage` via `apiFetch`. Même règle que la lecture : agent avec accès commune au projet. |
| 3 | **`project_directory` ensure** | 🟠 Moyenne | `POST /{slug}/directory/ensure` — `user_id` encore en query déclaratif (appel interne / legacy). |
| 4 | **Bucket `visualisation` public** | 🟡 Acceptée | Policy `allow_public_read` : les DOCX sont accessibles par URL directe si connue. OK pour des CUA (publics par nature). À revoir seulement si stockage de confidentiel. Pour confidentialité réelle → signed URLs. |
| 5 | **MiosPage / MainApp / HistoryPanel** | 🟡 À vérifier | Pages non migrées. Si routes mortes : sans risque. Si accessibles : 401 silencieux pour l'utilisateur → à migrer (même motif) ou retirer du routeur. |
| 6 | **Migration JWT asymétrique (JWKS)** | 🟢 Optionnelle | Validation actuelle = appel réseau à `/auth/v1/user`. Passer en validation locale (PyJWT + `SUPABASE_JWT_SECRET` déjà au `.env`) réduirait la latence. Changement isolé dans `get_current_user`, pas dans les routeurs. |

---

## 11. Tests de non-régression (à rejouer après modif auth)

1. **401 sans token** : `curl` sur `/pipelines/by_user`, `/account/commune-access`,
   `PATCH/DELETE /pipelines/{slug}`, `by_slug`, `/files`, `cua/generate` sans header
   `Authorization` → tous **401**.
2. **`user_id` bidon en query sans token** → toujours **401** (le query param n'est plus lu).
3. **Login réel** : déconnexion complète → login agent → redirection `/{commune}/cua`.
4. **Partage commune** : agent argeles voit les CUA de `d.winzer`, peut les éditer,
   changer le suivi (pas de 403).
5. **Isolation inter-commune** : agent Latresne sur un slug Argelès → **404**.
6. **Cloison superadmin** : agent ne voit pas les CUA créés par un superadmin ;
   superadmin voit tout.
7. **Fail-closed** : un user authentifié sans ligne `user_commune_access` → voit `[]`,
   pas tout.

---

## 12. Glossaire fichiers (repère rapide)

| Fichier | Rôle |
|---|---|
| `services/auth/current_user.py` | Dépendance auth (token → identité vérifiée) |
| `services/auth/commune_access.py` | Droits par commune + cloison superadmin |
| `services/auth/pipelines_query.py` | Lecture pipelines scopée commune |
| `services/history/project_management.py` | PATCH/DELETE + `assert_can_view_pipeline` |
| `services/history/suivi.py` | Suivi dossier (multi-schéma) |
| `services/history/centroid_history.py` | Historique carte (by_user, map-history) |
| `services/history/project_directory.py` | Fichiers projet (`/files`, upload, delete) |
| `app/routers/pipelines_supabase.py` | by_slug, latest |
| `app/routers/site_account.py` | commune-access (droits post-login) |
| `api/cuas/argeles/cua_router.py` | Génération CUA Argelès |
| `api/agents/plu_agent/routes/plu_auth.py` | Auth PLU agent (modèle d'origine) |
| `src/api/apiFetch.ts` | Helper front (injection token) |
| `src/auth/communeAccess.ts` | Résolution droits + redirection (front, fail-closed) |
| `main.py` | Montage routeurs (⚠️ contient encore les endpoints dette #1) |