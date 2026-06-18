# Accès utilisateurs et communes — Kerelia CUA

Ce document décrit comment sont gérés les **utilisateurs**, leurs **droits par commune**, et comment accéder aux **pages** et **documents** (CUAs, projets).

---

## Vue d'ensemble

```
Utilisateur (Supabase Auth)
        │
        ▼
public.user_commune_access     ← source de vérité (après migration SQL)
  • user_id
  • commune_slug  (latresne | argeles | mios)
  • code_insee    (33234 | 66008 | 33531)
  • role          (user | admin_commune | superadmin)
        │
        ▼
Contrôle API (FastAPI)
  • POST /communes/{slug}/cua/generate
  • GET  /pipelines/by_user
  • GET  /pipelines/by_slug
        │
        ▼
public.pipelines             ← historique CUA toutes communes
  • slug, user_id, commune_slug, code_insee, output_cua, …
```

**Données géographiques** (parcelles, PLU, couches) : toujours dans un schéma **par commune** (`latresne.*`, `argeles.*`, `mios.*`).

**Historique des CUAs** : table unique **`public.pipelines`** (après migration).

---

## Rôles

| Rôle | Description |
|------|-------------|
| `user` | Accès standard : carte, génération CUA, historique **sur les communes autorisées** uniquement. |
| `admin_commune` | Même périmètre qu'`user` aujourd'hui ; réservé pour futures fonctions d'administration locale. |
| `superadmin` | Accès à **toutes** les communes (équivalent « pas de restriction »). |

Les rôles sont stockés dans **`public.user_commune_access.role`**.

Un utilisateur peut avoir **plusieurs lignes** (une par commune) :

```sql
-- Exemple : accès Latresne + Argelès
user_id                              | commune_slug | code_insee | role
-------------------------------------+--------------+------------+------
a1b2c3d4-....                        | latresne     | 33234      | user
a1b2c3d4-....                        | argeles      | 66008      | user
```

---

## Où sont stockés les droits ?

### 1. Table `public.user_commune_access` (recommandé)

Créée par `sql/001_create_user_commune_access.sql`.

Le backend lit cette table en priorité via `services/auth/commune_access.py`.

### 2. Fallback legacy : métadonnées Supabase Auth

Ancien mécanisme Latresne : champ `insee` dans `auth.users.raw_user_meta_data` :

```json
{ "insee": "33234" }
```

ou multi-communes :

```json
{ "insee": ["33234", "66008"] }
```

**Comportement :**
- Si des lignes existent dans `user_commune_access` → elles priment.
- Sinon → lecture du champ `insee` en metadata.
- Si aucune des deux → **accès à toutes les communes** (comportement historique admin / dev).

Le script `sql/004_migrate_metadata_insee_to_user_commune_access.sql` importe les metadata existantes vers la table.

---

## Registre des communes

| Slug portail | Code INSEE | Schéma géo |
|--------------|------------|------------|
| `latresne` | 33234 | `latresne` |
| `argeles` | 66008 | `argeles` |
| `mios` | 33531 | `mios` |

Défini dans `services/auth/commune_access.py` (`COMMUNE_REGISTRY`).

---

## Contrôles côté API

### Génération CUA

`POST /communes/{commune_slug}/cua/generate`

- Si `user_id` est fourni → vérifie que l'utilisateur est autorisé sur la commune (`assert_authorized_for_commune_slug`).
- Sinon → pas de contrôle (comportement identique au pipeline CERFA legacy).

### Historique carte

`GET /pipelines/by_user?user_id=…&commune_slug=…`

- Filtre par `user_id` (ses propres CUAs).
- Filtre par **droits INSEE** (ne renvoie pas les CUAs des communes interdites).
- Paramètre optionnel `commune_slug` : n'affiche que l'historique de la carte courante (ex. `argeles`).

### Détail projet

`GET /pipelines/by_slug?slug=…&user_id=…`

- Vérifie droit sur le `code_insee` du pipeline.
- Vérifie que le pipeline appartient à l'utilisateur (`user_id`).

---

## Accès aux pages front

| URL | Contenu | Restriction |
|-----|---------|-------------|
| `/latresne/cua` | Carte + outils CUA Latresne | Garde front + API |
| `/argeles/cua` | Carte + CUA Argelès v2 | Garde front + API |
| `/mios/...` | Portail Mios | Garde front + API |
| `/{commune}/cua/projects/{slug}` | Page projet | Garde via `CommuneLayout` |
| `/cua?t=…` | Viewer DOCX | Token encodé (lien partageable) |

### Garde de routes (depuis 2026-06)

Le composant `CommuneLayout` vérifie les droits via :

1. `GET /account/commune-access?user_id=…` (backend)
2. Fallback : `user_metadata.insee` dans la session Supabase

Si l'utilisateur tente `/latresne/*` sans droit Latresne → redirection automatique vers sa première commune autorisée (ex. `/argeles/cua`).

Après login, un utilisateur restreint est redirigé directement vers son portail commune (ex. Argelès).

Fichiers front : `src/auth/communeAccess.ts`, `CommuneAccessContext.tsx`, `layouts/CommuneLayout.tsx`.

Pour masquer les liens vers d'autres communes sur la **landing page** publique, adapter les composants site (non couvert par la garde portail).

---

## Accès aux documents

| Document | Stockage | Accès |
|----------|----------|-------|
| CUA DOCX | Supabase Storage `visualisation/{slug}/CUA_unite_fonciere.docx` | URL publique Storage + viewer `/cua?t=…` |
| Métadonnées pipeline | `public.pipelines` | API backend (service role) |
| Fichiers projet | `latresne.project_files` (transition) | API `/pipelines/...` |

---

## Ajouter un nouvel utilisateur

### Étape 1 — Créer le compte Supabase Auth

**Dashboard Supabase → Authentication → Users → Add user**

Ou invitation par e-mail.

Noter l'**UUID** du user (`user_id`).

### Étape 2 — Accorder les droits commune (recommandé)

Dans le SQL Editor :

```sql
-- Accès Latresne uniquement
INSERT INTO public.user_commune_access (user_id, commune_slug, code_insee, role)
VALUES (
  'UUID-DE-L-UTILISATEUR',
  'latresne',
  '33234',
  'user'
);

-- Accès Latresne + Argelès
INSERT INTO public.user_commune_access (user_id, commune_slug, code_insee, role)
VALUES
  ('UUID-DE-L-UTILISATEUR', 'latresne', '33234', 'user'),
  ('UUID-DE-L-UTILISATEUR', 'argeles',  '66008', 'user');
```

### Étape 3 — Super-admin (toutes communes)

```sql
INSERT INTO public.user_commune_access (user_id, commune_slug, code_insee, role)
VALUES (
  'UUID-DE-L-ADMIN',
  'latresne',
  '33234',
  'superadmin'
);
```

Une seule ligne `superadmin` suffit : le backend ignore les restrictions INSEE.

### Alternative legacy (sans table)

Dashboard Supabase → User → **User Metadata** :

```json
{
  "insee": ["33234", "66008"]
}
```

À migrer vers `user_commune_access` dès que possible.

---

## Modifier / révoquer des droits

```sql
-- Retirer Argelès
DELETE FROM public.user_commune_access
WHERE user_id = 'UUID-...' AND commune_slug = 'argeles';

-- Passer en admin commune
UPDATE public.user_commune_access
SET role = 'admin_commune', updated_at = now()
WHERE user_id = 'UUID-...' AND commune_slug = 'latresne';
```

---

## Migrations SQL — ordre d'exécution

Voir `sql/README.md` :

1. `001_create_user_commune_access.sql`
2. `002_create_public_pipelines.sql`
3. `003_migrate_latresne_pipelines_to_public.sql`
4. `004_migrate_metadata_insee_to_user_commune_access.sql`
5. `005_rls_policies.sql` (optionnel)
6. `006_grant_postgrest.sql`

Après migration, vérifier :

```bash
curl https://VOTRE-API/debug/supabase
```

Variable backend :

```bash
PIPELINES_SCHEMA=public   # défaut dans le code
```

---

## Fichiers code concernés

| Fichier | Rôle |
|---------|------|
| `services/auth/commune_access.py` | Logique droits INSEE / slug |
| `services/auth/pipelines_query.py` | Requêtes pipelines filtrées |
| `api/cuas/cua_router.py` | Auth génération CUA |
| `services/history/centroid_history.py` | Historique carte filtré |
| `app/routers/pipelines_supabase.py` | Détail pipeline + debug |
| `api/cuas/db.py` | Écriture `public.pipelines` |
| `api/.../auth_utils.py` | Compatibilité pipeline CERFA |

---

## FAQ

**Q : Un utilisateur sans ligne dans `user_commune_access` et sans `insee` en metadata voit quoi ?**  
R : Toutes les communes (comportement historique). En production, donner des droits explicites à chaque utilisateur.

**Q : Les anciens pipelines Latresne sont-ils visibles ?**  
R : Oui, après exécution de `003_migrate_latresne_pipelines_to_public.sql`.

**Q : Le pipeline CERFA Latresne écrit encore dans `latresne.pipelines` ?**  
R : Oui (orchestrateur legacy). Relancer la migration `003` ou mettre à jour l'orchestrateur dans un second temps.

**Q : Comment tester un refus d'accès ?**  
R : Créer un user avec uniquement `66008` (Argelès), tenter `POST /communes/latresne/cua/generate` → HTTP 403.
