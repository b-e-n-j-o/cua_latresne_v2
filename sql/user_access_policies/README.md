# Migrations SQL — droits utilisateurs & pipelines multi-communes

Exécuter les scripts **dans l'ordre** sur la base Supabase (SQL Editor ou `psql`).

| Fichier | Description |
|---------|-------------|
| `001_create_user_commune_access.sql` | Table des droits par commune |
| `002_create_public_pipelines.sql` | Table unifiée `public.pipelines` |
| `003_migrate_latresne_pipelines_to_public.sql` | Copie `latresne.pipelines` → `public` |
| `004_migrate_metadata_insee_to_user_commune_access.sql` | Import legacy `user_metadata.insee` |
| `005_rls_policies.sql` | Politiques RLS (optionnel) |
| `006_grant_postgrest.sql` | GRANT pour PostgREST |

## Avant migration (transition)

Tant que `public.pipelines` n'existe pas, conserver dans `.env` :

```bash
PIPELINES_SCHEMA=latresne
```

Après exécution des scripts 001–003, passer à :

```bash
PIPELINES_SCHEMA=public
```

## Après migration

1. Vérifier : `GET /debug/supabase` sur l'API.
2. Variable d'environnement backend (défaut déjà `public`) :
   ```bash
   PIPELINES_SCHEMA=public
   ```
3. Les anciens orchestrateurs Latresne écrivent encore dans `latresne.pipelines` — relancer `003` périodiquement ou migrer ces scripts ensuite.

## Ajouter un utilisateur

Voir `docs/ACCESS_UTILISATEURS_COMMUNES.md`.
