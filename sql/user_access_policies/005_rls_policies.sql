-- 005_rls_policies.sql
-- Politiques RLS (optionnel mais recommandé si le front lit Supabase en session utilisateur).
-- Exécuter en dernier. Nécessite que PostgREST expose public.pipelines et user_commune_access.

BEGIN;

-- ---------------------------------------------------------------------------
-- user_commune_access
-- ---------------------------------------------------------------------------
ALTER TABLE public.user_commune_access ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS user_commune_access_select_own ON public.user_commune_access;
CREATE POLICY user_commune_access_select_own
    ON public.user_commune_access
    FOR SELECT
    TO authenticated
    USING (user_id = auth.uid());

-- Écriture réservée au service role / admin SQL (pas de policy INSERT pour authenticated).

-- ---------------------------------------------------------------------------
-- pipelines
-- ---------------------------------------------------------------------------
ALTER TABLE public.pipelines ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS pipelines_select_own_or_commune ON public.pipelines;
CREATE POLICY pipelines_select_own_or_commune
    ON public.pipelines
    FOR SELECT
    TO authenticated
    USING (
        user_id = auth.uid()
        OR EXISTS (
            SELECT 1
            FROM public.user_commune_access a
            WHERE a.user_id = auth.uid()
              AND (
                  a.role = 'superadmin'
                  OR a.code_insee = pipelines.code_insee
                  OR a.commune_slug = pipelines.commune_slug
              )
        )
    );

DROP POLICY IF EXISTS pipelines_insert_commune ON public.pipelines;
CREATE POLICY pipelines_insert_commune
    ON public.pipelines
    FOR INSERT
    TO authenticated
    WITH CHECK (
        user_id = auth.uid()
        AND (
            NOT EXISTS (SELECT 1 FROM public.user_commune_access a WHERE a.user_id = auth.uid())
            OR EXISTS (
                SELECT 1
                FROM public.user_commune_access a
                WHERE a.user_id = auth.uid()
                  AND (
                      a.role = 'superadmin'
                      OR a.code_insee = pipelines.code_insee
                      OR a.commune_slug = pipelines.commune_slug
                  )
            )
        )
    );

DROP POLICY IF EXISTS pipelines_update_own ON public.pipelines;
CREATE POLICY pipelines_update_own
    ON public.pipelines
    FOR UPDATE
    TO authenticated
    USING (user_id = auth.uid())
    WITH CHECK (user_id = auth.uid());

COMMIT;
