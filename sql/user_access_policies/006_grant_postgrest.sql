-- 006_grant_postgrest.sql
-- Droits lecture/écriture pour le rôle API Supabase (à adapter selon votre projet).
-- Exécuter après création des tables.

BEGIN;

GRANT SELECT, INSERT, UPDATE ON public.pipelines TO authenticated, service_role;
GRANT USAGE, SELECT ON SEQUENCE public.pipelines_id_seq TO authenticated, service_role;

GRANT SELECT ON public.user_commune_access TO authenticated, service_role;
GRANT INSERT, UPDATE, DELETE ON public.user_commune_access TO service_role;

COMMIT;
