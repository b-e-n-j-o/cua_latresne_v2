-- 001_create_user_commune_access.sql
-- Droits utilisateur par commune (source de vérité applicative).
-- Exécuter en premier.

BEGIN;

CREATE TABLE IF NOT EXISTS public.user_commune_access (
    user_id       uuid NOT NULL REFERENCES auth.users (id) ON DELETE CASCADE,
    commune_slug  text NOT NULL,
    code_insee    text NOT NULL,
    role          text NOT NULL DEFAULT 'user'
        CHECK (role IN ('user', 'admin_commune', 'superadmin')),
  created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, commune_slug)
);

COMMENT ON TABLE public.user_commune_access IS
    'Droits portail Kerelia : quelles communes un utilisateur peut consulter / générer des CUAs.';

COMMENT ON COLUMN public.user_commune_access.role IS
    'user = accès standard ; admin_commune = gestion locale ; superadmin = toutes communes.';

CREATE INDEX IF NOT EXISTS idx_user_commune_access_user_id
    ON public.user_commune_access (user_id);

CREATE INDEX IF NOT EXISTS idx_user_commune_access_code_insee
    ON public.user_commune_access (code_insee);

CREATE OR REPLACE FUNCTION public.set_updated_at_user_commune_access()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_user_commune_access_updated_at ON public.user_commune_access;
CREATE TRIGGER trg_user_commune_access_updated_at
    BEFORE UPDATE ON public.user_commune_access
    FOR EACH ROW
    EXECUTE FUNCTION public.set_updated_at_user_commune_access();

COMMIT;
