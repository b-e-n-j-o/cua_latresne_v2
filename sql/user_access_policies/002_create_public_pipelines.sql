-- 002_create_public_pipelines.sql
-- Table unifiée des pipelines CUA (toutes communes).
-- Exécuter après 001.

BEGIN;

CREATE TABLE IF NOT EXISTS public.pipelines (
    id                      bigserial PRIMARY KEY,
    slug                    text NOT NULL UNIQUE,
    commune_slug            text,
    commune                 text,
    code_insee              text,
    status                  text,
    bucket_path             text,
    output_cua              text,
    carte_2d_url            text,
    carte_3d_url            text,
    qr_url                  text,
    pipeline_result_url     text,
    user_id                 uuid,
    user_email              text,
    cerfa_data              jsonb,
    parcelles               jsonb,
    centroid                jsonb,
    intersections_gpkg_url  text,
    intersections_json_url  text,
    metadata                jsonb DEFAULT '{}'::jsonb,
    suivi                   integer,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE public.pipelines IS
    'Historique unifié des pipelines CUA (Latresne, Argelès, Mios, …).';

CREATE INDEX IF NOT EXISTS idx_pipelines_user_id_created
    ON public.pipelines (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipelines_commune_slug
    ON public.pipelines (commune_slug);

CREATE INDEX IF NOT EXISTS idx_pipelines_code_insee
    ON public.pipelines (code_insee);

CREATE OR REPLACE FUNCTION public.set_updated_at_pipelines()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_pipelines_updated_at ON public.pipelines;
CREATE TRIGGER trg_pipelines_updated_at
    BEFORE UPDATE ON public.pipelines
    FOR EACH ROW
    EXECUTE FUNCTION public.set_updated_at_pipelines();

COMMIT;
