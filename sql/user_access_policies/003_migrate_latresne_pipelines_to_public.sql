-- 003_migrate_latresne_pipelines_to_public.sql
-- Copie les données existantes latresne.pipelines → public.pipelines.
-- Exécuter après 002. Idempotent (ON CONFLICT sur slug).
-- Note : latresne.pipelines n'a pas de colonne updated_at → on réutilise created_at.

BEGIN;

INSERT INTO public.pipelines (
    slug,
    commune_slug,
    commune,
    code_insee,
    status,
    bucket_path,
    output_cua,
    carte_2d_url,
    carte_3d_url,
    qr_url,
    pipeline_result_url,
    user_id,
    user_email,
    cerfa_data,
    parcelles,
    centroid,
    intersections_gpkg_url,
    intersections_json_url,
    metadata,
    suivi,
    created_at,
    updated_at
)
SELECT
    p.slug,
    COALESCE(
        NULLIF(lower(trim(p.commune)), ''),
        CASE p.code_insee
            WHEN '33234' THEN 'latresne'
            WHEN '66008' THEN 'argeles'
            WHEN '33531' THEN 'mios'
            ELSE NULL
        END
    ) AS commune_slug,
    p.commune,
    p.code_insee,
    p.status,
    p.bucket_path,
    p.output_cua,
    p.carte_2d_url,
    p.carte_3d_url,
    p.qr_url,
    p.pipeline_result_url,
    p.user_id::uuid,
    p.user_email,
    p.cerfa_data,
    p.parcelles,
    p.centroid,
    p.intersections_gpkg_url,
    p.intersections_json_url,
    COALESCE(p.metadata, '{}'::jsonb),
    p.suivi,
    COALESCE(p.created_at, now()),
    COALESCE(p.created_at, now()) AS updated_at
FROM latresne.pipelines p
ON CONFLICT (slug) DO UPDATE SET
    commune_slug            = EXCLUDED.commune_slug,
    commune                 = EXCLUDED.commune,
    code_insee              = EXCLUDED.code_insee,
    status                  = EXCLUDED.status,
    bucket_path             = EXCLUDED.bucket_path,
    output_cua              = EXCLUDED.output_cua,
    carte_2d_url            = EXCLUDED.carte_2d_url,
    carte_3d_url            = EXCLUDED.carte_3d_url,
    qr_url                  = EXCLUDED.qr_url,
    pipeline_result_url     = EXCLUDED.pipeline_result_url,
    user_id                 = EXCLUDED.user_id,
    user_email              = EXCLUDED.user_email,
    cerfa_data              = EXCLUDED.cerfa_data,
    parcelles               = EXCLUDED.parcelles,
    centroid                = EXCLUDED.centroid,
    intersections_gpkg_url  = EXCLUDED.intersections_gpkg_url,
    intersections_json_url  = EXCLUDED.intersections_json_url,
    metadata                = EXCLUDED.metadata,
    suivi                   = EXCLUDED.suivi,
    updated_at              = now();

COMMIT;
