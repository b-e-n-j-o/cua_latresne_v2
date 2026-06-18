-- 004_migrate_metadata_insee_to_user_commune_access.sql
-- Importe les droits legacy (auth.users.raw_user_meta_data.insee) vers user_commune_access.
-- Exécuter après 001. Compléter manuellement les utilisateurs sans metadata.

BEGIN;

INSERT INTO public.user_commune_access (user_id, commune_slug, code_insee, role)
SELECT DISTINCT
    u.id AS user_id,
    CASE trim(both '"' from elem::text)
        WHEN '33234' THEN 'latresne'
        WHEN '66008' THEN 'argeles'
        WHEN '33531' THEN 'mios'
        ELSE lower(trim(both '"' from elem::text))
    END AS commune_slug,
    trim(both '"' from elem::text) AS code_insee,
    'user' AS role
FROM auth.users u
CROSS JOIN LATERAL (
    SELECT jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(u.raw_user_meta_data -> 'insee') = 'array'
                THEN u.raw_user_meta_data -> 'insee'
            WHEN u.raw_user_meta_data ? 'insee'
                THEN jsonb_build_array(u.raw_user_meta_data -> 'insee')
            ELSE '[]'::jsonb
        END
    ) AS elem
) expanded
WHERE u.raw_user_meta_data ? 'insee'
  AND trim(both '"' from elem::text) <> ''
ON CONFLICT (user_id, commune_slug) DO NOTHING;

COMMIT;
