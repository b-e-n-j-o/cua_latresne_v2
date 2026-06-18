-- Vérifier les CUAs d'un utilisateur test Argelès
-- Remplacer l'email si besoin.

SELECT
    u.id AS user_id,
    u.email,
    p.slug,
    p.commune_slug,
    p.commune,
    p.code_insee,
    p.user_id AS pipeline_user_id,
    p.centroid,
    p.cerfa_data,
    p.output_cua,
    p.created_at
FROM auth.users u
LEFT JOIN public.pipelines p ON p.user_id = u.id
WHERE u.email = 'test.argeles@kerelia.dev'
ORDER BY p.created_at DESC NULLS LAST;

-- Pipelines Argelès sans centroïde (invisibles sur la carte avant correctif front)
SELECT slug, user_id, commune_slug, centroid IS NULL AS sans_centroid, created_at
FROM public.pipelines
WHERE code_insee = '66008'
ORDER BY created_at DESC
LIMIT 20;

-- Pipelines orphelins (générés sans user_id — n'apparaissent dans aucun historique)
SELECT slug, commune_slug, code_insee, user_id, created_at
FROM public.pipelines
WHERE user_id IS NULL
  AND code_insee = '66008'
ORDER BY created_at DESC;
