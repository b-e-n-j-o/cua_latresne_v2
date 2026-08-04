-- 008_commune_domains_latresne.sql
-- Autorise l'inscription self-service pour les agents Latresne.
-- Le trigger public.handle_new_user() mappe ensuite domain → user_commune_access
-- (commune_slug = latresne, code_insee = 33234, role = user).
--
-- Domaines issus des adresses mairie :
--   *@latresne.fr
--   *@mairie-latresne.fr
--
-- Exécuter dans le SQL Editor Supabase (projet auth partagé).

CREATE TABLE IF NOT EXISTS public.commune_domains (
  domain       text PRIMARY KEY,
  commune_slug text NOT NULL,
  code_insee   text NOT NULL,
  default_role text NOT NULL DEFAULT 'user'
);

INSERT INTO public.commune_domains (domain, commune_slug, code_insee, default_role)
VALUES
  ('ville-argelessurmer.fr', 'argeles', '66008', 'user'),
  ('latresne.fr',            'latresne', '33234', 'user'),
  ('mairie-latresne.fr',     'latresne', '33234', 'user')
ON CONFLICT (domain) DO UPDATE
SET
  commune_slug = EXCLUDED.commune_slug,
  code_insee   = EXCLUDED.code_insee,
  default_role = EXCLUDED.default_role;

-- Vérification :
-- SELECT * FROM public.commune_domains ORDER BY commune_slug, domain;
