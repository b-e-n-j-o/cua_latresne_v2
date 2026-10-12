-- =============================================================================
-- filiation_api.sql — couche d'interface backend pour la page Historique & filiation
--
-- Prérequis : filiation_schema.sql rejoué (wrappers millesimes, historique_uf,
--             emprise_uf, geom_versions inclus).
-- Idempotent : se rejoue en entier.
--
-- Principes :
--   * Le schéma filiation_api N'EST PAS exposé dans PostgREST. Seul le backend
--     l'appelle, via le rôle groupe filiation_api_exec.
--   * Toutes les fonctions prennent p_insee en premier argument et renvoient NULL
--     si la cible n'appartient pas à la commune → 404 côté routeur.
--     NULL = introuvable / hors commune ; '[]' = trouvé mais vide.
--   * Retour en jsonb : le backend relaie la chaîne telle quelle, sans re-parser.
--   * security definer + search_path figé : le rôle backend n'a besoin d'aucun
--     droit sur les tables de filiation, seulement de execute ici.
-- =============================================================================

begin;

create schema if not exists filiation_api;

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'filiation_api_exec') then
    create role filiation_api_exec nologin;
  end if;
end $$;

revoke all on schema filiation_api from public;
grant usage on schema filiation_api to filiation_api_exec;
alter default privileges in schema filiation_api revoke execute on functions from public;

-- -----------------------------------------------------------------------------
-- Helpers de cloisonnement
-- -----------------------------------------------------------------------------

-- Vrai si la version appartient à la commune.
create or replace function filiation_api._version_de_commune(p_insee text, p_version bigint)
returns boolean
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select exists (
    select 1 from filiation.parcelle_version
    where version_id = p_version and code_insee = p_insee)
$$;

-- Vrai si 1 à 50 idus distincts, tous connus dans la commune (à une date quelconque).
-- La validité à la date de référence est gérée ensuite par emprise_idus.
create or replace function filiation_api._idus_de_commune(p_insee text, p_idus text[])
returns boolean
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  with demandes as (select distinct u as idu from unnest(p_idus) u)
  select (select count(*) from demandes) between 1 and 50
     and (select count(distinct v.idu)
          from filiation.parcelle_version v
          join demandes d on d.idu = v.idu
          where v.code_insee = p_insee)
         = (select count(*) from demandes)
$$;

-- -----------------------------------------------------------------------------
-- Fonctions d'interface
-- -----------------------------------------------------------------------------

create or replace function filiation_api.millesimes(p_insee text)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select coalesce(jsonb_agg(to_jsonb(m) order by m.millesime), '[]'::jsonb)
  from filiation.millesimes(p_insee) m
$$;

-- Réécrite plutôt qu'appelée : tri déterministe (plus petite parcelle d'abord)
-- pour les zones où le plan source a des chevauchements.
create or replace function filiation_api.parcelle_au_point(
  p_insee text, p_lon double precision, p_lat double precision, p_date date default null)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select to_jsonb(r) from (
    select v.version_id, v.idu, v.valid_from, v.valid_to
    from filiation.parcelle_version v,
         (select st_transform(st_setsrid(st_point(p_lon, p_lat), 4326), 2154) as pt) p
    where v.code_insee = p_insee
      and v.periode @> coalesce(p_date, current_date)
      and v.geom && p.pt
      and st_intersects(v.geom, p.pt)
    order by st_area(v.geom)
    limit 1
  ) r
$$;

create or replace function filiation_api.version_actuelle(p_insee text, p_idu text)
returns bigint
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select version_id from filiation.parcelle_version
  where idu = p_idu and code_insee = p_insee and valid_to is null
$$;

create or replace function filiation_api.frise(p_insee text, p_version bigint)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when filiation_api._version_de_commune(p_insee, p_version) then
    (select coalesce(jsonb_agg(to_jsonb(a)), '[]'::jsonb)
     from filiation.ascendance_resumee(p_version) a)
  end
$$;

-- Le contrôle porte sur la racine seulement : les ancêtres d'une commune
-- nouvelle peuvent porter un autre code INSEE, on ne coupe pas le graphe.
create or replace function filiation_api.graphe(p_insee text, p_version bigint)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when filiation_api._version_de_commune(p_insee, p_version) then
    filiation.graphe_filiation(p_version)::jsonb
  end
$$;

create or replace function filiation_api.evenements(p_insee text, p_idu text)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when filiation_api._idus_de_commune(p_insee, array[p_idu]) then
    (select coalesce(jsonb_agg(to_jsonb(e)), '[]'::jsonb)
     from filiation.evenements_parcelle(p_idu) e)
  end
$$;

create or replace function filiation_api.historique_uf(
  p_insee text, p_idus text[], p_date date default null)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when filiation_api._idus_de_commune(p_insee, p_idus) then
    (select coalesce(
              jsonb_agg(to_jsonb(h) order by h.observe_depuis desc, h.part_emprise desc),
              '[]'::jsonb)
     from filiation.historique_uf(p_idus, p_date) h)
  end
$$;

create or replace function filiation_api.emprise_uf(
  p_insee text, p_idus text[], p_date date default null)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when filiation_api._idus_de_commune(p_insee, p_idus) then
    filiation.emprise_uf(p_idus, p_date)
  end
$$;

-- Pas de filtre commune (survol d'ancêtres éventuellement hors commune),
-- mais un plafond de 200 versions par appel.
create or replace function filiation_api.geom_versions(p_versions bigint[])
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select case when cardinality(p_versions) between 1 and 200 then
    (select coalesce(jsonb_agg(to_jsonb(g)), '[]'::jsonb)
     from filiation.geom_versions(p_versions) g)
  end
$$;

-- Tuile MVT du cadastre d'une commune à une date (actuel si p_date nul).
-- Vide sous le zoom 13. Source-layer : 'cadastre'.
create or replace function filiation_api.tuile_cadastre(
  p_insee text, z int, x int, y int, p_date date default null)
returns bytea
language sql stable parallel safe security definer
set search_path = filiation, public, extensions
as $$
  with b as (select st_tileenvelope(z, x, y) as env)
  select coalesce((
    select st_asmvt(t, 'cadastre', 4096, 'geom') from (
      select v.version_id, v.idu, v.section, v.numero, v.valid_from, v.valid_to,
             st_asmvtgeom(st_transform(v.geom, 3857), b.env, 4096, 64, true) as geom
      from filiation.parcelle_version v, b
      where z >= 13
        and v.code_insee = p_insee
        and v.periode @> coalesce(p_date, current_date)
        and v.geom && st_transform(b.env, 2154)
    ) t
  ), ''::bytea)
$$;

-- En-tête parcelle : idu, aire, contour GeoJSON (lot 2, page historique).
create or replace function filiation_api.version_info(p_insee text, p_version bigint)
returns jsonb
language sql stable security definer
set search_path = filiation, public, extensions
as $$
  select jsonb_build_object(
    'version_id', v.version_id,
    'idu',        v.idu,
    'section',    v.section,
    'numero',     v.numero,
    'valid_from', v.valid_from,
    'valid_to',   v.valid_to,
    'aire_m2',    round(st_area(v.geom)::numeric),
    'geojson',    st_asgeojson(st_transform(v.geom, 4326), 6)::jsonb)
  from filiation.parcelle_version v
  where v.version_id = p_version and v.code_insee = p_insee
$$;

-- -----------------------------------------------------------------------------
-- Droits
-- -----------------------------------------------------------------------------
revoke execute on all functions in schema filiation_api from public;
grant execute on all functions in schema filiation_api to filiation_api_exec;

-- À exécuter une fois, avec le rôle de connexion de votre backend :
-- grant filiation_api_exec to <role_backend>;

commit;

-- =============================================================================
-- Vérifications : critères d'acceptation au niveau SQL, avant tout front
-- =============================================================================
-- Recherche AM 2150 :
--   select filiation_api.version_actuelle('33234', '33234000AM2150');
--
-- Frise : 3 étapes (division AM 2149, réunion AM 2133 + AM 2134, division AM 0671).
--   select jsonb_pretty(filiation_api.frise('33234',
--          filiation_api.version_actuelle('33234', '33234000AM2150')));
--
-- Preuve spatiale : 4 périodes (AM 2150, AM 2149, AM 2134, AM 0671 censure_gauche).
--   select e->>'idu', e->>'observe_depuis', e->>'observe_jusqu_au',
--          e->>'part_emprise', e->>'censure_gauche'
--   from jsonb_array_elements(
--          filiation_api.historique_uf('33234', array['33234000AM2150'])) e;
--
-- Cloisonnement : doit renvoyer NULL.
--   select filiation_api.frise('66008',
--          filiation_api.version_actuelle('33234', '33234000AM2150'));
--   select filiation_api.historique_uf('66008', array['33234000AM2150']);
--
-- Tuiles, autour du bourg de Latresne (z16, tuile approximative) :
-- AM 2099 doit changer entre les deux dates.
--   select length(filiation_api.tuile_cadastre('33234', 16, 32678, 23631, '2022-07-01'));
--   select length(filiation_api.tuile_cadastre('33234', 16, 32678, 23631, '2022-10-01'));
--   select length(filiation_api.tuile_cadastre('33234', 12, 2042, 1476));  -- 0 attendu