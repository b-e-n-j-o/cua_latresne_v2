-- argeles.servitudes — table unifiée CUA / agent PLU (lecture métier).
-- Source carto GPU : argeles.sup_assiette_s (conservée pour ingest / cartographie).
--
-- Colonnes source confirmées (GPU Argelès) :
--   gid, suptype, nomsuplitt, typeass, nom_servitude, gen_type, gen_tension, geometry
-- Pas de gml_id sur sup_assiette_s → NULL côté produit.

BEGIN;

DROP TABLE IF EXISTS argeles.servitudes;

CREATE TABLE argeles.servitudes AS
SELECT
    gid AS id,
    UPPER(TRIM(suptype)) AS suptype,
    'sup_assiette_s'::text AS source_table,
    nomsuplitt,
    typeass,
    NULL::text AS nature_protection,
    NULL::text AS precision_protection,
    NULL::text AS statut_proprietaire,
    nom_servitude AS nom_sup,
    gen_type AS type,
    NULL::text AS gml_id,
    NULL::text AS transporteur,
    NULL::text AS cat_fluide,
    NULL::text AS nom_captage,
    NULL::text AS perimetre_protection,
    NULL::text AS ins_pro__1,
    gen_tension AS tension,
    ST_MakeValid(
        CASE
            WHEN ST_SRID(geometry) = 2154 THEN geometry
            WHEN ST_SRID(geometry) = 0 OR ST_SRID(geometry) IS NULL
                THEN ST_SetSRID(geometry, 2154)
            ELSE ST_Transform(geometry, 2154)
        END
    ) AS geom_2154,
    ST_Transform(
        ST_MakeValid(
            CASE
                WHEN ST_SRID(geometry) = 2154 THEN geometry
                WHEN ST_SRID(geometry) = 0 OR ST_SRID(geometry) IS NULL
                    THEN ST_SetSRID(geometry, 2154)
                ELSE ST_Transform(geometry, 2154)
            END
        ),
        3857
    ) AS geom_3857
FROM argeles.sup_assiette_s
WHERE geometry IS NOT NULL
  AND suptype IS NOT NULL;

CREATE INDEX IF NOT EXISTS servitudes_geom_2154_gix
    ON argeles.servitudes USING GIST (geom_2154);
CREATE INDEX IF NOT EXISTS servitudes_geom_3857_gix
    ON argeles.servitudes USING GIST (geom_3857);
CREATE INDEX IF NOT EXISTS servitudes_suptype_idx
    ON argeles.servitudes (suptype);

COMMIT;
