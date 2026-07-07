-- Ajoute geom_3857 sur argeles.servitudes (requis par pmtiles_core.py).
-- À lancer si la table a été créée avant l'ajout de geom_3857 dans create_servitudes_from_sup_assiette_s.sql.

BEGIN;

ALTER TABLE argeles.servitudes
    ADD COLUMN IF NOT EXISTS geom_3857 geometry(Geometry, 3857);

UPDATE argeles.servitudes
SET geom_3857 = ST_Multi(ST_Force2D(ST_Transform(geom_2154, 3857)))
WHERE geom_2154 IS NOT NULL;

CREATE INDEX IF NOT EXISTS servitudes_geom_3857_gix
    ON argeles.servitudes USING GIST (geom_3857);

COMMIT;
