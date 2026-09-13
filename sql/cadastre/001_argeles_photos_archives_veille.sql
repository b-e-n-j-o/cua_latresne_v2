-- Photos cadastrales datées + fil de veille (Argelès-sur-Mer)
-- Idempotent. À exécuter dans l’éditeur SQL Supabase (rôle qui possède le schéma argeles).
--
-- But métier :
--   • garder une photo complète du cadastre à chaque vraie MAJ Etalab (~trimestriel)
--   • savoir « à quoi ressemblait cette parcelle à telle date »
--   • remonter parents / enfants (divisions, fusions, recodages)
--
-- parcelles            = carte actuelle (latest)
-- cadastre_photos      = l’album (une ligne par photo)
-- parcelles_archives   = les ~15 k polygones de chaque photo
-- cadastre_veille_*    = le récit des mouvements
--
-- Pour Latresne : sql/cadastre/002_latresne_photos_archives_veille.sql
-- Le script Python --ensure-schema pose le même DDL.

CREATE SCHEMA IF NOT EXISTS argeles;

-- ---------------------------------------------------------------------------
-- Datation sur le latest (deux horloges : Etalab vs millésime PCI / import)
-- ---------------------------------------------------------------------------
ALTER TABLE argeles.parcelles ADD COLUMN IF NOT EXISTS created_etalab DATE;
ALTER TABLE argeles.parcelles ADD COLUMN IF NOT EXISTS updated_etalab DATE;
ALTER TABLE argeles.parcelles ADD COLUMN IF NOT EXISTS millesime_pci DATE;
ALTER TABLE argeles.parcelles ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ;

-- ---------------------------------------------------------------------------
-- Album : une photo = tout le cadastre communal à un instant
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS argeles.cadastre_photos (
    id                UUID PRIMARY KEY,
    archived_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    millesime_pci     DATE,          -- millésime représenté (NULL = état initial non daté)
    millesime_suivant DATE,          -- millésime chargé juste après cette photo
    motif             TEXT NOT NULL, -- etat_initial | avant_maj
    nb_parcelles      INTEGER NOT NULL,
    note              TEXT
);

COMMENT ON TABLE argeles.cadastre_photos IS
    'Album des photos complètes du cadastre. Une ligne par vraie MAJ, pas une photo quotidienne.';
COMMENT ON COLUMN argeles.cadastre_photos.millesime_pci IS
    'Millésime PCI représenté par cette photo (NULL = copie initiale avant première datation).';
COMMENT ON COLUMN argeles.cadastre_photos.millesime_suivant IS
    'Millésime Etalab appliqué juste après la photo (borne haute de validité).';
COMMENT ON COLUMN argeles.cadastre_photos.motif IS
    'etat_initial = première photo ; avant_maj = snapshot pris avant d''écraser parcelles.';

-- ---------------------------------------------------------------------------
-- Archives : une ligne = une parcelle dans une photo
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS argeles.parcelles_archives (
    archive_id      BIGSERIAL PRIMARY KEY,
    photo_id        UUID NOT NULL REFERENCES argeles.cadastre_photos (id) ON DELETE CASCADE,
    idu             TEXT NOT NULL,
    numero          TEXT,
    section         TEXT,
    contenance      DOUBLE PRECISION,
    code_insee      TEXT,
    created_etalab  DATE,
    updated_etalab  DATE,
    millesime_pci   DATE,
    imported_at     TIMESTAMPTZ,
    geom_2154       geometry(MultiPolygon, 2154),
    geom_3857       geometry(MultiPolygon, 3857),
    sig_payload     JSONB
);

CREATE INDEX IF NOT EXISTS idx_parcelles_archives_photo
    ON argeles.parcelles_archives (photo_id);
CREATE INDEX IF NOT EXISTS idx_parcelles_archives_idu
    ON argeles.parcelles_archives (idu);
CREATE UNIQUE INDEX IF NOT EXISTS idx_parcelles_archives_photo_idu
    ON argeles.parcelles_archives (photo_id, idu);
CREATE INDEX IF NOT EXISTS idx_parcelles_archives_geom_2154_gist
    ON argeles.parcelles_archives USING GIST (geom_2154);

COMMENT ON TABLE argeles.parcelles_archives IS
    'Polygones + attributs d''une photo. Les parents divisés y gardent leur SIG après DELETE sur parcelles.';

-- ---------------------------------------------------------------------------
-- Fil de veille
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS argeles.cadastre_veille_runs (
    id             UUID PRIMARY KEY,
    photo_id       UUID REFERENCES argeles.cadastre_photos (id),
    run_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    millesime_pci  DATE,
    mode           TEXT,          -- apply | dry-run enregistré
    total_etalab   INTEGER,
    total_db       INTEGER,
    counts         JSONB,
    rapport        JSONB,
    apply_stats    JSONB
);

CREATE TABLE IF NOT EXISTS argeles.cadastre_veille_evenements (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES argeles.cadastre_veille_runs (id) ON DELETE CASCADE,
    type        TEXT NOT NULL,   -- division | fusion | recodage | remaniement | suppression | creation
    nb_parents  INTEGER,
    nb_enfants  INTEGER,
    parents     JSONB,
    enfants     JSONB,
    liens       JSONB            -- recouvrements géométriques idu_old → idu_new
);

CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_run
    ON argeles.cadastre_veille_evenements (run_id);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_type
    ON argeles.cadastre_veille_evenements (type);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_parents_gin
    ON argeles.cadastre_veille_evenements USING GIN (parents);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_enfants_gin
    ON argeles.cadastre_veille_evenements USING GIN (enfants);

-- ---------------------------------------------------------------------------
-- Période de validité de chaque photo
--   [debut, fin)  fin = millesime_suivant, sinon début de la photo suivante, sinon ouvert
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW argeles.cadastre_photos_periode AS
SELECT
    p.id,
    p.archived_at,
    p.millesime_pci,
    p.millesime_suivant,
    p.motif,
    p.nb_parcelles,
    p.note,
    COALESCE(p.millesime_pci, p.archived_at::date) AS debut,
    COALESCE(
        p.millesime_suivant,
        LEAD(COALESCE(p.millesime_pci, p.archived_at::date))
            OVER (ORDER BY p.archived_at, p.id)
    ) AS fin
FROM argeles.cadastre_photos p;

-- ---------------------------------------------------------------------------
-- Filiation : tous les couples parent → enfant d'un événement
-- (produit cartésien parents × enfants — utile pour remonter d'un IDU)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW argeles.cadastre_filiation AS
SELECT
    e.id AS evenement_id,
    e.run_id,
    r.run_at,
    r.millesime_pci,
    r.photo_id,
    e.type,
    parent.elem->>'idu' AS idu_parent,
    parent.elem->>'ref' AS ref_parent,
    enfant.elem->>'idu' AS idu_enfant,
    enfant.elem->>'ref' AS ref_enfant
FROM argeles.cadastre_veille_evenements e
JOIN argeles.cadastre_veille_runs r ON r.id = e.run_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.parents, '[]'::jsonb)) AS parent(elem)
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.enfants, '[]'::jsonb)) AS enfant(elem)
WHERE e.type IN ('division', 'fusion', 'recodage', 'remaniement');

-- Liens géométriques (plus précis que le produit cartésien)
CREATE OR REPLACE VIEW argeles.cadastre_filiation_liens AS
SELECT
    e.id AS evenement_id,
    e.run_id,
    r.run_at,
    r.millesime_pci,
    e.type,
    lk.elem->>'idu_old' AS idu_parent,
    lk.elem->>'idu_new' AS idu_enfant,
    (lk.elem->>'area_inter')::double precision AS area_inter_m2,
    (lk.elem->>'area_old')::double precision AS area_parent_m2,
    (lk.elem->>'area_new')::double precision AS area_enfant_m2,
    (lk.elem->>'pct_old')::double precision AS pct_parent,
    (lk.elem->>'pct_new')::double precision AS pct_enfant
FROM argeles.cadastre_veille_evenements e
JOIN argeles.cadastre_veille_runs r ON r.id = e.run_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.liens, '[]'::jsonb)) AS lk(elem)
WHERE lk.elem ? 'idu_old' AND lk.elem ? 'idu_new';

-- ---------------------------------------------------------------------------
-- Exemples (à lancer après le premier --apply)
-- ---------------------------------------------------------------------------
-- Photos disponibles
-- SELECT id, archived_at, millesime_pci, millesime_suivant, motif, nb_parcelles
-- FROM argeles.cadastre_photos_periode
-- ORDER BY archived_at;
--
-- Parcelle à une date (photo dont debut <= J < fin, sinon latest)
-- SELECT a.idu, a.section, a.numero, a.contenance, p.millesime_pci, p.motif
-- FROM argeles.cadastre_photos_periode p
-- JOIN argeles.parcelles_archives a ON a.photo_id = p.id
-- WHERE a.idu = '66008000BN0551'
--   AND p.debut <= DATE '2026-06-01'
--   AND (p.fin IS NULL OR DATE '2026-06-01' < p.fin);
--
-- Parents d'une parcelle
-- SELECT * FROM argeles.cadastre_filiation WHERE idu_enfant = '66008000BN0551';
--
-- Enfants d'une parcelle (après division)
-- SELECT * FROM argeles.cadastre_filiation WHERE idu_parent = '66008000AL0050';
