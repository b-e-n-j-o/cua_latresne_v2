-- Photos cadastrales datées + fil de veille (Latresne)
-- Idempotent. Même structure que 001_argeles_photos_archives_veille.sql.

CREATE SCHEMA IF NOT EXISTS latresne;

ALTER TABLE latresne.parcelles ADD COLUMN IF NOT EXISTS created_etalab DATE;
ALTER TABLE latresne.parcelles ADD COLUMN IF NOT EXISTS updated_etalab DATE;
ALTER TABLE latresne.parcelles ADD COLUMN IF NOT EXISTS millesime_pci DATE;
ALTER TABLE latresne.parcelles ADD COLUMN IF NOT EXISTS imported_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS latresne.cadastre_photos (
    id                UUID PRIMARY KEY,
    archived_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    millesime_pci     DATE,
    millesime_suivant DATE,
    motif             TEXT NOT NULL,
    nb_parcelles      INTEGER NOT NULL,
    note              TEXT
);

CREATE TABLE IF NOT EXISTS latresne.parcelles_archives (
    archive_id      BIGSERIAL PRIMARY KEY,
    photo_id        UUID NOT NULL REFERENCES latresne.cadastre_photos (id) ON DELETE CASCADE,
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
    ON latresne.parcelles_archives (photo_id);
CREATE INDEX IF NOT EXISTS idx_parcelles_archives_idu
    ON latresne.parcelles_archives (idu);
CREATE UNIQUE INDEX IF NOT EXISTS idx_parcelles_archives_photo_idu
    ON latresne.parcelles_archives (photo_id, idu);
CREATE INDEX IF NOT EXISTS idx_parcelles_archives_geom_2154_gist
    ON latresne.parcelles_archives USING GIST (geom_2154);

CREATE TABLE IF NOT EXISTS latresne.cadastre_veille_runs (
    id             UUID PRIMARY KEY,
    photo_id       UUID REFERENCES latresne.cadastre_photos (id),
    run_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    millesime_pci  DATE,
    mode           TEXT,
    total_etalab   INTEGER,
    total_db       INTEGER,
    counts         JSONB,
    rapport        JSONB,
    apply_stats    JSONB
);

CREATE TABLE IF NOT EXISTS latresne.cadastre_veille_evenements (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES latresne.cadastre_veille_runs (id) ON DELETE CASCADE,
    type        TEXT NOT NULL,
    nb_parents  INTEGER,
    nb_enfants  INTEGER,
    parents     JSONB,
    enfants     JSONB,
    liens       JSONB
);

CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_run
    ON latresne.cadastre_veille_evenements (run_id);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_type
    ON latresne.cadastre_veille_evenements (type);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_parents_gin
    ON latresne.cadastre_veille_evenements USING GIN (parents);
CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_enfants_gin
    ON latresne.cadastre_veille_evenements USING GIN (enfants);

CREATE OR REPLACE VIEW latresne.cadastre_photos_periode AS
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
FROM latresne.cadastre_photos p;

CREATE OR REPLACE VIEW latresne.cadastre_filiation AS
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
FROM latresne.cadastre_veille_evenements e
JOIN latresne.cadastre_veille_runs r ON r.id = e.run_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.parents, '[]'::jsonb)) AS parent(elem)
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.enfants, '[]'::jsonb)) AS enfant(elem)
WHERE e.type IN ('division', 'fusion', 'recodage', 'remaniement');

CREATE OR REPLACE VIEW latresne.cadastre_filiation_liens AS
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
FROM latresne.cadastre_veille_evenements e
JOIN latresne.cadastre_veille_runs r ON r.id = e.run_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.liens, '[]'::jsonb)) AS lk(elem)
WHERE lk.elem ? 'idu_old' AND lk.elem ? 'idu_new';
