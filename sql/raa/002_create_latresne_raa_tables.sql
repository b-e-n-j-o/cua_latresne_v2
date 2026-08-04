-- Création des tables RAA pour le schéma latresne (Gironde).
-- Reproduit la structure identique à argeles.raa / argeles.raa_analyse.
-- Idempotent (IF NOT EXISTS).

CREATE SCHEMA IF NOT EXISTS latresne;

CREATE TABLE IF NOT EXISTS latresne.raa (
    id              BIGSERIAL PRIMARY KEY,
    departement     TEXT NOT NULL DEFAULT '33',
    source          TEXT NOT NULL DEFAULT 'Gironde',
    page_url        TEXT,
    pdf_url         TEXT NOT NULL,
    titre           TEXT,
    date_publication DATE,
    taille_mo       DOUBLE PRECISION,
    statut          TEXT NOT NULL DEFAULT 'detecte',
    vu              BOOLEAN NOT NULL DEFAULT false,
    masque          BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT raa_pdf_url_unique UNIQUE (pdf_url)
);

CREATE INDEX IF NOT EXISTS latresne_raa_statut_idx
    ON latresne.raa (statut);
CREATE INDEX IF NOT EXISTS latresne_raa_date_pub_idx
    ON latresne.raa (date_publication DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS latresne_raa_masque_idx
    ON latresne.raa (masque) WHERE masque = false;

CREATE TABLE IF NOT EXISTS latresne.raa_analyse (
    id                    BIGSERIAL PRIMARY KEY,
    raa_id                BIGINT NOT NULL REFERENCES latresne.raa(id) ON DELETE CASCADE,
    modele                TEXT,
    niveau_alerte         TEXT,
    nb_arretes_total      INTEGER DEFAULT 0,
    nb_arretes_pertinents INTEGER DEFAULT 0,
    commune_mentionnee    BOOLEAN DEFAULT false,
    resume_global         TEXT,
    arretes               JSONB,
    tokens_in             INTEGER DEFAULT 0,
    tokens_out            INTEGER DEFAULT 0,
    cout_estime           DOUBLE PRECISION DEFAULT 0,
    erreur                TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS latresne_raa_analyse_raa_id_idx
    ON latresne.raa_analyse (raa_id);
