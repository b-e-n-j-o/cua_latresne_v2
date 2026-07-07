-- Masquage utilisateur des recueils RAA hors périmètre communal.
-- La ligne reste en base (pdf_url conservé) → pas de réinsertion ni réanalyse à la sync.
-- Idempotent : n'altère que les schémas où la table raa existe déjà.

DO $$
BEGIN
    IF to_regclass('argeles.raa') IS NOT NULL THEN
        ALTER TABLE argeles.raa
            ADD COLUMN IF NOT EXISTS masque BOOLEAN NOT NULL DEFAULT false;
        CREATE INDEX IF NOT EXISTS raa_masque_idx ON argeles.raa (masque) WHERE masque = false;
    END IF;

    IF to_regclass('latresne.raa') IS NOT NULL THEN
        ALTER TABLE latresne.raa
            ADD COLUMN IF NOT EXISTS masque BOOLEAN NOT NULL DEFAULT false;
        CREATE INDEX IF NOT EXISTS raa_masque_idx_latresne ON latresne.raa (masque) WHERE masque = false;
    END IF;
END $$;
