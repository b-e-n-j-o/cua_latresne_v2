-- Isolation des sessions PLU par utilisateur Supabase Auth.
-- À exécuter une fois par schéma dans l'éditeur SQL Supabase (évite les timeouts en prod).

-- latresne
ALTER TABLE latresne.plu_sessions ADD COLUMN IF NOT EXISTS user_id UUID;
CREATE INDEX IF NOT EXISTS plu_sessions_user_id_updated_at_idx
  ON latresne.plu_sessions (user_id, updated_at DESC);

-- argeles
ALTER TABLE argeles.plu_sessions ADD COLUMN IF NOT EXISTS user_id UUID;
CREATE INDEX IF NOT EXISTS plu_sessions_user_id_updated_at_idx
  ON argeles.plu_sessions (user_id, updated_at DESC);

-- mios
ALTER TABLE mios.plu_sessions ADD COLUMN IF NOT EXISTS user_id UUID;
CREATE INDEX IF NOT EXISTS plu_sessions_user_id_updated_at_idx
  ON mios.plu_sessions (user_id, updated_at DESC);

-- france (si le schéma existe)
-- ALTER TABLE france.plu_sessions ADD COLUMN IF NOT EXISTS user_id UUID;
-- CREATE INDEX IF NOT EXISTS plu_sessions_user_id_updated_at_idx
--   ON france.plu_sessions (user_id, updated_at DESC);
