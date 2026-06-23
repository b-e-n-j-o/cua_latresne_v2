-- Nature thématique par arrêté (stockée dans le JSONB raa_analyse.arretes[])
-- Valeurs : URBANISME | ENVIRONNEMENT | EVENEMENT | AUTRE
-- Relancer une analyse pour remplir le champ sur les recueils déjà traités.

COMMENT ON COLUMN argeles.raa_analyse.arretes IS
'Tableau JSON d''arrêtés. Chaque objet : titre, reference, pertinence, nature (URBANISME|ENVIRONNEMENT|EVENEMENT|AUTRE), raison, resume, pages.';

COMMENT ON COLUMN latresne.raa_analyse.arretes IS
'Tableau JSON d''arrêtés. Chaque objet : titre, reference, pertinence, nature (URBANISME|ENVIRONNEMENT|EVENEMENT|AUTRE), raison, resume, pages.';
