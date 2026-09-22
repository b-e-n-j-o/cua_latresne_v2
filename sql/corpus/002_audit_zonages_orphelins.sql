-- 002_audit_zonages_orphelins.sql
-- Lot 0.3 — diagnostic unique, lecture seule.
-- Correspondance exacte code carto ↔ corpus.textes.zone_code (sans alias).
--
-- Résultat de référence (14/09/2026, avant 004_alias_zonage) :
--   PLU 66008 : 3AU, 6AU (pas de texte) ; UA (carto UAa/UAb en règlement)
--   PLU 33234 : aucun orphelin
-- 3AU et 6AU : assumés, hors couverture_declaree (lot 1).

SELECT DISTINCT
  '66008'::text AS commune_insee,
  'PLU'::text AS document_type,
  z.zonage_reglement AS code_spatial,
  z.libelle
FROM argeles.zonage_plu z
WHERE NOT EXISTS (
  SELECT 1 FROM corpus.textes t
  WHERE t.document_type IN ('PLU', 'PLUI')
    AND (t.commune_insee = '66008' OR t.commune_insee IS NULL)
    AND upper(coalesce(t.zone_code, '')) = upper(z.zonage_reglement)
)
ORDER BY 3, 4;

SELECT DISTINCT
  '33234'::text AS commune_insee,
  'PLU'::text AS document_type,
  z.zonage_reglement AS code_spatial,
  z.libelle
FROM latresne.zonage_plu z
WHERE NOT EXISTS (
  SELECT 1 FROM corpus.textes t
  WHERE t.document_type IN ('PLU', 'PLUI')
    AND (t.commune_insee = '33234' OR t.commune_insee IS NULL)
    AND upper(coalesce(t.zone_code, '')) = upper(z.zonage_reglement)
)
ORDER BY 3, 4;
