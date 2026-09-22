-- 002b_audit_couches_risques.sql
-- Suite du diagnostic 0.3 (couches hors PLU). Lecture seule.
-- À lancer après 004_alias_zonage.sql.

-- PPR Argelès : code_degre / label
SELECT DISTINCT
  '66008'::text AS commune_insee,
  'PPR'::text AS document_type,
  p.code_degre AS code_spatial,
  p.label,
  'code_degre'::text AS origine
FROM argeles.ppr p
WHERE p.code_degre IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM corpus.textes t
    WHERE t.document_type = 'PPR'
      AND (t.commune_insee = '66008' OR t.commune_insee IS NULL)
      AND upper(coalesce(t.zone_code, '')) = upper(btrim(p.code_degre))
  )
  AND NOT EXISTS (
    SELECT 1 FROM corpus.alias_zonage a
    JOIN corpus.textes t
      ON t.document_type = 'PPR'
     AND t.commune_insee = '66008'
     AND t.zone_code = a.code_texte
    WHERE a.commune_insee = '66008'
      AND a.document_type = 'PPR'
      AND upper(a.code_spatial) = upper(btrim(p.code_degre))
  );

-- PPRMVT Latresne
SELECT DISTINCT
  '33234'::text AS commune_insee,
  'PPRMVT'::text AS document_type,
  p.codezone AS code_spatial
FROM latresne.pprmvt p
WHERE p.codezone IS NOT NULL
  AND btrim(p.codezone) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM corpus.textes t
    WHERE t.document_type = 'PPRMVT'
      AND t.commune_insee = '33234'
      AND upper(coalesce(t.zone_code, '')) = upper(btrim(p.codezone))
  )
  AND NOT EXISTS (
    SELECT 1 FROM corpus.alias_zonage a
    JOIN corpus.textes t
      ON t.document_type = 'PPRMVT'
     AND t.commune_insee = '33234'
     AND t.zone_code = a.code_texte
    WHERE a.commune_insee = '33234'
      AND a.document_type = 'PPRMVT'
      AND upper(a.code_spatial) = upper(btrim(p.codezone))
  );

-- PPRI Latresne (PM1)
SELECT DISTINCT
  '33234'::text AS commune_insee,
  'PPRI'::text AS document_type,
  p.codezone AS code_spatial,
  p.nom
FROM latresne.pm1_detaillee_gironde p
WHERE p.codezone IS NOT NULL
  AND btrim(p.codezone) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM corpus.textes t
    WHERE t.document_type = 'PPRI'
      AND t.commune_insee = '33234'
      AND upper(coalesce(t.zone_code, '')) = upper(btrim(p.codezone))
  )
  AND NOT EXISTS (
    SELECT 1 FROM corpus.alias_zonage a
    JOIN corpus.textes t
      ON t.document_type = 'PPRI'
     AND t.commune_insee = '33234'
     AND t.zone_code = a.code_texte
    WHERE a.commune_insee = '33234'
      AND a.document_type = 'PPRI'
      AND upper(a.code_spatial) = upper(btrim(p.codezone))
  );
