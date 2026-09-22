-- 003_dispositions_generales.sql
-- Lot 0.4 — marquer les DG et les inclure dans textes_pour_zonages
-- sans les passer dans p_zonages.
-- Idempotent. Ne touche pas aux schémas par commune.

UPDATE corpus.textes
SET portee = 'globale',
    updated_at = now()
WHERE zone_code ~ '^DG[0-9]*$'
  AND coalesce(portee, '') IS DISTINCT FROM 'globale';

CREATE OR REPLACE FUNCTION corpus.textes_pour_zonages(
  p_insee text,
  p_zonages text[],
  p_date date DEFAULT CURRENT_DATE
)
RETURNS SETOF corpus.textes
LANGUAGE sql
STABLE
AS $function$
  SELECT DISTINCT ON (document_type, coalesce(zone_code, ''), coalesce(chapitre, ''))
         *
  FROM corpus.textes
  WHERE (commune_insee = p_insee OR commune_insee IS NULL)
    AND (
      portee = 'globale'
      OR zone_code IS NULL
      OR zone_code = ANY (p_zonages)
    )
    AND coalesce(date_debut_applicabilite, '-infinity'::date) <= p_date
    AND (date_fin_applicabilite IS NULL OR date_fin_applicabilite > p_date)
  ORDER BY document_type,
           coalesce(zone_code, ''),
           coalesce(chapitre, ''),
           commune_insee NULLS LAST,
           date_debut_applicabilite DESC NULLS LAST;
$function$;

COMMENT ON FUNCTION corpus.textes_pour_zonages(text, text[], date) IS
  'Textes applicables : dispositions générales (portee=globale ou zone_code NULL) '
  'plus les zonages demandés. Priorité à la ligne communale sur la nationale.';
