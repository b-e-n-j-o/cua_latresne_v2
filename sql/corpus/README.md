# Lot 0 — schéma `corpus` (chat PLU)

Migrations à exécuter **dans l'ordre**, une par onglet SQL Supabase
(une transaction par fichier).

| Fichier | Tâche | Effet |
|---|---|---|
| `002_audit_zonages_orphelins.sql` | 0.3 | Lecture seule — PLU carto sans texte |
| `002b_audit_couches_risques.sql` | 0.3 | Lecture seule — PPR / PPRI / PPRMVT (après `004`) |
| `003_dispositions_generales.sql` | 0.4 | `portee='globale'` sur DG + `textes_pour_zonages` |
| `004_alias_zonage.sql` | 0.5 | Table + fonction `resoudre_codes_zonage` |
| `005_metriques_tour.sql` | 0.2 | Colonne `metriques_tour` + backfill |

Le backend ajoute aussi `metriques_tour` tout seul (`ADD COLUMN IF NOT EXISTS`)
si la migration 005 n'a pas encore tourné.

Les lectures runtime (`get_reglement_*`, préchargement de session) passent par
`api/agents/plu_agent/corpus.py`, **filtrées par `document_type`**. On n'appelle
pas `textes_pour_zonages()` dans ces chemins (cette fonction agrège tous les
documents + toutes les DG).
