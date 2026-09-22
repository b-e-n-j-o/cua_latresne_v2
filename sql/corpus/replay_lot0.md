# Replay lot 0.6 — lectures corpus vs tables source
## Correspondance table source ↔ corpus
- `argeles.reglements_ppr` (PPR) : 4 lignes source, 0 écart(s) de présence/longueur.
- `argeles.reglements_pprif` (PPRIF) : 6 lignes source, 0 écart(s) de présence/longueur.
- `argeles.plu_reglement` (PLU) : 16 lignes source, 0 écart(s) de présence/longueur.
- `latresne.plu_reglement` (PLU) : 9 lignes source, 0 écart(s) de présence/longueur.
- `latresne.ppri_reglements` (PPRI) : 9 lignes source, 0 écart(s) de présence/longueur.
- `latresne.pprmvt_reglements` (PPRMVT) : 14 lignes source, 0 écart(s) de présence/longueur.

## Alias UA / I-b2
- UA → `['UAa', 'UAb']`
- I-b2 → `['I']`
- Textes UAa/UAb : UAa (20834 car.), UAb (19725 car.)

## 15 questions réelles (messages user les plus récents)
1. [argeles] tu sais consulter le code de l'urbanisme? un article a propos de l'emprise au sol? tu sais de quand il date/est à jour?
2. [latresne] combien doi-il y avoir en distance entre les deux batiments ? Y a-t-il une reglemenation ? Dans le plan masse fournis, il y aurait moins de 6 mètres à certains endroits entre les d
3. [latresne] Le projet se caractérise par deux batiments : 1 avec 32 logements et l'autre avec 11 logements sociaux LLS plus la MAM. Il y aurait besoin que de 76 places, ce qui prendrait appare
4. [latresne] Un projet de de 43 lots dont 1 commerces MAM de 100m² et des logements est proposé sur la parcelle AK0324. Quelles sont les points de vigilance et contraintes ?
5. [latresne] AK 324
6. [latresne] Un particulier souhaite faire un projet de logement sur les parcelles AC0052 et AC0055. Quelles sont les contraintes possibles sur ces parcelles
7. [latresne] sur ces parcelles est il possible de faire de faire un carport, une véranda ? une petite piscine? un abri de jardin ?
8. [latresne] parcelle AI 287 AI 299
9. [latresne] zone agricole
10. [latresne] quelle est la date d approbation du PPRMT de latresne
11. [latresne] sans faire de constriction mais transformer le batiment existant en terrain de paddle
12. [latresne] si je fais 4 terrains de paddle en interieur
13. [latresne] est il possible d'installer une piscine sur la parcelle AC 278
14. [latresne] les restaurant est toujours actif. si ils justifient une activé agricole sur l'exterieur une guinguette est elle possible si oui sous quelles conditions?
15. [latresne] peux tu me dire si sur les parcelles suivants ou il y a deja eu une activité de restauration, il est possible aujourd'hui d'y installer de nouveau un restaurant avec une guiguette

Les tables `argeles.*` / `latresne.*` restent la source d'ingestion. Les écarts de longueur ci-dessus sont ceux à expliquer avant de considérer la bascule lot 0.6 comme validée.
