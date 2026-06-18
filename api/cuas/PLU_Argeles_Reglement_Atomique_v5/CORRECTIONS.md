# Corrections appliquées à la v5 (2026-06-17)

Deux correctifs ont été appliqués au jeu atomique v5 après confrontation au règlement écrit du PLU (modification de droit commun n°2, version 30/10/2025). Ils portent sur deux fuites de portée que la régénération v5 n'avait pas traitées (phrasés non reconnus par le détecteur de portée).

1. **UCb — article 1.2 (mixité).** La servitude de mixité sociale « 80 % du programme en logements sociaux pour les opérations de plus de 1000 m² » est **propre au secteur UCa** (règlement : « Dans la zone UCa… »). Elle figurait par erreur dans la fiche UCb. L'article 1.2 d'UCb est remplacé par « Non règlementé. » (UCb n'a aucune servitude de mixité).

2. **Fiches N hors Nrl — stationnement.** La phrase « Dans le sous-zonage Nrl les espaces de stationnement sont interdits », propre à Nrl, était recopiée dans les 9 fiches N non-Nrl (N, Nb, Nc, Nd, Ne, Ng, Nj, Nm, NTcl). Elle y a été retirée ; elle est conservée dans la seule fiche Nrl.

## Recommandation générateur (pour éviter la réapparition)
Le détecteur de portée de v5 reconnaît « Dans le seul / les seuls / les seules secteur(s)/zone(s) X » mais ignore les portées exprimées autrement. Ajouter à la détection :
- « Dans la zone X » (cas UCa) ;
- « Dans le secteur X » ;
- « Dans le sous-zonage X » (cas Nrl).

Les valeurs normatives (emprise, hauteur, implantation, quotas de mixité) sont par ailleurs correctes dans l'ensemble du jeu v5, et la ventilation des usages (article 1.1) y est propre — points qui constituaient les améliorations majeures de la v5.
