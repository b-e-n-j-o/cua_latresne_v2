ALTER TABLE argeles.prescriptions_surf ADD COLUMN IF NOT EXISTS oap_id text;

BEGIN;

-- ============ NÈGUEBOUS ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'neguebous',
  nom = 'OAP Nèguebous',
  reglementation = $t$OAP sectorielle « Nèguebous » — zone 1AU — vocation habitat.
Source : PLU d'Argelès-sur-Mer, pièce 5 OAP (modification n°2), p. 17 et 23. Les OAP sont opposables aux tiers dans un rapport de compatibilité ; des adaptations mineures sont possibles lors de la traduction opérationnelle.

Surfaces : 14,13 ha (périmètre de la carte) ; 12,22 ha retenus pour la programmation (hors fossé et abords de la RD 114, hors ER 26 — élargissement pour la piste cyclable village–Taxo — et hors partie du chemin rural Cami Trencat).

Programmation : de l'ordre de 455 logements, densité du secteur 37,23 lgt/ha.
- 273 logements en accession à la propriété, dont 80 minimum en accession sociale (PSLA ou logement communal) ;
- 182 logements locatifs, dont 110 minimum en locatif social.
Typologie : 240 logements en îlot mixte (individuel groupé à petits collectifs, 40/50 lgt/ha) ; 215 en habitat individuel dense (25/35 lgt/ha).

Hauteurs : par compatibilité avec le SCoT Littoral Sud (secteur « SPUS »), il est recherché qu'une majorité des constructions atteigne au moins 12 m au faîtage.

Principes du schéma : secteurs à dominante d'habitat collectif (densité moyenne) et d'habitat individuel (densité forte) ; voie principale et voies secondaires ; carrefour à aménager ; réseau de promenades et modes doux ; espace public et partagé végétal et mixte ; espace de nature à créer ou préserver ; création d'un ouvrage hydraulique (noue) ; arbres à préserver ou créer et végétalisation à conforter.
Une étude de circulation (LEE Conseil) conclut à l'absence de dysfonctionnement de trafic induit sur la RD 114.$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_2535';

-- ============ PORT JARDIN ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'port_jardin',
  nom = 'OAP Port Jardin',
  reglementation = $t$OAP sectorielle « Port Jardin » — zones 2AU / 1AU / Nrl — vocation habitat.
Source : PLU d'Argelès-sur-Mer, pièce 5 OAP (modification n°2), p. 16 et 23. Opposable dans un rapport de compatibilité.

Statut : la partie centrale est sanctuarisée en zone naturelle protégée et espaces remarquables. L'urbanisation est concentrée sur deux secteurs (Nord et Sud) en continuité du port et de la ville, sous forme de petits collectifs et d'habitat individuel groupé. Ces secteurs sont bloqués en 2AU dans l'attente des études pré-opérationnelles de la ZAC en cours.

Surfaces : 10,82 ha (périmètre) ; 3,71 ha retenus pour la programmation (hors coulée verte centrale d'intérêt général et hors équipement sportif, conformément au SCoT).

Programmation : environ 250 logements, densité du secteur 67,38 lgt/ha.
- Secteur Nord, de l'ordre de 147 logements : 88 en accession dont 26 minimum en accession sociale (PSLA ou logement communal) ; 59 locatifs dont 29 minimum en locatif social. Typologie : 109 en îlot mixte (intermédiaire à collectifs, 60/70 lgt/ha), 38 en îlot mixte (40/50 lgt/ha).
- Secteur Sud, de l'ordre de 103 logements : 61 en accession dont 34 minimum en accession sociale ; 42 locatifs dont 21 minimum en locatif social. Typologie : 103 en îlot mixte (40/50 lgt/ha).

Surface de plancher : 15 000 m² maximum pour l'ensemble du secteur.
Hauteurs : par compatibilité avec le SCoT Littoral Sud (secteur « SPUS »), il est recherché qu'une majorité des constructions atteigne au moins 12 m au faîtage.

Principes du schéma : secteur à dominante d'habitat collectif (densité forte) ; secteur d'équipements ; espaces publics et partagés (végétal, minéral, mixte) ; espace de nature à créer ou préserver ; espace de stationnement non imperméable ; voie principale existante et voies secondaires ; voie de desserte locale ; réseau de promenades et modes doux ; création d'ouvrages hydrauliques (noue).$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_26e2';

-- ============ LES OLIVETTES ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'olivettes',
  nom = 'OAP Les Olivettes',
  reglementation = $t$OAP sectorielle n°6 « Les Olivettes » — zone 1AU — vocation habitat.
Source : PLU d'Argelès-sur-Mer, pièce 5 OAP (modification n°2), p. 20, 21 et 23. Opposable dans un rapport de compatibilité.

Surface : 2,67 ha.
Programmation : de l'ordre de 71 logements, densité du secteur 26,59 lgt/ha, dont 28 logements sociaux minimum (10 en accession sociale — PSLA ou logement communal — et 18 en locatif social).
Typologie : 54 logements en îlot mixte (individuel groupé à petits collectifs, 40/50 lgt/ha) ; 17 en habitat individuel (15-20 lgt/ha).
Hauteurs indicatives : R+1.

Contexte : cœur d'îlot entre la voie ferrée et une départementale, accès au Nord et au Sud, boisement significatif en entrée Nord, nombreux oliviers. Zone ouverte à court/moyen terme car raccordable aux réseaux, notamment à l'assainissement collectif.

Principes :
- Continuité urbaine : vocation résidentielle en lien avec le centre-ville ; densités échelonnées entre le centre-ville et les extensions pavillonnaires.
- Mixité : offre diversifiée (habitat intermédiaire, maisons mitoyennes, maisons individuelles sur petites parcelles) ; deux espaces verts communs de partage.
- Desserte : aménager et sécuriser les deux accès Nord et Sud ; desserte en « rue habitée », en évitant les voies rectilignes ; voirie partagée au gabarit adapté ; mutualisation des entrées ; liaisons douces internes et vers les quartiers voisins.
- Paysage : clôtures végétales (haies vives) imposées en limites parcellaires et en fond de jardin au contact des tissus existants et des éléments paysagers sensibles, ainsi que sur l'espace public ; bandes tampons végétalisées ; espaces de transition et coulées vertes accessibles PMR ; oliviers existants à préserver ; le long de la voie centrale, absence de clôtures fortement encouragée, avec une bande végétalisée entre voie et bâti.
- Eaux pluviales : conserver les axes d'écoulement existants ; limiter l'imperméabilisation et favoriser l'infiltration (au cas par cas selon les sols) ; ouvrages multi-usages ; matériaux poreux ou drainants ; techniques alternatives aériennes (noues, fossés, tranchées, jardins de pluie).
- Stationnement : aires de stationnement privé figurées au schéma.$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_26cd';

-- ============ ROUTE DE COLLIOURE ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'route_collioure',
  nom = 'OAP Route de Collioure',
  reglementation = $t$OAP sectorielle « Route de Collioure » — zone 1AU — vocation habitat.
Source : PLU d'Argelès-sur-Mer, pièce 5 OAP (modification n°2), p. 19 et 23. Opposable dans un rapport de compatibilité.

Surface : 0,98 ha (programmation). Le périmètre numérisé au GPU fait environ 0,87 ha.
Programmation : de l'ordre de 50 logements, densité du secteur 51,02 lgt/ha, dont 20 logements sociaux, parmi lesquels 7 minimum en accession sociale.
Typologie : 40 logements en îlot mixte (intermédiaire à collectifs, 60/70 lgt/ha) ; 10 en habitat individuel dense (25/35 lgt/ha).
Hauteurs indicatives : R+2 sur le secteur collectif, R+1 sur le secteur individuel. L'implantation du bâti figurée au schéma est donnée à titre indicatif.

Principes du schéma : secteur à dominante d'habitat collectif (densité moyenne) et secteur à dominante d'habitat individuel (densité moyenne) ; voie de desserte locale avec entrées/sorties à sens unique et aire de retournement ; aires de stationnement privé ; espaces publics et partagés (végétal, minéral) ; espace à végétaliser ; création d'un ouvrage hydraulique (noue) ; arbres à préserver ou créer ; réseau de promenades et modes doux.$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_26f3';

-- ============ CHEMIN DE VALBONNE ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'valbonne',
  nom = 'OAP Chemin de Valbonne',
  reglementation = $t$OAP sectorielle « Chemin de Valbonne » — zone 1AU — vocation habitat.
Source : PLU d'Argelès-sur-Mer, pièce 5 OAP (modification n°2), p. 18 et 23. Opposable dans un rapport de compatibilité.

Surface : 0,74 ha.
Programmation : entre 5 et 7 logements, en habitat individuel (15-20 lgt/ha), densité du secteur entre 13 et 16 lgt/ha.

Principes du schéma : secteur à dominante d'habitat individuel (densité moyenne) ; desserte par une voie avec entrée/sortie à sens unique ; espace public et partagé végétal (au nord et en bordure sud) et espace public minéral ; arbres à préserver ou créer (alignement le long de la desserte) ; réseau de promenades et modes doux le long du chemin de Valbonne.$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_26e0';

-- ============ ORPHELINE (UBa1 / UEa) ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'orpheline_charlemagne',
  nom = 'OAP non identifiée (secteur Chemin de Charlemagne)',
  reglementation = $t$Périmètre d'orientation d'aménagement et de programmation figurant au Géoportail de l'urbanisme (zones UBa1 / UEa, secteur Chemin de Charlemagne / rue Elisa Deroche), sans correspondance identifiée dans la pièce 5 OAP (modification n°2). Se reporter aux pièces OAP du PLU en vigueur ou se rapprocher du service urbanisme de la commune.$t$
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_26df';

-- ============ 6AU (à ignorer) ============
UPDATE argeles.prescriptions_surf SET
  oap_id = 'ignore_6au'
WHERE gml_id = 'prescription_surf.fid--6821dc26_19e16354ab7_2514';

-- contrôle avant commit
SELECT right(gml_id, 4), oap_id, nom, length(reglementation) AS nb_car
FROM argeles.prescriptions_surf
WHERE typepsc = '18'
ORDER BY oap_id;

COMMIT;