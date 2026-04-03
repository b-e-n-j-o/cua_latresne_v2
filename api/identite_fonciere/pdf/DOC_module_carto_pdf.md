# Consignes — ajouter un module « carte + légende + laius » au rapport PDF

Ce document décrit comment intégrer une **nouvelle section** dans le rapport d’identité foncière (`rapport_identite_fonciere.py`) lorsqu’on veut :

- une **carte** (fond tuilé, unité foncière, parcelles cadastrales optionnelles, entités métier dans un buffer) ;
- une **légende** (couleurs, libellés) ;
- un ou plusieurs blocs de **texte réglementaire / laius** (souvent en Markdown).

Les implémentations de référence dans le dépôt :

| Module | Fichier | Idée |
|--------|---------|------|
| PLU (zonage) | `plu_visuels.py` + `header.py` | Carte + légende % par zone ; laius par `zonage_reglement` |
| PPRI | `section_ppri.py` | Carte + légende % par `codezone` ; laius par zone (Markdown) |
| Servitudes (multi-couches) | `servitudes.py` | Une carte, plusieurs couches catalogue (`type: servitude`) ; tableau récap ; pas de laius dans la section |
| Préemption | `zone_de_preemption.py` | Une couche, légende simple ; `reglementation` distincte (1 ou N textes) |

---

## 1. Décisions à prendre avant de coder

### 1.1 Données

- **Table(s)** PostGIS et **schéma** (`IDENTITE_FONCIERE_DB_SCHEMA`, souvent `latresne`).
- **Colonne géométrie** : en pratique `geom_3857` (buffer / tuiles) ou `geom_2154` ; détection via `information_schema` comme dans `servitudes.py` / `zone_de_preemption.py`.
- **Condition d’affichage de la page** : ex. intersection stricte avec l’UF, ou % surface ≥ seuil, etc. Si la condition n’est pas remplie → **ne pas générer d’image** et **ne pas ajouter** la section au `story`.

### 1.2 Carte

- **Buffer** autour de l’UF (mètres), typiquement 300.
- **Fond** : `contextily` (Esri WorldImagery, secours OSM) — même schéma que les modules existants.
- **Entités à tracer** : intersection avec le buffer, géométries renvoyées en **EPSG:3857** pour l’axe matplotlib.

### 1.3 Légende

- **Une couche / une sémantique** : une couleur + un libellé (ex. préemption).
- **Plusieurs classes** : une couleur par classe (zonage, `nom_code`, ou **une couche = une couleur** comme les servitudes).
- **Pourcentages** : optionnel (PLU, PPRI) ; la légende peut combiner libellé + `%`.

### 1.4 Laius / réglementation

Choisir un **mode** (on peut en combiner plusieurs dans un même module si besoin) :

| Mode | Usage | Exemple dans le projet |
|------|--------|-------------------------|
| **Texte unique** | Même contenu pour toute la couche ou une seule valeur DISTINCT | Préemption : plusieurs `reglementation` DISTINCT affichées chacune une fois |
| **Un texte par clé métier** | Jointure logique sur un attribut (zone, codezone, etc.) | PPRI : laius par `codezone` |
| **Pas de laius dans la section** | Tableau ou renvoi vers le corps du rapport / annexe | Servitudes |
| **Markdown** | Rendu via `zonage_markdown_pdf.laius_reglement_to_flowables` | PPRI, préemption, PLU (zonage) |

**Attribut source** : le plus souvent `reglementation` ou `laius_reglement` ; tout nom de colonne valide SQL peut être paramétré (voir `PREEMPTION_REGLEMENTATION_COLUMN` dans `zone_de_preemption.py`).

---

## 2. Structure type d’un module

1. **Constantes de configuration** en tête de fichier (table, buffer, couleurs, titres PDF, colonne de texte, exclusions éventuelles).
2. **Accès BDD** : réutiliser le même principe que `plu_visuels._db_params()` / `section_ppri._db_params()` (variables d’environnement Supabase).
3. **Fonctions métier** :
   - détection colonne geom ;
   - comptage ou test d’intersection **UF** ;
   - chargement des géométries pour le **buffer** (carte) ;
   - récupération du ou des textes (requêtes `DISTINCT`, `DISTINCT ON`, ou dict par clé).
4. **Rendu matplotlib** : figure type « carte carrée + panneau légende » — constantes partagées `PLU_MAP_SQUARE_SIDE_IN`, `PLU_MAP_RIGHT_PANEL_RATIO` dans `plu_visuels.py`.
5. **Flowables ReportLab** : titre, filet, `Image`, puis blocs laius (tableaux encadrés comme dans `header.build_plu_zonage_page_flowables` ou `zone_de_preemption.build_preemption_section_flowables`).
6. **Point d’entrée** pour le rapport :
   - `generate_*_from_uf_geometry(geometry, out_dir, srid=..., insee=..., parcelles_cadastrales=...)` → `None` si pas de section, sinon tuple `(chemin_png, …)` avec les données nécessaires au PDF.
   - `build_*_flowables_for_report(...)` → liste de flowables.

Nommer les fichiers de façon explicite (`section_<thematique>.py`, `zone_de_<nom>.py`, etc.).

---

## 3. Intégration dans `rapport_identite_fonciere.py`

### 3.1 Génération des assets (bloc `if geometry`)

À côté des appels PLU / PPRI / servitudes / préemption :

- créer le répertoire de sortie (`out_base`) ;
- appeler `generate_*_from_uf_geometry` dans un `try/except` ;
- en cas de succès, stocker chemins et métadonnées dans **`result`** (traçabilité API / debug) : ex. `result["ma_carte_png"] = …`.

### 3.2 Enchaînement des pages (`story`)

- Après la page de garde (et éventuellement PLU, PPRI, servitudes), enchaîner avec `PageBreak()` + `story.extend(build_*_flowables…)` **uniquement si** la génération a produit une carte (ou les conditions métier sont remplies).
- Si une couche était auparavant rendue dans le **corps par article** (ex. article 9 préemption) : **exclure** cet article (ou cette table) de la boucle `_build_article_section` pour éviter le doublon, et placer la **nouvelle page** à l’endroit voulu (après les articles 3–8, avant l’annexe, etc.).

### 3.3 Catalogue `catalogue_identite_fonciere.json`

- Vérifier que la couche y est décrite (`nom`, `type`, `article`, `keep`, etc.) pour le reste du pipeline (intersections, carte HTML).
- Le module PDF peut **lire** ce catalogue pour les libellés (ex. `servitudes.servitude_catalog_entries`, `zone_de_preemption.catalogue_display_name`).

---

## 4. Paramètres « configurables » récapitulés

Pour un futur module générique (ou une variante), lister explicitement :

| Paramètre | Exemples |
|-----------|----------|
| `schema`, `table` | `latresne`, `preemption` |
| `geom_column` (priorités) | `geom_3857`, `geom_2154` |
| `buffer_m` | `300` |
| `uf_intersection_rule` | `COUNT(*) > 0`, ou filtre % |
| `legend_mode` | `single` \| `by_attribute` \| `by_layer` \| `with_percent` |
| `colors` | liste fixe, ou dict par clé |
| `pdf_kicker`, `pdf_title` | textes affichés |
| `laius_source_column` | `reglementation`, `laius_reglement`, … |
| `laius_layout` | `one_block` \| `distinct_values` \| `per_key` (dict) |
| `markdown` | oui / non (`laius_reglement_to_flowables`) |
| `catalogue_key` | clé JSON pour `nom` |
| `exclude_from_article` | ex. sauter `article == "9"` dans le corps |

---

## 5. Checklist avant merge

- [ ] Pas de section PDF si l’UF n’est pas concernée (comportement clair, pas de page vide).
- [ ] Identifiants SQL (table, colonnes dynamiques) validés ou issus de `information_schema` uniquement.
- [ ] PNG écrit sous un sous-dossier dédié (`*_visuels_assets`) avec un nom dérivé du hash de la géométrie (évite collisions).
- [ ] `matplotlib` en backend `Agg` ; pas d’affichage interactif.
- [ ] Texte PDF : `xml_escape` pour le hors-Markdown ; liens PDF avec échappement `&`, etc.
- [ ] Docstring du module + une phrase dans la doc de `generate_rapport_pdf` si le flux global change.

---

## 6. Piste d’évolution : module générique

Si le nombre de thématiques croît, on pourra extraire une **classe ou dataclass** `CartoPdfSectionConfig` + une fonction `render_standard_map_legend_png(...)` commune, et ne laisser dans chaque fichier que : requêtes SQL spécifiques, construction de la `GeoDataFrame`, règles de légende et collecte des laius. Les modules actuels restent la **référence concrète** jusqu’à cette extraction.

---

*Document à jour avec la structure du package `api/identite_fonciere/pdf/` (PLU, PPRI, servitudes, préemption). Pour le flux carte HTML / storage / URLs publiques, voir `NOTE_ARCHITECTURE_CARTE_ET_PDF.md` à la racine du package identité foncière.*
