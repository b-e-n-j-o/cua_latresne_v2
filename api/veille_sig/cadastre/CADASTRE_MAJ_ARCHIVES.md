# Mise à jour, archivage et veille cadastrale

Trace du dispositif mis en place le 11 septembre 2026.

Emplacement dans l’API (veille SIG) :

- Script : `api/veille_sig/cadastre/sync_or_add_parcelles.py`
- Ce dossier : prochaines mises à jour de couches (PLU, SUP, etc.) iront sous `api/veille_sig/`

Objectif pour la ville (ex. Argelès-sur-Mer) :

1. À l’écran : **toujours la dernière version Etalab** du cadastre.
2. Être **prévenue** des divisions / fusions / créations / suppressions.
3. Pouvoir **rouvrir le cadastre à une date passée** (audit, CUA, contentieux).

Source publique : GeoJSON Etalab  
`https://cadastre.data.gouv.fr/bundler/cadastre-etalab/communes/{insee}/geojson/parcelles`  
(millésime PCI DGFiP, ~tous les 3 mois). Ce n’est pas le WFS IGN, et ce n’est pas l’acte authentique DGFiP.

---

## En une phrase

**`parcelles` = la carte actuelle.**  
**Une photo complète à chaque vraie MAJ** (toutes les parcelles, pas seulement les 17 qui bougent).  
**Un fil de veille** = le récit des divisions/fusions.  
On ne prend **pas** une photo tous les jours : seulement quand Etalab a réellement changé.

---

## Architecture SQL (par commune / schéma)

Exemple Argelès : préfixe `argeles.`

### 1. `parcelles` — latest (carte à l’écran)

Inchangé pour le métier CUA (géométries + `sig_*`). Colonnes **ajoutées** :

| Colonne | Sens |
|---|---|
| `created_etalab` | Naissance de **cet IDU** côté DGFiP / Etalab (`created`) |
| `updated_etalab` | Dernière modif de **cette** parcelle (`updated`) |
| `millesime_pci` | Millésime du **fichier** chargé (ex. `2026-06-01`) |
| `imported_at` | Date/heure où **Kerelia** a écrit cette version |

Deux horloges distinctes :

- `created_etalab` / `updated_etalab` = histoire cadastrale de l’IDU.
- `millesime_pci` / `imported_at` = quelle copie on sert au client.

Une parcelle peut être `created` en 2010 et figurer dans le PCI de juin 2026.

Les colonnes `sig_*` (zonage, PPR, etc.) **ne sont pas écrasées** sur un IDU qui reste.  
Les **nouveaux** IDU n’ont pas de SIG tant que l’enrichissement n’a pas été relancé.

### 2. `cadastre_photos` — l’album (une ligne par photo)

| Colonne | Sens |
|---|---|
| `id` | UUID de la photo |
| `archived_at` | Quand on a pris la photo |
| `millesime_pci` | Millésime **représenté** par cette photo (NULL = état initial non daté) |
| `millesime_suivant` | Millésime qu’on s’apprête à charger après la photo |
| `motif` | `etat_initial` (première photo) ou `avant_maj` |
| `nb_parcelles` | Ex. 15 585 |
| `note` | Libellé humain |

Pour voir « le cadastre en juin 2026 » : la photo dont `millesime_pci = 2026-06-01`.

### 3. `parcelles_archives` — les 15 k polygones de chaque photo

Une ligne = une parcelle **dans une photo**.

Contient : IDU, section, numéro, contenance, géométries 2154/3857, dates Etalab si déjà connues, et `sig_payload` (JSON de tous les `sig_*` au moment de la photo).

Les 17 parents divisés gardent ainsi leur SIG dans l’archive, même après DELETE sur `parcelles`.

```sql
-- Cadastre d'une photo
SELECT * FROM argeles.parcelles_archives WHERE photo_id = '...';

-- Liste des photos
SELECT id, archived_at, millesime_pci, motif, nb_parcelles
FROM argeles.cadastre_photos
ORDER BY archived_at;
```

### 4. `cadastre_veille_runs` + `cadastre_veille_evenements`

Un run = un passage de comparaison (souvent lié à la photo prise juste avant `--apply`).

Événements classés :

| Type | Sens |
|---|---|
| `division` | 1 parent → n enfants (recouvrement géométrique) |
| `fusion` | n parents → 1 enfant |
| `recodage` | 1 → 1, nouvel IDU, même emprise |
| `remaniement` | n → m |
| `suppression` | disparu, aucun successeur dans la commune |
| `creation` | apparu, aucun prédécesseur dans la commune |

Seuils de filiation : intersection ≥ 0,5 m² et ≥ 12 % de la plus petite emprise (évite les slivers de limite).

---

## Boucle opérationnelle

```
Etalab (millésime PCI)
        │
        ▼
   DIFF lecture seule
   (IDU + contenance + géom + veille)
        │
        ├── rien n’a changé  → on ne touche à rien
        │
        └── delta métier
                │
                ├─ 1. PHOTO de parcelles (album)
                ├─ 2. MAJ de parcelles (latest)
                ├─ 3. DATATION (created/updated/millesime/imported_at)
                ├─ 4. ENREGISTREMENT de la veille
                └─ 5. ensuite : rejouer SIG + BAN sur les IDU touchés
```

`--apply` refuse de réécrire s’il n’y a pas de delta (nouveaux, supprimés, contenance, géométrie réelle, ou événement de veille).

Premier `--apply` Argelès : photo `etat_initial` (millésime inconnu, c’est la copie actuelle non datée) puis latest = PCI en cours.

---

## Commandes

Depuis `cua_latresne_v4` :

```bash
# Diff seulement (rien n’est écrit dans parcelles)
PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles \
  --insee 66008 --schema argeles --dry-run --no-slack

# Créer colonnes + tables d’archive / veille (sans changer les parcelles)
PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles \
  --insee 66008 --schema argeles --ensure-schema --no-slack

# Photo + mise à jour latest + veille (vrai changement de carte)
PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles \
  --insee 66008 --schema argeles --apply --no-slack

# Forcer le millésime PCI si la détection auto échoue
PYTHONPATH=. python -m api.veille_sig.cadastre.sync_or_add_parcelles \
  --insee 66008 --schema argeles --apply --millesime 2026-06-01
```

`--insert` est un **alias** de `--apply` (photo d’abord). L’ETL commune (`run_etl_commune.py`, mode `etalab`) appelle ce script via `--apply`.

Rapports disque (en plus de la base) :

- `api/veille_sig/cadastre/reports/diff_parcelles_{insee}_{schema}_parcelles_{horodatage}.json`
- `..._veille.txt`
- `...log`

---

## Différences géométriques : ce qui compte

La reprojection Etalab 4326 → 2154 décale les sommets d’environ **1 cm**. Ce n’est pas un vrai changement.

On ne compte une géométrie comme modifiée que si :

- écart de surface **> 10 m²**, ou
- distance de Hausdorff **> 0,5 m**.

---

## Constat Argelès (diff du 11/09/2026, sans `--apply`)

Comparaison `argeles.parcelles` vs Etalab INSEE **66008** (le JSON `etl_communes.json` indiquait à tort 66136 = Saint-Cyprien ; corrigé).

| | |
|---|---|
| Base | 15 585 parcelles, toutes avec SIG |
| Etalab | 15 614 |
| Parcelles communes inchangées | 15 568 (0 contenance, 0 géom réelle) |
| Divisions | **17** (tous les IDU « disparus » sont des parents) |
| Créations sans père dans la table | **2** : `BN:0551`, `BV:0576` |
| Fusions / suppressions nettes | 0 |

Les enfants de division ont surtout `updated` Etalab autour de **mai 2026**.

Rapport historique (premier run) : `services/ingestion_cadastre/reports/diff_parcelles_66008_argeles_parcelles_20260911_173847_veille.txt`  
Les runs suivants s’écrivent dans `api/veille_sig/cadastre/reports/`.

**`--apply` n’a pas encore été lancé** sur Argelès au moment de cette doc : la carte latest n’a pas été remplacée. Le DDL (`--ensure-schema`) a été exécuté le 11/09/2026 : colonnes de dates sur `argeles.parcelles` + tables `cadastre_photos`, `parcelles_archives`, `cadastre_veille_runs`, `cadastre_veille_evenements` (vides).

Millésime PCI courant Etalab : **2026-06-01** (à passer en `--millesime` si la détection auto échoue).

Après `--apply`, il faudra **recalculer les intersections SIG** (et BAN) sur les 46 nouveaux IDU. On ne copie pas le SIG du parent sur les enfants : les géométries ont changé.

---

## Écran ville

Sous-onglet **Veille cadastre** dans **Veille réglementaire** (`/:commune/raa?onglet=cadastre`).

| Besoin | Source |
|---|---|
| Carte à jour | `argeles.parcelles` |
| Fil divisions / fusions / créations | `cadastre_veille_evenements` |
| « À quoi ça ressemblait à telle date ? » | `cadastre_photos` + `parcelles_archives` |
| Parents / enfants d’un IDU | vues `cadastre_filiation` et `cadastre_filiation_liens` |
| Notif Slack / mail | même rapport de veille |

API lecture :

- `GET /{slug}/cadastre-veille`
- `GET /{slug}/cadastre-veille/evenements`
- `GET /{slug}/cadastre-veille/parcelle/{idu}`
- `GET /{slug}/cadastre-veille/parcelle/{idu}/a-date?date=YYYY-MM-DD`

Cron quotidien (cron-job.org → Render, pas l’écran ville) :

- `POST /api/tasks/trigger-veille` — header `X-Cron-Token: $INTERNAL_TOKEN`
- Répond 200 tout de suite ; `--apply` en arrière-plan pour chaque commune de `etl_communes.json`
- `GET /api/tasks/trigger-veille/status` — même header

SQL à jouer (idempotent) :

- `sql/cadastre/001_argeles_photos_archives_veille.sql`
- `sql/cadastre/002_latresne_photos_archives_veille.sql`

Mention à faire figurer : millésime PCI, date d’import Kerelia, et le fait que la donnée est le **PCI open data**, pas le plan cadastral opposable des impôts.

---

## Périmètre volontairement hors de ce script

- Recalcul des `sig_*` / adresses BAN (scripts d’enrichissement déjà existants).
- Historique quotidien : Etalab n’est pas un flux temps réel.

Même schéma à poser sur `latresne` et `mios` via `--schema` / `--insee`.
