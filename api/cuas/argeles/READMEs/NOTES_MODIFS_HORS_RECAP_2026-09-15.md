# Modifications hors recap — 15/09/2026

Doc de rollback. Le module `recap.py` et `section_recap` dans le builder sont **hors sujet ici** : ce fichier décrit uniquement les autres changements faits en même temps, pour pouvoir revenir à l’ancien comportement (seuils, agrégations, textes) si un CUA diverge de ce qui avait été calibré.

Ces scripts avaient été pensés et testés (audit `tests/audit_seuils_intersections.py`). Les mods ci-dessous ne touchent **pas** le seuil SQL `min_pct_sig` du catalogue. Elles changent surtout ce que Python fait **après** le SQL, sur des polygones déjà retenus.

---

## Ce qui n’a pas changé (le cœur calibré)

| Élément | Statut |
|---|---|
| Catalogue `min_pct_sig: 1.0` | **inchangé** |
| Filtre SQL par objet / surface **UF** : un polygone ≤ 1 % de l’UF est toujours exclu | **inchangé** |
| Seuil géométrique 0,01 m² | **inchangé** |
| Une zone à 100 % d’une petite parcelle mais 0,8 % de l’UF | **toujours exclue en SQL** (comportement voulu à l’audit) |
| `prescriptions_plu.py` (filtre encore par objet) | **non touché** |
| Modules prairies, taxes, ENEDIS, servitudes (SQL) | **non touchés** (sauf passage du `min_pct` catalogue au PPR / zonage métier) |

Si un CUA « bizarre » apparaît, ce n’est **pas** parce que le 1 % SQL a bougé. C’est parce que Python **agrège maintenant les fragments** d’un même libellé avant d’appliquer le 1 %.

---

## Risque réel : somme vs max

### Avant (calibré)

Pour un même libellé (zone UB, sous-zone PPR R2, secteur de hauteur…) :

1. SQL : chaque polygone doit faire **> 1 % de l’UF** tout seul.
2. Python : parmi les polygones restants du même libellé, on prenait le **max** (ou on refiltrait encore par objet).

Conséquences :

- UB découpée en 60 % + 10 % → le CUA affichait **60 %** (sous-estimation).
- UB découpée en 0,6 % + 0,6 % → **les deux polygones déjà morts en SQL**, donc toujours absents. Inchangé.
- UB découpée en 0,8 % + 0,8 % → morts en SQL. Inchangé.
- UB découpée en **1,1 % + 1,1 %** → avant : affichage **1,1 %** (max). Maintenant : **2,2 %** (somme).

Cas où le **contenu** du CUA peut changer (pas seulement le chiffre) :

- Plusieurs fragments **chacun > 1 %** : le libellé était déjà affiché, seul le % change (max → somme).
- Fragments **chacun ≤ 1 %** : toujours exclus en SQL. Pas d’effet.
- **PPR / zonage / hauteurs côté Python seulement** : si le SQL a laissé passer plusieurs fragments > 1 %, ils sont maintenant sommés puis re-testés au seuil. Un libellé dont **aucun** fragment ne dépassait 1 % en Python (mais dont la somme dépasse 1 %) peut **apparaître alors qu’il était masqué**. C’est le seul vrai élargissement.

Exemple PPR : deux polygones R2 à 0,7 % et 0,6 %.

- SQL (inchangé) : les deux sont exclus (≤ 1 %) → rien ne change.
- Si le SQL était à 0 (audit brut) : avant Python droppait les deux ; maintenant 1,3 % → la zone **apparaîtrait**. En prod le SQL est à 1 %, donc ce cas ne se produit pas.

**En production catalogue 1 %, l’effet attendu est surtout des % plus justes (60+10=70), pas de nouvelles zones issues de micro-fragments.** Le risque de « nouvelles couches » est faible tant que `min_pct_sig` reste à 1.0 en SQL.

Si le catalogue passe un jour à 0,5 % ou 0, le Python sommé **élargirait** davantage qu’avant. C’est volontaire pour ne plus sous-compter, mais ce n’est plus le comportement testé à l’audit.

---

## Fichiers touchés (hors recap.py)

| Fichier | Nature | Revert isolé ? |
|---|---|---|
| `intersection_modules/zonage_plu.py` | sémantique (somme + formats FR) | **oui**, restaurer le fichier entier |
| `intersection_modules/ppr_et_pprif.py` | sémantique (blocs PPR/PPRIF sommés + formats FR) | **oui**, restaurer le fichier entier |
| `intersections.py` | SQL CollectionExtract + garde-fous status KO + `min_pct` catalogue passé aux modules | **oui**, mais mélange plusieurs sujets |
| `generate_cua.py` | ne plus inventer `date_depot` | **oui** |
| `builder.py` | mélange recap **et** correctifs | **non** : ne pas restaurer le fichier entier si on veut garder l’encart |

---

## 1. `intersection_modules/zonage_plu.py`

### Avant

- `_items_avec_pct` : filtre `pct > seuil` **par objet**, puis `max` par libellé.
- `_zones_plu_avec_pct` (intro UF) utilisait ce max.
- `_zones_agregees` (somme) existait déjà, mais seulement pour le **détail par parcelle**.
- `_objets_significatifs` : `pct_objet > seuil` (un petit fragment d’une grande zone disparaissait du rendu réglementaire).
- Textes : `75.00 %` (format anglo).

### Maintenant

- Intro UF = `_zones_agregees` (somme plafonnée à 100 %, puis seuil sur le total).
- Objets affichés = ceux dont la **zone agrégée** dépasse le seuil.
- Suffixe de titre multi-zones = % agrégé, format FR (`75 %`, `9,5 %`).
- `intersections.py` passe `min_zonage_pct=resolve_min_pct_sig(catalogue["zonage_plu"])` au lieu du `1.0` figé du module. **Aujourd’hui le catalogue vaut 1.0, donc identique.**

### Impact CUA

- Intro du type « zone UB (60.00 % de la surface) » alors que l’UF est à 70 % UB → devient 70 %.
- Pas de nouvelle zone si SQL 1 % (fragments < 1 % jamais arrivés jusqu’ici).

### Revert

Remettre l’ancienne version du fichier (historique Cursor / Time Machine / copie). Points de restauration :

- réintroduire `_items_avec_pct` (max) ;
- `_format_intro` doit rappeler `_zones_plu_avec_pct` / `_items_avec_pct` ;
- `_objets_significatifs` = `[obj for obj in objets if _pct_sig(obj) > min_zonage_pct]`.

Dans `intersections.py`, retirer l’argument `min_zonage_pct=resolve_min_pct_sig(...)` de l’appel `compute_zonage_plu_reglementation` (le défaut du module `MIN_ZONAGE_PCT = 1.0` reprend le relais).

---

## 2. `intersection_modules/ppr_et_pprif.py`

C’est le fichier le plus sensible juridiquement (PPR).

### Avant

- `_ppr_objets_significatifs` : drop de chaque objet `pct <= 1 %`.
- Puis groupement par `label` ; `pct_sig` du bloc = celui du **plus gros** fragment (`_pick_best_entity`).
- PPRIF : `pct_sig` du meilleur fragment, pas de somme.
- Textes détail parcelle : `12.34 %`.

### Maintenant

- Groupement d’abord, **somme** des `%` (plafond 100 %), **puis** drop si total ≤ seuil.
- `pct_sig` du bloc = somme.
- Formats FR sur les libellés parcelle.
- Seuil passé depuis le catalogue (`min_detail_pct=resolve_min_pct_sig(ppr)`), aujourd’hui 1.0.

### Impact CUA

- Même logique que le zonage : % plus justes si une sous-zone est en plusieurs polygones > 1 %.
- Une sous-zone dont **tous** les fragments sont ≤ 1 % reste absente (SQL).
- Si un jour le SQL baisse le seuil, Python peut faire remonter une sous-zone fragmentée que l’ancien filtre par objet aurait encore masquée.

### Revert

Restaurer le fichier. Comportement cible :

```python
def _ppr_objets_significatifs(objets, min_pct=MIN_PPR_PCT):
    return [obj for obj in objets if _pct_sig(obj) > min_pct]

def _build_ppr_blocs(objets, min_pct=MIN_PPR_PCT):
    objets = _ppr_objets_significatifs(objets, min_pct)
    # groupement, _merge_ppr_fields, pct = celui du meilleur fragment
```

Et dans `intersections.py`, retirer `min_detail_pct=resolve_min_pct_sig(...)`.

---

## 3. `intersections.py` (hors recap)

Plusieurs mods indépendantes. On peut en revertir une sans les autres.

### 3.a SQL surfacique — `ST_CollectionExtract(..., 3)`

**Avant :** `ST_Intersection(ST_MakeValid(t.geom), uf.geom)`

**Maintenant :** `ST_CollectionExtract(ST_MakeValid(ST_Intersection(...)), 3)` pour ne garder que le polygonal (évite une GeometryCollection si `MakeValid` casse un polygone).

- **Risque :** une intersection qui n’a plus de partie surfacique (juste une ligne de contact) sort à aire 0 et est filtrée. C’était en principe déjà le cas via `ST_Area > 0.01`.
- **Revert :** remettre la ligne `ST_Intersection(ST_MakeValid(t.{geom_col}), uf.geom) AS inter_geom`.
- Couches linéaires / ponctuelles : **non touchées**. Prairies / taxes ont leur propre SQL : **non touché**.

### 3.b `v.get("objets")` dans `main()` + `"objets": []` avant le merge taxes

Évite un `KeyError` si un module métier ne renvoie pas `objets`. Cosmétique / robustesse CLI. Sans effet sur le CUA API.

### 3.c Ne plus écraser un statut `erreur` / `table_absente`

**Avant :** si `zonage_plu` (ou `alea_feu`) était en `table_absente`, le module métier tournait quand même sur une liste vide et réécrivait `status: non_concernee`. Le CUA lisait « zonage non déterminé » / rien, comme un vrai hors-zone.

**Maintenant :** si la couche brute est KO, on **ne lance pas** le module, on garde `erreur` / `table_absente`. Le builder / recap affichent « donnée indisponible ».

- **Pas un changement de seuil.** C’est un changement de vérité : on arrête de déguiser une panne en absence.
- **Revert :** remettre le `try/except` inconditionnel autour de `compute_zonage_plu_reglementation` / `compute_alea_feu_reglementation`.

### 3.d `min_pct_sig` recopié dans les métadonnées de couche

`_layer_catalogue_meta` ajoute `"min_pct_sig": resolve_min_pct_sig(cfg)` pour que le builder lise le seuil catalogue au lieu du `1.0` en dur. **Aujourd’hui égal à 1.0.**

### 3.e Commentaire

Le commentaire « UF ou parcelle » a été corrigé : le SQL ne regarde que l’UF. Doc uniquement.

---

## 4. `generate_cua.py`

**Avant :** si le dossier n’avait pas de `date_depot`, on écrivait **la date du jour**. Cette date part dans le CUA et sert de référence à la cristallisation 18 mois.

**Maintenant :** on ne remplit plus. Le builder affiche `—`.

- **Revert :** dans `_merge_dossier` :

```python
if not str(merged.get("date_depot") or "").strip():
    merged["date_depot"] = datetime.now().strftime("%d/%m/%Y")
```

Aucun lien avec les seuils.

---

## 5. `builder.py` — correctifs hors encart recap

Le fichier mélange recap et le reste. **Ne pas restaurer builder.py en entier** si on veut garder la synthèse. Ci-dessous uniquement le hors-recap.

### 5.a Hauteurs — même logique somme que le zonage

`_items_avec_pct` : max + filtre par objet → somme puis seuil.

`_objets_par_libelles_significatifs` : on garde les objets dont le **secteur agrégé** dépasse le seuil.

`_seuil_pct(layer)` lit `layer["min_pct_sig"]` (catalogue), sinon `MIN_ZONAGE_PCT = 1.0`.

**Revert ciblé :** remettre l’ancienne `_items_avec_pct` (max + `if pct <= MIN_ZONAGE_PCT: continue`) et `_objets_significatifs` (filtre par objet). L’intro hauteurs redevient sous-estimée sur secteurs fragmentés.

### 5.b DPU / SUP / PPR / zonage en erreur

On n’affirme plus « n’est pas soumise au DPU » / on n’omet plus la section risques si la couche est KO. Texte : « Donnée indisponible — à vérifier manuellement » + `logger.error`.

**La génération n’est pas bloquée** (choix volontaire : l’agent a quand même un CUA).

Sans lien avec les seuils.

### 5.c Servitudes sans `suptype`

**Avant :** `_dedupe_servitudes` jetait toute SUP sans code type.

**Maintenant :** dédoublonnage par `suptype` **ou** par libellé.

Peut faire **réapparaître** une servitude mal typée qui était silencieuse. Pas un sujet de seuil.

### 5.d Bâti cadastré

`batiments` est passé dans `LAYERS_METIER` : plus de N puces identiques « Bâti dur » dans Prescriptions. Le détail bâti n’est plus dans le corps du CUA (la synthèse recap le liste). Si on revert ceci sans recap, le bâti disparaît des deux côtés.

### 5.e Divers rendu (sans effet métier)

- `nom_affichage` : signature « Argelès-sur-Mer » au lieu de `nom.title()` → « Argelès-Sur-Mer ».
- Formats `fmt_num` / `fmt_pct` (1 252 m², 9,5 %).
- `add_title_bar` : bordures XML avant le fond (Word / LibreOffice).
- `add_kv_table` : `autofit = False` + largeur de colonnes.
- `_bloc_couche` : `est_multi_entites(rows)` (objets déjà filtrés, plus les DPU).
- Section prescriptions : on teste `_objets_affichables` avant d’afficher la barre (évite une section vide s’il ne reste que du DPU).
- Notes Natura : déplacées **dans** le bloc Natura/Prairies (plus sous ENEDIS).
- Routage mort retiré : `ppr` / `pprif` / `alea_feu` / `zonage_plu` n’étaient déjà plus dans le flux générique (`LAYERS_METIER`). `PRESCRIPTION_PLU_KEYS` idem.

---

## Ordre de rollback recommandé si un CUA « ne ressemble plus »

1. **D’abord** restaurer `zonage_plu.py` et `ppr_et_pprif.py` (c’est là que l’intro / les blocs PPR peuvent bouger).
2. Dans `builder.py`, seulement `_items_avec_pct` / hauteurs si l’intro hauteurs change.
3. `ST_CollectionExtract` seulement si une couche surfacique « disparaît » de façon anormale (peu probable).
4. Garder les garde-fous `erreur` / `table_absente` et la date de dépôt : ce ne sont pas des seuils, c’étaient des contre-vérités.

Comparer un même dossier (ex. BR273, BR303) **avant / après** :

- intro zonage (libellés + %) ;
- présence/absence des sous-zones PPR ;
- intro hauteurs ;
- DPU (oui / non / indisponible) ;
- liste des SUP.

Les fichiers `tests/output/rapport_intersections_*.json` déjà générés **avant** ces mods restent valides pour rejouer le builder : le SQL n’a pas besoin d’être relancé pour juger le rendu zonage/PPR Python. En revanche `ST_CollectionExtract` et le non-écrasement des status KO ne se voient que sur un **nouveau** `run_intersections`.

---

## Verdict

Les mods hors recap se classent en deux familles :

1. **Correctifs de vérité / rendu** (DPU, status KO, date de dépôt, formats, XML, servitude sans type, notes Natura, bâti). Risque faible, plutôt des bugs.
2. **Agrégation Python max → somme** (zonage, PPR, hauteurs). Avec le SQL toujours à 1 % par objet, ça corrige surtout des % trop bas. Ça ne réintroduit pas les micro-recouvrements frontaliers que l’audit avait voulu tuer.

Si on doit choisir une seule chose à revertir en cas de doute client : **`ppr_et_pprif.py`**, puis **`zonage_plu.py`**.
