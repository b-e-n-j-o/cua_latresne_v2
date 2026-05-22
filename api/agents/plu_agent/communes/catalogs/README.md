# Catalogues de couches par commune

## Principe

| Fichier | Rôle |
|---------|------|
| `default.json` | Toutes les couches « standard » (zonage, prescriptions, servitudes, infos) |
| `argeles.json` | **Uniquement les différences** pour Argelès (peut être vide `{}`) |
| `latresne.json` | Différences Latresne : nouvelles couches, `enabled: false`, autre nom de table |

Chargement : `load_commune_catalog("latresne")` dans `layer_catalog.py`.

**Source de vérité agent PLU** : uniquement `default.json` + `{slug}.json` ici. Le catalogue identité foncière (`api/identite_fonciere/catalogues/`) sert un autre service — pas de lien au runtime ni de script de sync obligatoire.

## Actions courantes

**Désactiver une couche** (ex. pas d'infos ponctuelles à Latresne) :

```json
{
  "layers": {
    "infos_pct": { "enabled": false }
  }
}
```

**Renommer une table** (même logique, autre nom en BDD) :

```json
{
  "layers": {
    "servitudes": { "table": "sup_assiette_s_custom" }
  }
}
```

**Ajouter une couche** (PPRT) :

```json
{
  "layers": {
    "pprt_surf": {
      "enabled": true,
      "table": "pprt_surf",
      "group": "pprt",
      "subgroup": "surfaciques",
      "kind": "surfacique",
      "color": "#6A4C93",
      "strict_parcel": true,
      "optional": true,
      "context_llm": true,
      "context_carto": true,
      "attributes": ["gml_id", "libelle"]
    }
  }
}
```

**Changer les attributs LLM/carte** : modifier `attributes` (ou `keep`) sur l'id de couche.

**Couches hors GPU (Latresne, PPRT, …)** : même schéma + libellés lisibles pour le LLM :

```json
"radon": {
  "table": "radon",
  "title": "IRSN - Radon métropole",
  "group": "prescription_locale",
  "keep": ["classe_pot", "reglementation"],
  "clean_attributes": ["Classe", "Réglementation"],
  "context_llm": true
}
```

- `keep` / `attributes` → colonnes SQL (`SELECT p.classe_pot, …`).
- `clean_attributes` → clés envoyées au LLM dans `couches_supplementaires` (ex. `"Classe": "3"`).
- `title` / `nom` → champ `couche` sur chaque élément du contexte.

Le `group` ne doit pas être `prescriptions` / `servitudes` / `informations` (réservés au socle GPU) : utiliser `prescription_locale`, `servitude`, `information`, etc.

## Champs utiles

| Champ | Description |
|-------|-------------|
| `enabled` | `false` = couche ignorée partout |
| `table` | Nom table dans le schéma commune (`latresne.<table>`) |
| `title` / `nom` | Libellé humain de la couche dans le contexte LLM |
| `keep` / `attributes` | Colonnes SQL à récupérer |
| `clean_attributes` | Libellés correspondants pour le LLM (même ordre que `keep`) |
| `strict_parcel` | `true` = intersection stricte parcelle (LLM + carto extra par défaut) |
| `inclu_buffer` | `true` = carte : entités dans le buffer ; défaut = parcelle seule, buffer = clip GeoJSON |
| `buffer_m` | Zonage : `strict_parcel: false` → sélection dans le buffer (socle GPU) |
| `context_llm` | Inclure dans `get_contexte_parcelle` → `couches_supplementaires` |
| `context_carto` | Inclure dans `GET /map` → clé `extra` |
| `optional` | Ne pas faire échouer si la table/colonne manque |

Le code métier lit ce catalogue via `CommuneProfile.catalog` :
- GPU : `prescriptions.py`, `servitudes.py`, `infos.py` (via `catalog_bridge`)
- orchestration : `cartography/spatial_context.py` → `contexte_parcelle` / `cartography/carto`
- hors GPU : `fetch_layer.py` → clé `couches_supplementaires` (LLM) et `extra` (carte)

## Couches custom sur la carte (multi-communes)

Dans `{slug}.json`, toute couche dont le `group` **n'est pas** `zonage` / `prescriptions` / `servitudes` / `informations` / `parcelle` est traitée comme **extra** :

```json
"radon": {
  "table": "radon",
  "title": "IRSN - Radon",
  "group": "prescription_locale",
  "kind": "surfacique",
  "color": "#9D4EDD",
  "context_llm": true,
  "context_carto": true,
  "keep": ["classe_pot"],
  "clean_attributes": ["Classe"]
}
```

- `context_carto: true` → GeoJSON dans `GET /map` → `extra.radon`
- `context_llm: true` → `get_contexte_parcelle` → `couches_supplementaires`
- Le frontend boucle sur `mapData.extra` (aucune liste en dur par couche)
- Carte (couches extra) :
  - **Défaut** : entités qui intersectent la **parcelle** (strict) ; géométrie **clippée** au `buffer_m` pour l'affichage léger
  - **`inclu_buffer: true`** : sélection dans le **buffer** (fossés, réseaux linéaires le long de la parcelle) + clip buffer
  - **Zonage GPU** : `strict_parcel: false` + `buffer_m` dans `default.json` (sélection + clip buffer, géré à part)
- Argelès : pas d'entrées extra → `extra: {}` ; Latresne : 33 ids dans `latresne.json` → 33 clés possibles dans `extra`
