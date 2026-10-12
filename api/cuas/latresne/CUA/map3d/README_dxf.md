# Export DXF « pack CAO » (`map3d/dxf/`)

Package CAO qui **remplace** l’ancien `terrain_dxf.py` monolithique.  
Le terrain (courbes, TIN) est dans `dxf/terrain.py` ; les couches SIG de la carte 2D sont dans `dxf/sig.py`.

Objectif : **la carte 2D actuelle, mais en DXF**, plus la topographie — pour un architecte / dessinateur CAO.

Point d’entrée pipeline : `write_frozen_terrain_files()` → `build_cad_package()`.

---

## Livrable

Un appel à `build_cad_package(...)` produit `<nom>_pack_cao.zip` :

| Fichier | Rôle |
|---|---|
| `<nom>_plan_2D.dxf` | Fichier principal : SIG réglementaire + cadastre UF + courbes / points cotés, **tout à Z=0** |
| `<nom>_terrain_3D.dxf` | Secondaire : maillage TIN + UF / parcelles **drapées**. Pas de couches PLU |
| `LISEZMOI.txt` | CRS, origine, sources, liste des calques |

Les deux payloads sont facultatifs : sans `terrain_payload` → plan réglementaire seul ; sans `carto_context` → pack terrain + cadastre depuis le MNT.

---

## Plan 2D (ce que voit l’archi)

| Carte web | DXF |
|---|---|
| Catalogue (zonage, prescriptions, SUP, risques, env, réseaux, cadastre) | Calque par couche, préfixe famille : `PLU_…`, `PRESC_…`, `SUP_…`, `RISQ_…`, `ENV_…`, `RES_…` |
| Groupes (UA, N, zone rouge…) | Sous-calques `PLU_ZONAGE_UA`, `PLU_ZONAGE_N`… |
| Remplissage semi-transparent | Hachures `…_HACH` (masquables d’un coup) |
| Couleurs légende | RGB + ACI proche pour impression |
| Libellés | Calques `…_ETIQ` |
| Popup attributs | XDATA + hyperlien règlement si URL |
| Légende latérale | Bloc légende à droite du plan |
| UF + tampon | `CAD_UF_LIMITE`, `CAD_UF_PARCELLES` + numéros, `EMPRISE_ETUDE` |
| *(hors carte)* | Courbes / cotes / points cotés `TN_…`, cartouche |

Pas de fond IGN. Le choix « UF vs alentours » se fait à l’export : `SigOptions(scope="uf"|"alentours")` (défaut alentours).  
Toutes les couches **avec données** sont exportées (pas seulement celles allumées sur la carte). Pour coller à l’état par défaut : `SigOptions(only_visible_by_default=True)`.

---

## API

```python
from api.cuas.latresne.CUA.map3d.dxf import build_cad_package, PackOptions

result = build_cad_package(
    "exports/AB12",
    terrain_payload=payload_mnt,      # même payload que la carte 3D
    carto_context=context,            # même dict que render_carto_context_html
    carto_catalogue=catalogue,
    options=PackOptions(name="AB12"),
)
result["zip"]
```

`write_frozen_terrain_files(..., carto_context=..., carto_catalogue=..., meta=...)` transmet ces arguments.

---

## Fichiers

| Fichier | Rôle |
|---|---|
| `dxf/core.py` | CRS, calques, cartouche, légende, textes, échelle 1/`scale` |
| `dxf/terrain.py` | MNT : courbes, points cotés, TIN 3D |
| `dxf/sig.py` | Couches catalogue + cadastre UF / parcelles |
| `dxf/pack.py` | Assembleur zip + fusion parcelles MNT |

---

## Calques notables

| Calque | Fichier | Contenu |
|---|---|---|
| `TN_MAILLAGE_3D` | 3D | TIN |
| `CAD_UF_LIMITE_3D` | 3D | Limite UF drapée |
| `CAD_UF_PARCELLES_3D` | 3D | Parcelles drapées |
| `TN_COURBES_*` / `TN_POINTS_COTES` | 2D | Topo |
| `CAD_UF_LIMITE` / `CAD_UF_PARCELLES` / `CAD_UF_NUMEROS` | 2D | Cadastre projet |
| `EMPRISE_ETUDE` | 2D | UF + tampon |
| `PLU_` / `PRESC_` / `SUP_` / … | 2D | Catalogue (Argelès via `carto_context`) |
| `CARTOUCHE` / `LEGENDE` | 2D (+ cartouche 3D) | Métadonnées |

---

## Options

### `PackOptions`

`name`, `dxf_version` (R2013), `crs` (`L93` ou `CC42`…`CC50`), `local_origin` (Revit/SketchUp), `scale` (défaut 500), `project_label`.

### `TerrainOptions`

`pixel_is_center=True` (demi-pixel GeoTIFF IGN — à caler sur le payload), `flatten_2d=True` (plan de masse à Z=0), `mesh_max_error_m=0.05`, `mesh_entity=polyface|mesh|3dface`.

### `SigOptions`

`scope="alentours"|"uf"`, `only_visible_by_default=False`, `hatch_mode="solid"`.

---

## Intégration

- **Argelès** : `generate_cua` passe `carto_payload` + `carto_catalogue` → plan 2D = carte MapLibre vectorielle + topo.
- **Latresne** : pas de `carto_context` MapLibre ; le pack contient topo + UF/parcelles depuis le payload MNT (pas les couches Folium métier, format différent).

Upload Storage : zip `*_pack_cao.zip`. Bouton projet : « Télécharger DXF pour CAO ».

---

## Dépendances

`ezdxf`, `numpy`, `contourpy` (requis), `pydelatin` (recommandé), `pyproj`, `shapely`.
