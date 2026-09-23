# Pipeline CUA Latresne

Dossier **vivant** (HTTP + jobs). Argelès reste dans `api/cuas/argeles/` (builder et routing séparés).

## Run HTTP

- `POST /analyze-parcelles`
- `POST /analyze-parcelles-with-json-data`
- polling : `GET /status/{job_id}`

Entrée : `app/routers/cua_pipeline.py` → `app/pipeline_jobs.py` → sous-processus
`INTERSECTIONS/pipeline_from_parcelles.py`.

## Étapes

1. UF — `CERFA_ANALYSE/verification_unite_fonciere.py`
2. Intersections PostGIS — `INTERSECTIONS/intersections.py`
3. Carto 2D Folium — `CUA/map2d/`
4. Carto 3D Plotly + MNT (`public.mnt_dalles`) — `CUA/map3d/map_3d.py`
5. DOCX — `CUA/docx/cua_builder.py`
6. Persist Supabase storage `visualisation` + table `latresne.pipelines` — `CUA/sub_orchestrator_cua.py`

## Catalogues de couches

| Fichier | Usage |
|---|---|
| `catalogues/catalogue_intersections_tagged.json` | Intersections CUA + GPKG + articles du DOCX |
| `catalogues/catalogue_couches_map.json` | Carto 2D (styles / attributs carte) |

Copies de travail aussi dans `INTERSECTIONS/` et `CUA/` : le code charge **`catalogues/`** à la racine de ce dossier.

L’historique `api/communes/latresne/cuas/` n’est plus l’entrée HTTP.
