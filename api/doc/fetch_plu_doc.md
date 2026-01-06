# Documentation - Système de consultation PLU/PLUI

## Vue d'ensemble

Ce système permet de consulter la réglementation d'urbanisme (PLU/PLUI) de manière contextuelle et optimisée :
- **PLU classiques** : Consultation du règlement complet de la commune
- **PLUI Bordeaux Métropole** : Consultation ciblée par zone (ex: UP27 = 40 pages au lieu de 7000)

---

## Architecture générale

```
Clic sur carte → Détection commune → Détection zone PLUI → Récupération PDF ciblé
```

### 1. Détection du contexte territorial

**Au clic sur la carte** :
1. Récupération des coordonnées GPS
2. Reverse geocoding (API Adresse) → Code INSEE + nom commune
3. Interrogation base de données → Vérifier si commune appartient à un EPCI avec PLUI

**Mapping commune → EPCI** (table `plu_epci_mapping`) :
```sql
insee | commune_name | epci_code    | epci_name          | document_type
33063 | Bordeaux     | 243300316    | Bordeaux Métropole | PLUI
33281 | Mérignac     | 243300316    | Bordeaux Métropole | PLUI
33234 | Latresne     | 33234        | NULL               | PLU
```

---

## 2. Cas PLU simple (commune isolée)

**Exemple : Latresne (33234)**

```
Clic → INSEE 33234 → Pas d'EPCI → PLU communal
```

**Endpoints** :
- `/api/plu/check/33234` → Vérifie disponibilité
- `/api/plu/reglement/33234` → Retourne PDF complet (1-2 Mo)

**Stockage cache** :
```
reglements/33234.pdf
```

---

## 3. Cas PLUI avec zonage (Bordeaux Métropole)

**Exemple : Clic à Bordeaux centre (zone UP27)**

### Étape 1 : Détection de la zone

```
Clic (lon, lat) → INSEE 33063 → EPCI 243300316
                ↓
Requête PostGIS sur table zonage PLU
                ↓
ST_Intersects(geom_2154, point) → Zone UP27
```

**Fonction SQL** :
```sql
CREATE FUNCTION get_zonage_at_point(
    code_siren TEXT,  -- 243300316
    lon FLOAT,
    lat FLOAT
) RETURNS TABLE(libelle TEXT)
```

**Requête** :
```sql
SELECT libelle FROM carto.plu
WHERE source_type = 'PLUI'
  AND idurba LIKE '243300316%'
  AND ST_Intersects(geom_2154, ST_Transform(point, 2154))
```

### Étape 2 : Récupération du règlement de zone

**Endpoints** :
- `/api/plu/zonage/33063?lon=X&lat=Y` → `{"zones": ["UP27"]}`
- `/api/plu/reglement/33063/zone/UP27` → PDF 40 pages

**Stockage cache** (180 zones pré-découpées) :
```
reglements/243300316/AB.pdf
reglements/243300316/UP27.pdf
reglements/243300316/UM12.pdf
...
```

---

## 4. Flux complet côté frontend

### MapPage.tsx

```typescript
async function fetchParcelleParPoint(lon: number, lat: number) {
  // 1. Récupérer commune
  const communeInfo = await getInseeFromCoordinates(lon, lat);
  setCurrentInsee(communeInfo.insee);
  setCurrentCommune(communeInfo.commune);
  
  // 2. Récupérer zonage PLUI (si applicable)
  const zonageRes = await fetch(
    `${apiBase}/api/plu/zonage/${communeInfo.insee}?lon=${lon}&lat=${lat}`
  );
  const zonageData = await zonageRes.json();
  setCurrentZones(zonageData.zones); // Ex: ["UP27"]
  
  // 3. Afficher parcelles...
}
```

### PLUConsultation.tsx

```typescript
const openPLU = async () => {
  const apiBase = import.meta.env.VITE_API_BASE;
  
  if (zones && zones.length > 0) {
    // PLUI : règlement de zone
    const res = await fetch(
      `${apiBase}/api/plu/reglement/${inseeCode}/zone/${zones[0]}`
    );
  } else {
    // PLU classique : règlement complet
    const res = await fetch(
      `${apiBase}/api/plu/reglement/${inseeCode}`
    );
  }
  
  const data = await res.json();
  window.open(data.url, '_blank');
};
```

---

## 5. Backend FastAPI

### Structure des endpoints

```python
# Détection zone
@router.get("/zonage/{insee}")
async def get_zonage_at_point(insee: str, lon: float, lat: float):
    plu_info = get_plu_code(insee)  # Résolution commune → EPCI
    if plu_info["type"] != "PLUI":
        return {"zones": []}  # PLU simple
    
    # Requête PostGIS
    result = supabase.rpc("get_zonage_at_point", {
        "code_siren": plu_info["code"],  # 243300316
        "lon": lon,
        "lat": lat
    })
    return {"zones": [z["libelle"] for z in result.data]}

# Règlement complet (PLU)
@router.get("/reglement/{insee}")
async def get_reglement_plu(insee: str):
    plu_code = get_plu_code(insee)["code"]
    cached_url = get_cached_plu(plu_code)
    return {"url": cached_url}

# Règlement par zone (PLUI)
@router.get("/reglement/{insee}/zone/{zone}")
async def get_reglement_zone(insee: str, zone: str):
    plu_code = get_plu_code(insee)["code"]  # 243300316
    cached_url = get_cached_plu(f"{plu_code}/{zone}")  # 243300316/UP27
    return {"url": cached_url}
```

---

## 6. Cache Supabase Storage

### Structure du bucket `plu-reglements-cached`

```
reglements/
├── 33234.pdf                    # PLU Latresne (1 Mo)
├── 33281.pdf                    # PLU autre commune
├── 243300316/                   # PLUI Bordeaux Métropole
│   ├── AB.pdf                   # Zone AB (20 pages)
│   ├── UM12.pdf                 # Zone UM12 (40 pages)
│   ├── UP27.pdf                 # Zone UP27 (35 pages)
│   └── ... (180 zones au total)
```

### Fonction de récupération cache

```python
def get_cached_plu(path: str) -> str | None:
    """
    path = '33234'           → reglements/33234.pdf
    path = '243300316/UP27'  → reglements/243300316/UP27.pdf
    """
    file_path = f"reglements/{path}.pdf"
    result = supabase.storage.from_("plu-reglements-cached")\
        .create_signed_url(file_path, expires_in=3600)
    return result.get("signedURL")
```

---

## 7. Avantages du système

| Critère | PLU classique | PLUI par zone |
|---------|---------------|---------------|
| **Téléchargement** | 1-5 Mo | 200-500 Ko |
| **Pages** | 50-200 | 20-50 |
| **Temps d'accès** | 1-5s | 0.2-0.5s (cache) |
| **Pertinence** | Toute la commune | Seulement la zone concernée |

**Cas d'usage Bordeaux Métropole** :
- Règlement complet : 7000 pages, 50 Mo
- Zone UP27 seule : 40 pages, 400 Ko
- **Gain : 175x en taille, 100% de pertinence**

---

## 8. Données requises

### Base PostGIS (table `carto.plu`)

```sql
CREATE TABLE carto.plu (
  id uuid PRIMARY KEY,
  insee text,           -- Code INSEE commune
  source_type text,     -- 'PLU' ou 'PLUI'
  libelle text,         -- 'UP27', 'UM12', etc.
  idurba text,          -- '243300316_PLUI_20250903'
  geom_2154 geometry    -- Géométrie zone en Lambert 93
);
```

### Table de mapping (table `plu_epci_mapping`)

```sql
CREATE TABLE plu_epci_mapping (
  insee text PRIMARY KEY,
  commune_name text,
  epci_code text,
  epci_name text,
  document_type text    -- 'PLU' ou 'PLUI'
);
```

---

## 9. Exemple complet de flow

**Utilisateur clique à Bordeaux centre (zone UP27)**

```
1. Clic carte → coords (-0.5737, 44.8321)
                    ↓
2. Reverse geocoding → INSEE 33063 (Bordeaux)
                    ↓
3. Lookup mapping → EPCI 243300316 (Bordeaux Métropole)
                    ↓
4. Requête PostGIS → ST_Intersects → Zone UP27
                    ↓
5. GET /reglement/33063/zone/UP27
                    ↓
6. get_cached_plu("243300316/UP27")
                    ↓
7. Supabase Storage → URL signée (valide 1h)
                    ↓
8. Frontend → window.open(url) → PDF 40 pages
```

**Affichage interface** :
```
┌────────────────────────────────┐
│ PLUI - Bordeaux Métropole      │
│ Zone UP27                       │
│                                 │
│ [Consulter le règlement] 🔗    │
└────────────────────────────────┘
```

---

## 10. Maintenance

### Ajout d'un nouveau PLUI

1. **Découper le PDF** en zones (script Python)
2. **Uploader** les 180 PDFs dans `reglements/{code_epci}/`
3. **Ajouter le mapping** dans `plu_epci_mapping`
4. **Importer le zonage** dans `carto.plu` (shapefile → PostGIS)

### Mise à jour d'un PLU/PLUI

- **Cache automatique** : expire après 30 jours (cron job)
- **Ré-upload manuel** : écrase avec `x-upsert: "true"`

---

## Technologies utilisées

- **Frontend** : React + TypeScript + MapLibre GL
- **Backend** : FastAPI + Python
- **Base de données** : PostgreSQL + PostGIS
- **Stockage** : Supabase Storage
- **Géocodage** : API Adresse (data.gouv.fr)
- **Source PLU** : Géoportail de l'Urbanisme (GPU)