# Gemini / Vertex — notes pour la veille RAA

Mémo rapide sur **quel client Google Gen AI utiliser** et **comment envoyer des PDF** à Gemini.

## Deux modes du SDK `google-genai`

| | **Vertex AI** (notre choix actuel) | **Gemini Developer** (ex-API Google AI Studio) |
|---|---|---|
| Client | `genai.Client(vertexai=True)` | `genai.Client(api_key="...")` sans `vertexai` |
| Auth | Clé API Vertex **ou** ADC (`gcloud auth application-default login`) | Clé `GEMINI_API_KEY` / Google AI Studio |
| Où chez nous | Chat PLU, analyse RAA | Ancien code RAA (avant juil. 2026) |
| Facturation | GCP / contrat Vertex | Google AI Studio |

Référence commune dans le repo :

```python
# api/agents/plu_agent/routes/chat.py
# api/raa/service_analyse_raa.py → get_client()
def get_client() -> genai.Client:
    if GEMINI_API_KEY:
        return genai.Client(vertexai=True, api_key=GEMINI_API_KEY)
    return genai.Client(vertexai=True)
```

Variables d'environnement : `GEMINI_API_KEY`, `GEMINI_MODEL` (voir `api/_env.py`).

---

## Envoyer un PDF à Gemini : 3 méthodes

### 1. Inline bytes — **ce qu'on utilise pour la RAA (Vertex OK)**

PDF lu en local, passé dans `generate_content` :

```python
pdf_bytes = pdf_path.read_bytes()

response = client.models.generate_content(
    model=GEMINI_MODEL,
    contents=[
        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt_texte,
    ],
    config=types.GenerateContentConfig(
        system_instruction=system_prompt,
        temperature=0.1,
    ),
)
```

- **Compatible Vertex et Developer**
- Limite : **~50 Mo** par PDF en inline
- Implémenté dans : `service_analyse_raa.py` → `_call_gemini()`
- Même pattern que : `services/analyse_cerfa_mistral/GEMINI/extraire_infos.py` (images JPEG)

### 2. Files API (`client.files.upload`) — **Developer uniquement**

Ancienne implémentation RAA (ne pas utiliser avec `vertexai=True`) :

```python
# ❌ Erreur avec Vertex :
# ValueError: This method is only supported in the Gemini Developer client.

client = genai.Client(api_key=GEMINI_API_KEY)  # pas vertexai=True

uploaded = client.files.upload(
    file=pdf_path,
    config=types.UploadFileConfig(mime_type="application/pdf"),
)
# Attendre state != PROCESSING…
response = client.models.generate_content(
    model=GEMINI_MODEL,
    contents=[uploaded, prompt],
    config=...,
)
client.files.delete(name=uploaded.name)
```

- **Uniquement client Developer**
- Utile pour gros PDF ou réutilisation multi-tours
- Fichiers temporaires côté Google (~48 h)

### 3. URI GCS — **Vertex, gros fichiers**

Pour PDF > 50 Mo ou stockage durable sur GCP :

```python
part = types.Part.from_uri(
    file_uri="gs://mon-bucket/raa/recueil.pdf",
    mime_type="application/pdf",
)
response = client.models.generate_content(model=..., contents=[part, prompt])
```

- Nécessite un bucket GCS + droits IAM sur le projet Vertex
- Pas implémenté dans la RAA aujourd'hui

---

## Chat PLU vs analyse RAA

| Besoin | Chat PLU | Analyse RAA |
|--------|----------|-------------|
| Entrée | Texte (+ tools) | PDF + prompt JSON |
| API | `generate_content` seul | `generate_content` + `Part.from_bytes` |
| Files API | Non | Non (plus, depuis fix Vertex) |

Le chat n'a jamais eu besoin de `files.upload` → passer en Vertex était trivial.  
La RAA avait besoin d'un **changement d'envoi du PDF**, pas seulement du client.

---

## Si un jour on repasse en client Developer

1. **Client** — dans `get_client()` :

```python
def get_client() -> genai.Client:
    key = GEMINI_API_KEY or os.environ.get("GOOGLE_API_KEY", "")
    if not key:
        raise RuntimeError("GEMINI_API_KEY manquante")
    return genai.Client(api_key=key)  # sans vertexai=True
```

2. **PDF** — soit garder `Part.from_bytes` (fonctionne aussi en Developer), soit réactiver Files API pour les gros recueils (voir section 2 ci-dessus).

3. **Chat PLU** — aligner `_build_gemini_client()` dans `chat.py` si on veut un seul mode partout.

4. **Clé API** — clé **Google AI Studio**, pas clé Vertex ; vérifier que l'API `generativelanguage.googleapis.com` n'est pas bloquée sur le projet (erreur `API_KEY_SERVICE_BLOCKED` vue en prod).

---

## Erreurs fréquentes

| Message | Cause | Action |
|---------|-------|--------|
| `only supported in the Gemini Developer client` | `files.upload` avec `vertexai=True` | Utiliser `Part.from_bytes` ou passer en client Developer |
| `API_KEY_SERVICE_BLOCKED` / `PERMISSION_DENIED` sur FileService | Clé Developer sur API bloquée | Utiliser Vertex ou débloquer l'API dans GCP |
| `Fichier Gemini bloqué en PROCESSING` | Files API, upload lent | Timeout ; ou passer en inline bytes |
| `PDF trop volumineux` (> 50 Mo) | Limite inline | GCS + `Part.from_uri` ou Files API (Developer) |

---

## Fichiers concernés

- `api/raa/service_analyse_raa.py` — client + `_call_gemini`
- `api/raa/raa_api.py` — endpoints HTTP, tâches de fond
- `api/agents/plu_agent/routes/chat.py` — référence client Vertex texte
- `api/_env.py` — `GEMINI_API_KEY`, `GEMINI_MODEL`
