# Tests automatisés — guide rapide

Ce dossier contient les tests **pytest** du backend Kerelia CUA.  
Pour l’instant, seuls les **smoke tests auth** sont en place (vérification bout-en-bout après deploy).

---

## Comment pytest fonctionne (en 30 secondes)

1. Tu lances `pytest` depuis la racine `cua_latresne_v4/`.
2. Pytest **scanne** le dossier `tests/` et trouve les fichiers `test_*.py`.
3. Chaque fonction `def test_...()` est un **cas de test** : si un `assert` échoue → test rouge.
4. Avant les tests, pytest charge `conftest.py` (fixtures partagées, options CLI).
5. Résultat : `X passed`, `Y failed`, `Z skipped`.

Ce n’est pas magique : ce sont des scripts Python qui appellent ton API / Supabase et vérifient les réponses.

---

## Arborescence actuelle

```
cua_latresne_v4/
├── pytest.ini                 # config pytest (où chercher les tests, marqueurs)
├── .env                       # config app (Supabase, DB…) — pas les mdp de test
├── .env.test.example          # modèle commité
├── .env.test.local            # tes identifiants de test — gitignoré
├── scripts/
│   └── test_auth_commune_access.py   # même logique, en CLI manuelle
└── tests/
    ├── README.md              # ce fichier
    ├── conftest.py            # fixtures + option --prod
    ├── test_env.py            # chargement des .env
    └── smoke/
        ├── auth_e2e.py        # fonctions réutilisables (login, appels HTTP)
        └── test_auth_commune_access.py   # les vrais tests pytest
```

À terme, tu pourras ajouter :

- `tests/unit/` — fonctions Python isolées (sans réseau)
- `tests/integration/` — API locale avec `TestClient` FastAPI (sans vrai login)

---

## `pytest.ini` — la config globale

Fichier à la racine du backend. Dit à pytest :

- **`testpaths = tests`** → cherche les tests dans `tests/`
- **`pythonpath = .`** → permet les imports `from tests.smoke...`
- **`markers = smoke`** → étiquette pour les tests réseau / Supabase

Tu peux filtrer : `pytest -m smoke` ne lance que les tests marqués `@pytest.mark.smoke`.

---

## `test_env.py` — séparer config app et identifiants de test

**Problème évité :** mélanger dans `.env` les clés API de l’app et le mot de passe d’un compte de test.

**Rôle :**

| Fonction | Charge quoi |
|----------|-------------|
| `load_app_env()` | `.env` — `SUPABASE_URL`, clés Supabase, etc. |
| `load_test_credentials_env()` | `.env.test.local` — `AUTH_TEST_EMAIL`, `AUTH_TEST_PASSWORD`, … |
| `load_all_test_env()` | les deux, dans cet ordre |

Les variables déjà définies dans le **shell** (ou en CI) ne sont pas écrasées — pratique pour GitHub Actions avec des secrets.

**Setup une fois :**

```bash
cp .env.test.example .env.test.local
# éditer .env.test.local avec email / mdp de test
```

---

## `conftest.py` — le « kit de départ » de tous les tests

Pytest charge **automatiquement** ce fichier. Tu n’as pas besoin de l’importer.

### Ce qu’il fait

1. **Au démarrage** : `load_all_test_env()` (lit `.env` + `.env.test.local`).
2. **Option `--prod`** : ajoute un flag CLI pour cibler `https://api.kerelia.fr` au lieu de `localhost:8000`.
3. **Fixtures** : objets préparés une fois et réinjectés dans les tests.

### Les fixtures (ingrédients prêts à l’emploi)

Une **fixture**, c’est une fonction décorée `@pytest.fixture`.  
Quand un test déclare un paramètre du même nom, pytest lui passe la valeur automatiquement.

| Fixture | Fournit | Scope |
|---------|---------|-------|
| `api_base` | URL de l’API (`localhost` ou prod selon `--prod`) | session |
| `auth_credentials` | `(email, password)` depuis `.env.test.local` | session |
| `auth_session` | login Supabase → `{access_token, user_id, …}` | session |
| `expect_superadmin` | `True` si `AUTH_TEST_EXPECT_SUPERADMIN=1` | session |
| `commune_slug` | ex. `argeles` pour `/pipelines/by_user` | session |

**`scope="session"`** = calculé **une seule fois** par run pytest (pas un login Supabase par test).

Si `AUTH_TEST_EMAIL` / `AUTH_TEST_PASSWORD` manquent → les tests qui ont besoin de `auth_session` sont **skipped** (ignorés), pas en erreur.

### Exemple de chaîne

```
test_commune_access_with_bearer(api_base, auth_session)
         │                        │           │
         │                        │           └── conftest : login Supabase (1×)
         │                        └── conftest : http://127.0.0.1:8000 ou api.kerelia.fr
         └── test_auth_commune_access.py : assert status 200
```

---

## Dossier `smoke/` — tests « ça marche en vrai ? »

**Smoke test** = test de fumée : on vérifie que le système **répond** correctement de bout en bout, avec du **vrai réseau** (Supabase + API déployée ou locale).

Ce n’est **pas** un test unitaire (pas de mock, pas ultra rapide).

### `auth_e2e.py` — la boîte à outils

Code partagé entre pytest et le script CLI `scripts/test_auth_commune_access.py` :

- `supabase_sign_in()` — login email/mdp → JWT
- `api_get()` — GET HTTP vers l’API avec ou sans Bearer
- `run_checks()` — enchaîne tous les contrôles (équivalent du rapport CLI)

Pas de `test_` dans le nom → pytest **ne l’exécute pas** seul, c’est une bibliothèque.

### `test_auth_commune_access.py` — les tests proprement dits

Chaque `def test_...` = un scénario :

| Test | Vérifie |
|------|---------|
| `test_commune_access_requires_bearer` | Sans token → **401** (détecte l’ancien backend qui renvoyait 422) |
| `test_commune_access_rejects_legacy_query_user_id` | `?user_id=` seul ne suffit plus |
| `test_commune_access_with_bearer` | JWT valide → **200** + droits renvoyés |
| `test_superadmin_when_expected` | Superadmin si flag activé (sinon skip) |
| `test_supabase_token_valid_like_backend` | Même validation que `current_user.py` |
| `test_pipelines_by_user` | Historique CUA accessible |
| `test_auth_e2e_suite` | Récap groupé (comme le script CLI) |

`pytestmark = pytest.mark.smoke` → tous ces tests portent le marqueur `smoke`.

---

## Commandes utiles

```bash
# Local (uvicorn doit tourner sur :8000)
pytest tests/smoke -v

# Prod Render (après deploy)
pytest tests/smoke --prod -v

# Un seul test
pytest tests/smoke/test_auth_commune_access.py::test_commune_access_requires_bearer -v

# Script CLI équivalent (debug manuel)
python scripts/test_auth_commune_access.py --prod --email ... --password '...'
```

---

## Flux complet d’un run `pytest tests/smoke --prod -v`

```
1. pytest lit pytest.ini
2. pytest charge tests/conftest.py
      → load .env + .env.test.local
      → enregistre l’option --prod
3. pytest collecte tests/smoke/test_auth_commune_access.py
4. Pour chaque test :
      → injecte api_base (= https://api.kerelia.fr car --prod)
      → si besoin : auth_session (login Supabase une fois)
      → exécute les assert
5. Affiche passed / failed / skipped
```

---

## skipped vs failed

| Résultat | Signification |
|----------|---------------|
| **PASSED** | Tout est OK |
| **FAILED** | Un `assert` a échoué → bug ou prod cassée |
| **SKIPPED** | Test volontairement ignoré (ex. pas de mdp dans `.env.test.local`, ou test superadmin sans le flag) |

Un skip n’est pas une erreur ; un fail oui.

---

## Évolution prévue

| Dossier | Usage futur |
|---------|-------------|
| `tests/unit/` | `commune_access.py`, parsing, fail-closed — sans réseau |
| `tests/integration/` | `TestClient(app)` — API en mémoire, pas de deploy |
| `tests/smoke/` | Post-deploy, login réel, prod + local |
| `.github/workflows/` | Lancer `pytest` automatiquement à chaque push (CI) |

---

## Résumé en une phrase

**`test_env`** charge les bons fichiers de config, **`conftest`** prépare login et URL, **`smoke/`** contient les tests qui tapent vraiment Supabase et l’API — pytest orchestre le tout quand tu lances `pytest`.
