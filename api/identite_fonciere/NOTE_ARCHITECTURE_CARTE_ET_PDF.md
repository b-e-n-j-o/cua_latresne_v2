# Identité foncière — cartes, PDF et URLs publiques

Document court pour expliquer le flux technique (carte HTML, rapport PDF, liens partageables).

---

## 1. Objectif

Après analyse d’une unité foncière (intersections couche par couche), le système :

1. **dépose** la carte Folium (HTML) et le rapport PDF sur **Supabase Storage** (bucket dédié, ex. `identite-fonciere`) ;
2. **expose** des **liens HTTPS** utilisables dans le PDF et par des tiers (pas de dépendance au navigateur d’origine).

---

## 2. Flux applicatif (résumé)

1. Le front lance l’analyse en **SSE** : `POST /api/identite-fonciere/intersect/stream`.
2. À la fin du flux, le front appelle **`POST /api/identite-fonciere/publier`** avec géométrie, intersections, etc.
3. Le backend :
   - génère le **HTML** de la carte ;
   - l’**upload** sur Storage (`{project_id}/carte.html`) ;
   - génère le **PDF** (page de garde avec lien « carte interactive » si une URL absolue `http(s)` est fournie dans le dict métier) ;
   - **upload** le PDF (`{project_id}/rapport_identite_fonciere.pdf`) ;
   - renvoie en JSON les URLs **`carte_url`** et **`pdf_url`**.

Les fichiers restent sur **Supabase** ; les URLs renvoyées pointent en principe vers notre **API** (proxy), pas uniquement vers l’URL brute `*.supabase.co` (meilleur `Content-Type` pour le HTML dans le navigateur).

---

## 3. Rôle du proxy `/api/identite-fonciere/public/if/{project_id}/{filename}`

L’URL publique directe Supabase peut renvoyer un **mauvais en-tête** `Content-Type` : le navigateur affiche alors le HTML comme **texte brut**.  

La route **`GET .../public/if/...`** sur notre API **télécharge l’objet depuis Storage** et le renvoie avec **`text/html; charset=utf-8`** (ou `application/pdf` pour le PDF).

Donc le **lien à mettre dans le PDF** pour une carte **partageable** est de la forme :

`{BASE_PUBLIQUE}/api/identite-fonciere/public/if/if_xxxxxxxx/carte.html`

où **`BASE_PUBLIQUE`** est l’URL **vue par l’utilisateur** (ex. domaine Kerelia ou URL Render).

---

## 4. Variable d’environnement `PUBLIC_API_BASE_URL`

Le backend construit ces liens à partir de :

- **`PUBLIC_API_BASE_URL`** (ou, pour certains chemins, **`IDENTITE_FONCIERE_PUBLIC_BASE_URL`**) : **origine publique** sans slash final, ex. `https://www.kerelia.fr` ou `https://cua-latresne-v2-xxx.onrender.com` ;
- à défaut : **`request.base_url`** (adapté au dev local, pas toujours correct derrière un reverse proxy).

**Important :** mettre `PUBLIC_API_BASE_URL=https://www.kerelia.fr` **ne suffit pas** si le domaine **www.kerelia.fr** ne **transmet pas** les requêtes `/api/...` vers le service qui héberge FastAPI (Render, etc.). Sinon l’utilisateur obtient une erreur (404, page d’hébergeur, « service suspended », etc.) alors que la même URL sur `*.onrender.com` fonctionne.

**À faire côté infrastructure Kerelia :** configurer un **reverse proxy** (Nginx, Cloudflare, règles hébergeur) du type :

`https://www.kerelia.fr/api/*` → `https://<service-backend>/api/*`

Tant que ce routage n’existe pas, il faut soit :

- garder **`PUBLIC_API_BASE_URL`** sur l’URL **réelle** du backend (ex. Render), soit  
- déployer / réactiver le service Render visé par le proxy.

---

## 5. Différence avec `https://www.kerelia.fr/maps?t=...` (autre produit)

Certaines cartes s’ouvrent via la **page front** `/maps` avec un paramètre **`t=`** (souvent une **payload encodée** contenant des **URLs Supabase** vers d’autres buckets/chemins, ex. visualisation CUA).

Dans ce cas :

- le **navigateur** charge **kerelia.fr** (site statique / SPA) ;
- le **JavaScript** du front récupère ensuite le HTML depuis **Supabase en direct** ;

**Aucun appel** n’est fait à `www.kerelia.fr/api/identite-fonciere/...` pour ces cartes-là. C’est pourquoi ça peut « marcher sur kerelia.fr » **sans** proxy `/api` sur ce domaine, alors que les liens **identité foncière** du PDF, eux, **doivent** atteindre **le backend** (ou rester en `*.onrender.com` tant que le proxy n’est pas en place).

---

## 6. Récapitulatif pour la mise en production « jolie URL »

| Étape | Action |
|--------|--------|
| 1 | Backend déployé et joignable (ex. Render). |
| 2 | Variables Storage : `SUPABASE_URL`, clé service, bucket identité foncière, etc. |
| 3 | **`PUBLIC_API_BASE_URL`** = URL que tu veux **afficher** dans les PDF et réponses JSON. |
| 4 | Si cette URL est **www.kerelia.fr** : **router `/api/*`** vers le même backend que Render. |
| 5 | Redémarrer / redéployer après changement d’env. |
| 6 | Régénérer un rapport : les anciens PDF gardent les anciennes URLs. |

---

## 7. Tests rapides

- **Intégration** : `API_BASE_URL=https://<ton-backend> python -m pytest tests/test_identite_fonciere_carte_lien.py -v` (variable **`API_BASE_URL`** = uniquement pour pytest, pas lue par l’app).
- **Manuel** : script `scripts/curl_identite_publier.sh` avec `API_BASE_URL` exportée.

---

*Document interne Kerelia — identité foncière (cua_latresne_v4).*
