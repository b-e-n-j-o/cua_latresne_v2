---

## Comment Kerelia sert ses PDF et cartes avec un beau domaine

### Le problème de départ

Supabase Storage héberge les fichiers (PDF, HTML), mais ses URLs publiques posent deux problèmes :
- Elles contiennent `*.supabase.co` — pas `kerelia.fr`
- Elles renvoient parfois un mauvais `Content-Type` (ex. `text/plain` pour un fichier HTML), ce qui fait que le navigateur affiche le code source au lieu de rendre la carte

### La solution : un proxy dans FastAPI

L'endpoint `GET /api/identite-fonciere/public/if/{project_id}/{filename}` joue le rôle de **proxy transparent** :

1. Le navigateur demande `https://api.kerelia.fr/api/identite-fonciere/public/if/if_xxx/carte.html`
2. FastAPI récupère l'URL privée Supabase via `public_object_url()` et fait un `requests.get()` dessus côté serveur
3. Il retourne le contenu avec le bon `Content-Type` forcé (`text/html; charset=utf-8` ou `application/pdf`)
4. L'utilisateur ne voit jamais l'URL Supabase

C'est la fonction `proxy_identite_fonciere_depuis_storage()` dans `route_identite_parcelle.py`.

### Pourquoi `api.kerelia.fr` et pas `www.kerelia.fr`

`www.kerelia.fr` sert le site vitrine (hébergé ailleurs). Y faire passer l'API nécessiterait un reverse proxy Nginx ou Cloudflare Worker. Le sous-domaine `api.kerelia.fr` est plus simple : un CNAME OVH → Render, Render accepte ce domaine custom, tout est propre.

### Le chemin d'une URL générée

```
POST /publier
  → génère project_id (ex. if_0b7d8457...)
  → upload HTML + PDF dans Supabase Storage
  → _prefer_identite_proxy_url() remplace l'URL supabase.co
    par https://api.kerelia.fr/api/identite-fonciere/public/if/{project_id}/...
  → stocke ces URLs propres en base (latresne.identite_fonciere_projects)
  → les retourne au front dans carte_url et pdf_url
```

### La config infra (ce qu'on vient de faire)

| Étape | Où | Ce qui a été fait |
|---|---|---|
| CNAME | OVH | `api` → `cua-latresne-v2-623d.onrender.com.` |
| Custom domain | Render | `api.kerelia.fr` ajouté + vérifié |
| Var d'env | Render | `PUBLIC_API_BASE_URL=https://api.kerelia.fr` |

La variable `PUBLIC_API_BASE_URL` est lue par `_public_api_base_url()` dans le router — c'est elle qui préfixe toutes les URLs générées.