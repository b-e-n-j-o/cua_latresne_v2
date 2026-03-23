-- ============================================================
-- ProjectDirectory + ProjectFile (schema latresne)
-- ============================================================

-- 1) Bucket de stockage des fichiers projet
insert into storage.buckets (id, name, public)
values ('project-directories', 'project-directories', true)
on conflict (id) do nothing;

-- 2) Table dossier logique par projet
create table if not exists latresne.project_directories (
  id uuid primary key default gen_random_uuid(),
  project_slug text not null unique,
  storage_prefix text not null,
  user_id text,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_project_directories_slug
  on latresne.project_directories(project_slug);

-- 3) Table métadonnées fichiers projet
create table if not exists latresne.project_files (
  id uuid primary key default gen_random_uuid(),
  project_directory_id uuid not null references latresne.project_directories(id) on delete cascade,
  project_slug text not null,
  file_kind text not null,
  filename text not null,
  mime_type text,
  size_bytes bigint,
  storage_bucket text not null default 'project-directories',
  storage_path text not null,
  public_url text,
  source text not null default 'user_upload', -- user_upload | pipeline_output
  uploaded_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_project_files_slug
  on latresne.project_files(project_slug);

create index if not exists idx_project_files_directory
  on latresne.project_files(project_directory_id);

-- 4) Policies Storage (adapte selon ton auth modèle)
-- NOTE: ici on ouvre lecture publique + upload/delete pour users authentifiés
-- via chemins /projects/{slug}/...

-- Lecture publique
drop policy if exists "project_files_public_read" on storage.objects;
create policy "project_files_public_read"
on storage.objects for select
to public
using (bucket_id = 'project-directories');

-- Upload authentifié
drop policy if exists "project_files_auth_insert" on storage.objects;
create policy "project_files_auth_insert"
on storage.objects for insert
to authenticated
with check (bucket_id = 'project-directories');

-- Delete authentifié
drop policy if exists "project_files_auth_delete" on storage.objects;
create policy "project_files_auth_delete"
on storage.objects for delete
to authenticated
using (bucket_id = 'project-directories');

