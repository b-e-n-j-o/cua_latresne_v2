# -*- coding: utf-8 -*-
"""DDL photos / archives / filiation — partagé par --ensure-schema."""

from __future__ import annotations

import re

_SCHEMA_RE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


def assert_schema_name(schema: str) -> str:
    if not _SCHEMA_RE.fullmatch(schema or ""):
        raise ValueError(f"Nom de schéma SQL invalide: {schema!r}")
    return schema


def lookup_objects_ddl(schema: str) -> str:
    """Index de lookup + vues période / filiation (idempotent)."""
    s = assert_schema_name(schema)
    return f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_parcelles_archives_photo_idu
            ON {s}.parcelles_archives (photo_id, idu);

        CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_parents_gin
            ON {s}.cadastre_veille_evenements USING GIN (parents);
        CREATE INDEX IF NOT EXISTS idx_cadastre_veille_evenements_enfants_gin
            ON {s}.cadastre_veille_evenements USING GIN (enfants);

        CREATE OR REPLACE VIEW {s}.cadastre_photos_periode AS
        SELECT
            p.id,
            p.archived_at,
            p.millesime_pci,
            p.millesime_suivant,
            p.motif,
            p.nb_parcelles,
            p.note,
            COALESCE(p.millesime_pci, p.archived_at::date) AS debut,
            COALESCE(
                p.millesime_suivant,
                LEAD(COALESCE(p.millesime_pci, p.archived_at::date))
                    OVER (ORDER BY p.archived_at, p.id)
            ) AS fin
        FROM {s}.cadastre_photos p;

        CREATE OR REPLACE VIEW {s}.cadastre_filiation AS
        SELECT
            e.id AS evenement_id,
            e.run_id,
            r.run_at,
            r.millesime_pci,
            r.photo_id,
            e.type,
            parent.elem->>'idu' AS idu_parent,
            parent.elem->>'ref' AS ref_parent,
            enfant.elem->>'idu' AS idu_enfant,
            enfant.elem->>'ref' AS ref_enfant
        FROM {s}.cadastre_veille_evenements e
        JOIN {s}.cadastre_veille_runs r ON r.id = e.run_id
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.parents, '[]'::jsonb)) AS parent(elem)
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.enfants, '[]'::jsonb)) AS enfant(elem)
        WHERE e.type IN ('division', 'fusion', 'recodage', 'remaniement');

        CREATE OR REPLACE VIEW {s}.cadastre_filiation_liens AS
        SELECT
            e.id AS evenement_id,
            e.run_id,
            r.run_at,
            r.millesime_pci,
            e.type,
            lk.elem->>'idu_old' AS idu_parent,
            lk.elem->>'idu_new' AS idu_enfant,
            (lk.elem->>'area_inter')::double precision AS area_inter_m2,
            (lk.elem->>'area_old')::double precision AS area_parent_m2,
            (lk.elem->>'area_new')::double precision AS area_enfant_m2,
            (lk.elem->>'pct_old')::double precision AS pct_parent,
            (lk.elem->>'pct_new')::double precision AS pct_enfant
        FROM {s}.cadastre_veille_evenements e
        JOIN {s}.cadastre_veille_runs r ON r.id = e.run_id
        CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.liens, '[]'::jsonb)) AS lk(elem)
        WHERE lk.elem ? 'idu_old' AND lk.elem ? 'idu_new';
    """
