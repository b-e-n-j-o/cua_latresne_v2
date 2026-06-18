# -*- coding: utf-8 -*-
"""
uf.py — Construction et vérification de l'unité foncière.

Entrée : une ou plusieurs références parcellaires {section, numero}.
Sortie : un objet UniteFonciere (WKT unioné + surfaces).

La vérification "est-ce une UF ?" se fait ici : si plusieurs parcelles sont
fournies, on exige qu'elles soient contiguës (limite commune, pas un simple
point de contact). Un ST_Union de parcelles partageant une arête fusionne en
UN polygone ; sinon le résultat reste un MultiPolygon à N morceaux.
=> contiguë  ⇔  ST_NumGeometries(ST_Multi(ST_Union(...))) == 1
"""

from dataclasses import dataclass, field

from sqlalchemy import text

try:
    from api.cuas.argeles.db import GEOM_COL, SCHEMA, SRID, get_engine, logger
except ImportError:
    from db import GEOM_COL, SCHEMA, SRID, get_engine, logger


@dataclass
class UniteFonciere:
    wkt: str
    surface_sig: float          # ST_Area (mesure SIG, sert au calcul des %)
    surface_cadastrale: float   # somme des contenances (surface indicative juridique)
    n_parcelles: int
    parcelles: list = field(default_factory=list)  # [(section, numero), ...]


def _normalize_section(value: str) -> str:
    return (value or "").strip().upper()


def _normalize_numero(value: str) -> str:
    """Aligné sur parcelles/resume : comparaison via lpad(..., 4, '0')."""
    raw = (value or "").strip()
    if not raw:
        return ""
    return raw.zfill(4)


def build_uf(refs, engine=None, schema: str = SCHEMA) -> UniteFonciere:
    """
    refs : iterable de dicts {'section': 'AB', 'numero': '0123'}.

    Lève ValueError si :
      - une référence n'existe pas dans <schema>.parcelles
      - plusieurs parcelles sont fournies mais ne forment pas une UF contiguë.
    """
    engine = engine or get_engine()

    input_pairs = [(str(r["section"]).strip(), str(r["numero"]).strip()) for r in refs]
    if not input_pairs:
        raise ValueError("Aucune référence parcellaire fournie.")

    # Même logique que GET /parcelles/resume : section insensible à la casse,
    # numéro comparé en lpad(4) pour CE:298 ↔ CE:0298.
    clauses, params = [], {}
    for i, (s, n) in enumerate(input_pairs):
        clauses.append(
            f"(upper(trim(section)) = :s{i} AND lpad(trim(numero), 4, '0') = :n{i})"
        )
        params[f"s{i}"] = _normalize_section(s)
        params[f"n{i}"] = _normalize_numero(n)
    where = " OR ".join(clauses)

    sql = text(f"""
        WITH sel AS (
            SELECT section,
                   numero,
                   ST_MakeValid({GEOM_COL}) AS g,
                   contenance
            FROM {schema}.parcelles
            WHERE {where}
        )
        SELECT count(*)                                        AS n,
               ST_AsText(ST_Union(g))                          AS wkt,
               ST_Area(ST_Union(g))                            AS surface_sig,
               SUM(contenance)                                 AS surface_cad,
               ST_NumGeometries(ST_Multi(ST_Union(g)))         AS n_parts,
               jsonb_agg(jsonb_build_object('section', section,
                                            'numero',  numero)) AS found
        FROM sel
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, params).mappings().one()

    # --- Parcelles manquantes ---
    found_rows = row["found"] or []
    if row["n"] != len(input_pairs):
        found_norm = {
            (_normalize_section(f["section"]), _normalize_numero(f["numero"]))
            for f in found_rows
        }
        missing = [
            f"{s} {n}"
            for (s, n) in input_pairs
            if (_normalize_section(s), _normalize_numero(n)) not in found_norm
        ]
        raise ValueError(
            f"Parcelle(s) introuvable(s) dans {schema}.parcelles : {', '.join(missing)}"
        )

    # --- Contiguïté (uniquement si UF multi-parcelles) ---
    if len(input_pairs) > 1 and (row["n_parts"] or 1) > 1:
        raise ValueError(
            f"Les {len(input_pairs)} parcelles ne forment pas une unité foncière contiguë "
            f"({row['n_parts']} blocs disjoints). Vérifie les références."
        )

    db_parcelles = [(f["section"], f["numero"]) for f in found_rows]

    surface_sig = float(row["surface_sig"])
    surface_cad = float(row["surface_cad"]) if row["surface_cad"] is not None else surface_sig

    uf = UniteFonciere(
        wkt=row["wkt"],
        surface_sig=round(surface_sig, 2),
        surface_cadastrale=round(surface_cad, 2),
        n_parcelles=int(row["n"]),
        parcelles=db_parcelles,
    )
    logger.info(
        f"📐 UF construite : {uf.n_parcelles} parcelle(s) | "
        f"SIG {uf.surface_sig} m² | contenance {uf.surface_cadastrale} m²"
    )
    return uf
