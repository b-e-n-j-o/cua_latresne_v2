# -*- coding: utf-8 -*-
"""
Résolution parcelle(s) → adresse(s) via les tables BAN-PLUS locales.

Chaîne métier (identique au flux WFS parcelles_to_adresse, mais en base) :
    1. (section, numero) → IDU cadastral 14 car.
    2. argeles.lien_adresses_parcelles (idu) → id_adr
    3. argeles.adresses (id_adr) → libellé formaté

Alimente le tableau d'identité en tête du CUA (builder.section_identite).
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

try:
    from api.cuas.argeles.db import SCHEMA
    from api.cuas.argeles.intersection_modules.parcelles_geom import (
        format_parcelle_ref,
        normalize_numero,
        normalize_section,
    )
except ImportError:
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from db import SCHEMA
    from intersection_modules.parcelles_geom import (
        format_parcelle_ref,
        normalize_numero,
        normalize_section,
    )

DEFAULT_INSEE = "66008"
DEFAULT_PREFIXE = "000"


def build_idu(
    section: str,
    numero: str | int,
    code_insee: str = DEFAULT_INSEE,
    prefixe: str = DEFAULT_PREFIXE,
) -> str:
    """INSEE(5) + préfixe(3) + section(2) + numéro(4)."""
    code_insee = str(code_insee).strip().zfill(5)
    prefixe = str(prefixe).strip().zfill(3)
    section = str(section).strip().upper().rjust(2, "0")
    numero = str(numero).strip().rjust(4, "0")
    idu = f"{code_insee}{prefixe}{section}{numero}"
    if len(idu) != 14:
        raise ValueError(f"IDU invalide ({len(idu)} caractères) : {idu!r}")
    return idu


def format_adresse(
    numero,
    rep,
    nom_voie: str | None,
    nom_com: str | None,
) -> str:
    """'621 Chemin de la Massane, Argelès-sur-Mer' à partir des attributs BAN."""
    rep = (rep or "").strip()
    voie = (nom_voie or "").strip()
    com = (nom_com or "").strip()

    num_part = ""
    if numero not in (None, "", 0):
        num_part = f"{numero} {rep}".strip() if rep else str(numero)

    gauche = f"{num_part} {voie}".strip()
    return f"{gauche}, {com}".strip(", ").strip()


def _tables_available(engine, schema: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT COUNT(*) = 2
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name IN ('adresses', 'lien_adresses_parcelles')
            """),
            {"schema": schema},
        ).scalar()
    return bool(row)


def _fetch_adresses_for_idu(
    engine,
    schema: str,
    idu: str,
) -> list[str]:
    sql = text(f"""
        SELECT DISTINCT
            a.numero,
            a.rep,
            a.nom_voie,
            a.nom_com
        FROM {schema}.lien_adresses_parcelles l
        JOIN {schema}.adresses a ON a.id_adr = l.id_adr
        WHERE l.idu = :idu
        ORDER BY a.nom_voie, a.numero
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"idu": idu}).mappings().all()

    adresses: list[str] = []
    seen: set[str] = set()
    for row in rows:
        adr = format_adresse(
            row.get("numero"),
            row.get("rep"),
            row.get("nom_voie"),
            row.get("nom_com"),
        )
        if adr and adr not in seen:
            seen.add(adr)
            adresses.append(adr)
    return adresses


def _format_texte_header(parcelles: list[dict[str, Any]]) -> str | None:
    if not parcelles:
        return None

    if len(parcelles) == 1:
        adresses = parcelles[0].get("adresses") or []
        return " ; ".join(adresses) if adresses else None

    parts: list[str] = []
    for parcelle in parcelles:
        adresses = parcelle.get("adresses") or []
        if not adresses:
            continue
        ref = format_parcelle_ref(parcelle["section"], parcelle["numero"])
        parts.append(f"{ref} : {' ; '.join(adresses)}")
    return " | ".join(parts) if parts else None


def compute_adresses_parcelles(
    *,
    parcelles: list[dict] | None = None,
    engine,
    schema: str = SCHEMA,
    code_insee: str = DEFAULT_INSEE,
    prefixe: str = DEFAULT_PREFIXE,
) -> dict[str, Any]:
    """
    Résout les adresses BAN liées à chaque parcelle de l'UF.

    Retourne un bloc prêt pour le rapport d'intersections et le header CUA.
    """
    refs = list(parcelles or [])
    if not refs:
        return {
            "status": "non_concernee",
            "diagnostic_metier": "Aucune parcelle dans l'UF",
            "parcelles": [],
            "adresses_uniques": [],
            "texte_header": None,
        }

    if not _tables_available(engine, schema):
        return {
            "status": "table_absente",
            "diagnostic_metier": "Tables BAN (adresses / lien_adresses_parcelles) absentes",
            "parcelles": [],
            "adresses_uniques": [],
            "texte_header": None,
        }

    result_parcelles: list[dict[str, Any]] = []
    adresses_uniques: list[str] = []
    seen_adresses: set[str] = set()
    n_avec_adresse = 0

    for ref in refs:
        section = normalize_section(ref.get("section", ""))
        numero = normalize_numero(ref.get("numero", ""))
        idu = build_idu(section, numero, code_insee, prefixe)
        adresses = _fetch_adresses_for_idu(engine, schema, idu)
        if adresses:
            n_avec_adresse += 1
        for adr in adresses:
            if adr not in seen_adresses:
                seen_adresses.add(adr)
                adresses_uniques.append(adr)
        result_parcelles.append(
            {
                "section": section,
                "numero": numero,
                "idu": idu,
                "adresses": adresses,
            }
        )

    texte_header = _format_texte_header(result_parcelles)
    if n_avec_adresse:
        diagnostic = (
            f"{n_avec_adresse}/{len(refs)} parcelle(s) avec adresse(s) "
            f"({len(adresses_uniques)} adresse(s) distincte(s))"
        )
        status = "concernee"
    else:
        diagnostic = f"Aucune adresse BAN liée aux {len(refs)} parcelle(s)"
        status = "non_concernee"

    return {
        "status": status,
        "diagnostic_metier": diagnostic,
        "parcelles": result_parcelles,
        "adresses_uniques": adresses_uniques,
        "texte_header": texte_header,
    }
