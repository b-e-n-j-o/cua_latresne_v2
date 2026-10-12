"""Tool Gemini : autorisations Sitadel d'une parcelle actuelle."""

from __future__ import annotations

from google.genai import types

from api.autorisations.service import compact_parcelle_rows
from ..commune_context import q
from .utils.db import db_query

SQL = """
select a.id, a.categorie, a.type_dau, a.etat, a.date_depot, a.date_autorisation,
       a.nature_projet, a.nb_logements, a.surface_creee, a.geoloc_statut,
       ap.methode, ap.idu_origine, ap.millesime_origine
from {table_ap} ap
join {table_a} a on a.id = ap.autorisation_id
where ap.idu = %s
order by coalesce(a.date_autorisation, a.date_depot) desc nulls last
"""


def autorisations_parcelle(
    db_config: dict,
    idu: str,
    depuis: int | None = None,
) -> dict:
    idu_n = (idu or "").strip().upper()
    if not idu_n:
        return {"error": "idu manquant", "autorisations": [], "count": 0}
    try:
        sql = SQL.format(table_ap=q("autorisation_parcelle"), table_a=q("autorisation"))
        rows = db_query(db_config, sql, (idu_n,))
        if depuis is not None:
            kept = []
            for row in rows:
                d = row.get("date_autorisation") or row.get("date_depot")
                year = getattr(d, "year", None)
                if year is None and isinstance(d, str) and len(d) >= 4:
                    try:
                        year = int(d[:4])
                    except ValueError:
                        year = None
                if year is None or year >= int(depuis):
                    kept.append(row)
            rows = kept
        compact = compact_parcelle_rows(rows)
        return {"autorisations": compact, "count": len(compact), "error": None}
    except Exception as exc:
        return {"error": str(exc), "autorisations": [], "count": 0}


DECL_AUTORISATIONS_PARCELLE = types.FunctionDeclaration(
    name="autorisations_parcelle",
    description=(
        "Liste les autorisations d'urbanisme (Sitadel) liées à une parcelle cadastrale actuelle (IDU). "
        "JSON compact : id, type, date, état, nature, nombre de logements, méthode. "
        "methode = actuel : permis déposé sur cette parcelle. "
        "methode = millesime : permis déposé sur la parcelle mère avant division — le formuler ainsi. "
        "methode = millesime_operation : opération d'aménagement d'ensemble."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "idu": types.Schema(
                type=types.Type.STRING,
                description="IDU 14 caractères de la parcelle actuelle",
            ),
            "depuis": types.Schema(
                type=types.Type.INTEGER,
                description="Année minimale optionnelle (filtre sur la date d'autorisation ou de dépôt)",
            ),
        },
        required=["idu"],
    ),
)
