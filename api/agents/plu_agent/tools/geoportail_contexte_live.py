"""Tool get_geoportail_contexte_live — fallback France entière via GPU live."""

from __future__ import annotations

from google.genai import types

from .get_geoportail_contexte.get_geoportail_contexte import get_geoportail_contexte


def get_geoportail_contexte_live(
    db_config: dict,
    parcelles: list[dict] | None = None,
    idus: list[str] | None = None,
    section: str | None = None,
    numero: str | None = None,
    idu: str | None = None,
    insee: str | None = None,
    buffer_m: float = 0.0,
) -> dict:
    # db_config conservé pour compatibilité signature avec le dispatcher tools.
    return get_geoportail_contexte(
        db_config=db_config,
        parcelles=parcelles,
        idus=idus,
        section=section,
        numero=numero,
        idu=idu,
        insee=insee,
        buffer_m=buffer_m,
    )


DECL_GEOPORTAIL_CONTEXTE_LIVE = types.FunctionDeclaration(
    name="get_geoportail_contexte_live",
    description=(
        "Retourne le contexte d'urbanisme d'une parcelle ou unité foncière en France entière, "
        "depuis les couches live du Géoportail de l'Urbanisme (zonage, prescriptions, servitudes, informations). "
        "À utiliser en fallback pour les communes hors bases locales pré-intégrées."
    ),
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "parcelles": types.Schema(
                type=types.Type.ARRAY,
                description=(
                    "Liste de parcelles cadastrales (section + numéro). "
                    "Pour une unité foncière : toutes les parcelles contiguës du même ensemble."
                ),
                items=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "section": types.Schema(type=types.Type.STRING),
                        "numero": types.Schema(type=types.Type.STRING),
                    },
                    required=["section", "numero"],
                ),
            ),
            "idus": types.Schema(
                type=types.Type.ARRAY,
                description="Liste d'IDU cadastraux.",
                items=types.Schema(type=types.Type.STRING),
            ),
            "section": types.Schema(type=types.Type.STRING),
            "numero": types.Schema(type=types.Type.STRING),
            "idu": types.Schema(type=types.Type.STRING),
            "insee": types.Schema(
                type=types.Type.STRING,
                description="Code INSEE 5 chiffres, recommandé avec section+numero.",
            ),
            "buffer_m": types.Schema(
                type=types.Type.NUMBER,
                description="Conservé pour compatibilité (ignoré, intersection stricte).",
            ),
        },
    ),
)

