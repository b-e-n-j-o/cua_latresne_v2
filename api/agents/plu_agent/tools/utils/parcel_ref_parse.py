"""
Détection déterministe des références cadastrales dans un texte libre.

Format officiel (DGFiP) : section 1–2 lettres + numéro 4 chiffres (SS NNNN).
Saisie courante : AL 74, AL74, A 7, A7, AN 0076, section AL n°74, IDU 14 car.

Le parseur privilégie la précision (moins de faux positifs) :
  - ne transforme pas « à » en section A ;
  - ignore « de 12 m », « en 2024 », « il y a 12 », « article L421-6 », « zone A ».
"""

from __future__ import annotations

import re
from typing import Any

# IDU parcelle : INSEE(5) + préfixe(3) + section paddée(2) + numéro(4) = 14.
_IDU_RE = re.compile(r"(?<![0-9A-Za-z])([0-9A-Za-z]{10}\d{4})(?![0-9A-Za-z])")

_CADASTRAL_CUE_RE = re.compile(
    r"\b(?:parcelles?|cadastr\w*|feuilles?|idus?|sections?|"
    r"unit[eé]s?\s+fonci[eè]res?)\b",
    re.IGNORECASE,
)

# Section + numéro : lettres ASCII seulement (pas à/é).
# Séparateur : rien, espace, n°, numéro, - / .
_REF_RE = re.compile(
    r"(?<![0-9A-Za-z])"
    r"([A-Za-z]{1,2})"
    r"(?:[\s._/-]*n[°o]\s*|[\s._/-]*num[ée]ro\s+|[\s._/-]*)"
    r"(\d{1,4})"
    r"(?!\d)",
    re.IGNORECASE,
)

# « AL 74 et 75 », « AL 74, 76 », « AL 418 417 » (sans virgule).
_FOLLOW_RE = re.compile(
    r"(?:"
    r"\s*(?:,|;|/|et|ou)\s+"
    r"|"
    r"\s+"
    r")"
    r"(?:([A-Za-z]{1,2})(?:[\s._/-]*n[°o]\s*|[\s._/-]*num[ée]ro\s+|[\s._/-]*))?"
    r"(\d{1,4})(?!\d)",
    re.IGNORECASE,
)

_UNIT_AFTER_RE = re.compile(
    r"\s*(?:"
    r"%|€|euros?|"
    r"m(?:²|2)?(?:\b|ètres?\b|etres?\b)|"
    r"cm\b|mm\b|ml\b|ha\b|"
    r"ans\b|mètres?\b|metres?\b"
    r")",
    re.IGNORECASE,
)

_ZONE_BEFORE_RE = re.compile(
    r"\b(?:zones?|secteurs?|types?|classements?|classées?)\s+$",
    re.IGNORECASE,
)

_ARTICLE_BEFORE_RE = re.compile(
    r"\b(?:articles?|art\.?)\s+$",
    re.IGNORECASE,
)

_YA_BEFORE_RE = re.compile(r"\by\s+$", re.IGNORECASE)

# Mots FR 2 lettres quasi jamais une section, sauf contexte cadastral.
_FUNCTION_SECTIONS = frozenset({
    "DE", "DU", "LE", "LA", "ET", "OU", "EN", "AU", "CE", "IL", "ON",
    "SE", "SA", "SI", "NE", "NI", "OR", "UN", "ME", "TE", "TU", "CA",
    "VA", "VS", "YE", "TA", "TO",
})

# Conjonctions : « AL 416 et 418 » ≠ section ET.
_CONJUNCTION_SECTIONS = frozenset({"ET", "OU"})

# Vraies sections possibles mais aussi mots courants (« an 2024 »).
_AMBIGUOUS_SECTIONS = frozenset({"AN", "AI", "AS"})

_IMMEDIATE_SECTION_CUE_RE = re.compile(
    r"\b(?:parcelles?|feuilles?|sections?|idus?)\s+$",
    re.IGNORECASE,
)

_ARTICLE_LETTERS = frozenset({"L", "R", "D"})

_YEAR_RE = re.compile(r"^(?:19|20)\d{2}$")

_EXPLICIT_SECTION_RE = re.compile(
    r"\bsection\s+([A-Za-z]{1,2})\s+(?:n[°o]\s*|num[ée]ro\s+)?(\d{1,4})\b",
    re.IGNORECASE,
)


def pad_section(section: str) -> str:
    """Section officielle 2 caractères (espace à gauche si une lettre)."""
    return (section or "").upper().strip()[:2].rjust(2, " ")


def pad_numero(numero: str | int) -> str:
    return str(numero).strip().zfill(4)


def official_label(section: str, numero: str | int) -> str:
    """Libellé SS NNNN (ex. « AL 0074 », « A 0007 »)."""
    sec = (section or "").upper().strip()
    return f"{sec} {pad_numero(numero)}"


def _has_cadastral_cue(text: str) -> bool:
    return bool(_CADASTRAL_CUE_RE.search(text or ""))


def _numero_was_padded(raw_numero: str) -> bool:
    return len(raw_numero) >= 2 and raw_numero.startswith("0")


def _reject_neighborhood(text: str, start: int, end: int) -> bool:
    before = text[:start]
    after = text[end:]
    if _UNIT_AFTER_RE.match(after):
        return True
    if after[:2] and re.match(r"-\d", after):
        return True
    if _ZONE_BEFORE_RE.search(before):
        return True
    if _ARTICLE_BEFORE_RE.search(before):
        return True
    return False


def _accept_candidate(
    *,
    section: str,
    raw_section: str,
    raw_numero: str,
    glued: bool,
    context: bool,
    before: str,
) -> bool:
    if _YEAR_RE.match(raw_numero) and section in _FUNCTION_SECTIONS | _AMBIGUOUS_SECTIONS:
        return False

    if section in _ARTICLE_LETTERS and len(raw_numero) >= 3 and not context:
        return False

    if section == "A" and _YA_BEFORE_RE.search(before):
        return False

    if section in _CONJUNCTION_SECTIONS:
        return bool(_IMMEDIATE_SECTION_CUE_RE.search(before))

    if section in _FUNCTION_SECTIONS:
        return context

    if section in _AMBIGUOUS_SECTIONS:
        # AM 1106 : 3–4 chiffres ≠ « an 2024 » (déjà filtré).
        return context or _numero_was_padded(raw_numero) or glued or len(raw_numero) >= 3

    if len(section) == 1 and raw_section == "a" and not glued and not context:
        return False

    return True


def extract_parcel_pairs(text: str) -> list[dict[str, str]]:
    """Liste ordonnée dédupliquée [{section, numero}] (numéro non paddé)."""
    if not text or not str(text).strip():
        return []

    src = str(text)
    context = _has_cadastral_cue(src)
    pairs: list[dict[str, str]] = []
    seen: set[str] = set()

    def _add(section: str, numero: str) -> None:
        sec = section.upper().strip()
        num = str(numero).strip()
        if not sec or not num or not num.isdigit():
            return
        if int(num) <= 0:
            return
        key = f"{sec}:{pad_numero(num)}"
        if key in seen:
            return
        seen.add(key)
        pairs.append({"section": sec, "numero": num})

    for m in _REF_RE.finditer(src):
        raw_section = m.group(1)
        raw_numero = m.group(2)
        start, end = m.start(), m.end()
        if _reject_neighborhood(src, start, end):
            continue
        between = src[m.start(1) + len(raw_section) : m.start(2)]
        glued = between == ""
        section = raw_section.upper()
        if not _accept_candidate(
            section=section,
            raw_section=raw_section,
            raw_numero=raw_numero,
            glued=glued,
            context=context,
            before=src[:start],
        ):
            continue
        _add(section, raw_numero)

        # « AL 74 et 75 » / « AL 74, AL 76 »
        cursor = end
        last_section = section
        while True:
            fm = _FOLLOW_RE.match(src, cursor)
            if not fm:
                break
            follow_sec = (fm.group(1) or last_section).upper()
            follow_num = fm.group(2)
            f_end = fm.end()
            if _reject_neighborhood(src, fm.start(), f_end):
                break
            if follow_sec in _FUNCTION_SECTIONS and not context:
                break
            _add(follow_sec, follow_num)
            last_section = follow_sec
            cursor = f_end

    if not pairs:
        for m in _EXPLICIT_SECTION_RE.finditer(src):
            _add(m.group(1), m.group(2))

    return pairs


def extract_idus(text: str) -> list[str]:
    if not text:
        return []
    found: list[str] = []
    seen: set[str] = set()
    for m in _IDU_RE.finditer(str(text)):
        idu = m.group(1).upper()
        if idu in seen:
            continue
        seen.add(idu)
        found.append(idu)
    return found


def parse_parcel_refs_from_text(text: str) -> dict[str, Any]:
    """
    Même contrat que l'ancien ``refs_from_user_text`` :
    {section, numero} | {parcelles} | {idu} | {idus} | {}.
    """
    idus = extract_idus(text)
    if len(idus) > 1:
        return {"idus": idus}
    if len(idus) == 1:
        return {"idu": idus[0]}

    pairs = extract_parcel_pairs(text)
    if len(pairs) >= 2:
        return {"parcelles": pairs}
    if len(pairs) == 1:
        return {"section": pairs[0]["section"], "numero": pairs[0]["numero"]}
    return {}


def format_parcel_identity_prompt(identity: dict | None) -> str:
    """Bloc système : parcelle déjà résolue (sans règlement, sans géométrie)."""
    if not identity:
        return ""
    commune = identity.get("commune") or ""
    insee = identity.get("insee") or ""
    where = f" dans {commune}" if commune else ""
    if insee:
        where += f" (INSEE {insee})"

    if identity.get("error") and not identity.get("parcelles"):
        missing = identity.get("missing_labels") or []
        miss_line = f" Introuvables{where} : {', '.join(missing)}." if missing else ""
        return (
            "\n\n## Parcelle\n"
            "Références cadastrales détectées mais non résolues"
            f"{where} : {identity['error']}.{miss_line}\n"
            "Vérifie la commune du chat et les numéros (format officiel SS NNNN, "
            "ex. AL 0074). Ne pas inventer de parcelle.\n"
        )

    rows = identity.get("parcelles") or []
    if not rows and identity.get("parcelle"):
        rows = [identity["parcelle"]]
    if not rows:
        return ""

    lines = [
        "\n\n## Parcelle(s) déjà identifiée(s) par le serveur",
        "Utilise EXACTEMENT ces références pour get_contexte_parcelle "
        "(section + numero, ou parcelles[] si plusieurs).",
        "Ne cherche pas à « trouver » la parcelle : elle est déjà résolue.",
        "",
    ]
    for p in rows:
        section = p.get("section") or "?"
        numero = p.get("numero") or "?"
        idu = p.get("idu") or "—"
        contenance = p.get("contenance")
        superficie = p.get("superficie_m2")
        extra = []
        if contenance is not None:
            extra.append(f"contenance {contenance} m²")
        if superficie is not None:
            try:
                extra.append(f"superficie {round(float(superficie), 1)} m²")
            except (TypeError, ValueError):
                extra.append(f"superficie {superficie} m²")
        extra_s = f" — {', '.join(extra)}" if extra else ""
        lines.append(f"- {official_label(section, numero)} (idu {idu}){extra_s}")

    unite = identity.get("unite_fonciere") or {}
    nb = unite.get("nb_parcelles") or identity.get("nb_parcelles") or len(rows)
    if nb and int(nb) > 1:
        surf = unite.get("superficie_m2")
        surf_s = ""
        if surf is not None:
            try:
                surf_s = f", union {round(float(surf), 1)} m²"
            except (TypeError, ValueError):
                surf_s = f", union {surf} m²"
        lines.append(f"- unité foncière : {nb} parcelles{surf_s}")
    missing_labels = identity.get("missing_labels") or []
    if missing_labels:
        lines.append(
            f"- introuvables{where} : {', '.join(missing_labels)} "
            "(ne pas les analyser, ne pas les inventer)"
        )
    lines.append("")
    return "\n".join(lines)


__all__ = [
    "extract_idus",
    "extract_parcel_pairs",
    "format_parcel_identity_prompt",
    "official_label",
    "pad_numero",
    "pad_section",
    "parse_parcel_refs_from_text",
]
