#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cua_builder_v5.py — Builder principal pour génération du CUA DOCX
Intègre :
- filtrage entités < 0,5 %
- attribution par article selon le catalogue enrichi
- gestion des cas spécifiques (via cas_speciaux.py)
- header avec QR code (via cua_header.py)
"""

import argparse, os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from docx.shared import Cm, Pt
from docx.enum.text import WD_ALIGN_PARAGRAPH

from cua_utils import (
    read_json, fmt_surface, fmt_pct, join_addr, parcels_label,
    build_footer_number, setup_doc, set_footer_num,
    add_first_article_title, add_article_title, add_paragraph, add_kv_table, add_objects_table,
    filter_intersections, equilibrer_pourcentages,
    add_annexes_section, ensure_page_space_for_article,
)

from cas_speciaux import appliquer_cas_speciaux
from cua_header import render_first_page_header, add_mayor_section_with_vu

# === PPRI (PM1) ===
from ppri_cua_module import analyser_ppri_corrige, generer_rapport_cua_avec_table


# ====================== BUILD CUA DOC ======================

def build_cua_docx(
    cerfa_json: Dict[str, Any],
    intersections_json: Dict[str, Any],
    catalogue_json: Dict[str, Any],
    output_docx: str,
    *,
    wkt_path: Optional[str] = None,  # ✅ Paramètre WKT
    logo_first_page: Optional[str] = None,
    signature_logo: Optional[str] = None,
    qr_url: str = "https://www.kerelia.com/carte",
    plu_nom="PLU en vigueur",
    plu_date_appro="13/02/2017",
) -> None:

    meta = cerfa_json.get("data") or {}
    commune = (meta.get("commune_nom") or "—").upper()
    parcelles = parcels_label(meta.get("references_cadastrales") or [])
    terrain = join_addr(meta.get("adresse_terrain") or {})
    surface_total = meta.get("superficie_totale_m2")
    footer_num = build_footer_number(meta)
    ncu = meta.get("numero_cu") or "—"

    inters = intersections_json or {}
    parcelle_surface = inters.get("surface_m2") or surface_total
    intersections_raw = inters.get("intersections") or {}

    # --- Filtrage par entité (>= 0,5 %) + Arrondi automatique ---
    # Note: filter_intersections() arrondit déjà les surfaces (m² entiers) et pourcentages (0.01%)
    intersections = filter_intersections(intersections_raw, parcelle_surface, min_pct=0.5)

    # --- Initialisation du regroupement par article ---
    layers_by_article: Dict[str, List[Dict[str, Any]]] = {}

    # --- Application des cas particuliers (ZNIEFF, etc.) ---
    appliquer_cas_speciaux(intersections, layers_by_article)

    # --- Regroupement des couches selon leur article dans le catalogue ---
    unknown_layers = []
    for key, layer in intersections.items():
        article = str(catalogue_json.get(key, {}).get("article") or "").strip()
        if article and article.isdigit():
            layers_by_article.setdefault(article, []).append(layer)
        elif article in {"7", "8", "9"}:  # si texte
            layers_by_article.setdefault(article, []).append(layer)
        else:
            unknown_layers.append(key)

    # --- Avertissement si des couches ne sont pas dans le catalogue ---
    if unknown_layers:
        print("\n⚠️  Les couches suivantes n'ont pas d'article défini dans le catalogue :")
        for k in unknown_layers:
            print(f"   - {k}")

    # --- Équilibrage des pourcentages dans chaque article ---
    for art, layers in layers_by_article.items():
        layers_by_article[art] = equilibrer_pourcentages(layers)

    # --- Initialisation des annexes (pour réglementations PLU) ---
    annexes = []

    # --- DOCX init ---
    doc = setup_doc()
    set_footer_num(doc, footer_num)

    # --- Header première page avec QR code ---
    render_first_page_header(
        doc,
        cerfa_json,
        logo_commune_path=logo_first_page,
        qr_url=qr_url,
        qr_logo_path=signature_logo
    )
    
    # --- Section "Le Maire" avec Vu et CERTIFIE ---
    add_mayor_section_with_vu(doc, cerfa_json, commune, plu_date_appro)

    # --- Article 1 (premier article sans espacement avant) ---
    add_first_article_title(doc, "Article UN - Objet")
    add_paragraph(doc,
        "Les règles d'urbanisme, la liste des taxes et participations d'urbanisme ainsi que "
        "les limitations administratives au droit de propriété applicables au terrain sont "
        "mentionnées aux articles 2 et suivants du présent certificat.\n\n"
        "Conformément au quatrième alinéa de l'article L. 410-1 du code de l'urbanisme, "
        "si une demande de permis de construire, d'aménager ou de démolir ou si une déclaration "
        "préalable est déposée dans le délai de dix-huit mois à compter de la date du présent "
        "certificat d'urbanisme, les dispositions d'urbanisme, le régime des taxes et participations "
        "d'urbanisme ainsi que les limitations administratives au droit de propriété tels qu'ils "
        "existaient à cette date ne peuvent être remis en cause à l'exception des dispositions qui "
        "ont pour objet la préservation de la sécurité ou de la salubrité publique."
    )

    # --- Article 2 ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article DEUX - Identification et localisation du terrain")
    add_kv_table(doc, [
        ("Commune", f"{meta.get('commune_nom') or '—'} ({meta.get('commune_insee') or '—'})"),
        ("Adresse / Localisation", terrain),
        ("Références cadastrales", parcelles),
        ("Surface indicative", (fmt_surface(surface_total) + " m²") if surface_total else "—"),
        ("Document d'urbanisme opposable", f"{plu_nom} — approuvé le {plu_date_appro}")
    ])

    # --- Article 3 : Zonage ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article TROIS - Dispositions d'urbanisme (Zonage)")
    add_paragraph(doc,
        "Les occupations et utilisations du sol, ainsi que les règles de constructibilité, "
        "sont définies par le règlement du PLU. Ci-dessous, les thématiques majeures sont "
        "rappelées de manière neutre avec renvoi aux articles sources (le texte du règlement fait foi)."
    )

    for ly in layers_by_article.get("3", []):
        add_paragraph(doc, ly.get("nom") or "Zonage", bold=True)
        add_paragraph(
            doc,
            f"Surface concernée : {fmt_surface(ly.get('surface_m2'))} m² ({fmt_pct(ly.get('pourcentage'))})"
        )

        objets = ly.get("objets") or []

        # Séparation entre données de table et textes réglementaires
        reglements = []
        objets_pour_table = []

        for obj in objets:
            # Si l'objet contient un texte de réglementation, on le garde pour la table
            # mais sans la clé 'reglementation', et on enregistre le texte à part
            if "reglementation" in obj and obj["reglementation"]:
                reglements.append(obj["reglementation"])
                obj_sans_regl = {k: v for k, v in obj.items() if k != "reglementation"}
                objets_pour_table.append(obj_sans_regl)
            else:
                objets_pour_table.append(obj)

        # --- Affichage de la table de zonage (informations générales)
        if objets_pour_table:
            add_objects_table(doc, objets_pour_table)

        # --- Ajout de la réglementation en annexe si présente
        if reglements:
            annexes.append({
                "titre": f"Règlement du PLU – {ly.get('nom')}",
                "contenu": "\n\n".join(reglements)
            })
            add_paragraph(doc, "→ Le texte complet du règlement du PLU est renvoyé en annexe.", italic=True)

    # Si aucune couche de zonage
    if not layers_by_article.get("3"):
        add_paragraph(doc, "Aucune donnée de zonage disponible.", italic=True)

    # --- Article 4 : Servitudes d'utilité publique ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article QUATRE - Servitudes d'utilité publique (SUP)")
    for ly in layers_by_article.get("4", []):
        add_paragraph(doc, ly.get("nom") or "Servitude", bold=True)
        add_paragraph(doc, f"Surface concernée : {fmt_surface(ly.get('surface_m2'))} m² ({fmt_pct(ly.get('pourcentage'))})")
        add_objects_table(doc, ly.get("objets") or [])
    if not layers_by_article.get("4"):
        add_paragraph(doc, "Aucune servitude détectée (après filtrage des entités < 0,5 %).", italic=True)

    # --- Intégration automatique du PPRI PM1 ---
    try:
        # Récupérer le code INSEE depuis le CERFA
        code_insee = meta.get("commune_insee") or "33234"
        
        print(f"🌊 Vérification du PPRI (PM1) pour l'unité foncière (INSEE: {code_insee})…")

        # ✅ Utilisation du WKT passé en paramètre
        if wkt_path and os.path.exists(wkt_path):
            with open(wkt_path, "r", encoding="utf-8") as f:
                geom_wkt = f.read().strip()
            resultats_ppri = analyser_ppri_corrige(geom_wkt=geom_wkt, code_insee=code_insee)
            print(f"✅ WKT chargé depuis : {wkt_path}")
        else:
            # Fallback cadastral (ne devrait jamais arriver)
            refs = meta.get("references_cadastrales", [])
            if refs:
                ref = refs[0]
                section = ref.get("section") or "AC"
                numero = ref.get("numero") or "0242"
            else:
                section, numero = "AC", "0242"
            print("⚠️ WKT non fourni, fallback cadastral.")
            resultats_ppri = analyser_ppri_corrige(section=section, numero=numero, code_insee=code_insee)

        # Si aucune zone PM1 n'intersecte la parcelle → on n'ajoute rien
        if not resultats_ppri or not resultats_ppri.get("zones_avec_regles"):
            print("ℹ️  Parcelle non concernée par le PPRI (aucune zone intersectée).")
        else:
            add_article_title(doc, "SUP PM1 – Risques et Inondations (PPRI)")
            generer_rapport_cua_avec_table(doc, resultats_ppri)
            print("✅ Rapport PPRI intégré dans la section PM1 avec tableau.")

    except Exception as e:
        print(f"⚠️ Erreur PPRI : {e}")
        # Aucune insertion dans le DOCX en cas d'erreur
        pass

    # Rappel général
    add_paragraph(doc, "Avertissement : seuls les actes de servitudes publiés (et leurs annexes cartographiques) font foi.", italic=True)

    # --- Article 5 : Risques et protections environnementales ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article CINQ – Risques et protections environnementales")
    for ly in layers_by_article.get("5", []):
        add_paragraph(doc, ly.get("nom") or "Information environnementale", bold=True)
        add_paragraph(doc, f"Surface concernée : {fmt_surface(ly.get('surface_m2'))} m² ({fmt_pct(ly.get('pourcentage'))})")
        add_objects_table(doc, ly.get("objets") or [])
    if not layers_by_article.get("5"):
        add_paragraph(doc, "Aucune donnée pertinente après filtrage.", italic=True)

    # --- Article 6 : Réseaux et équipements ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article SIX – Réseaux et équipements")
    for ly in layers_by_article.get("6", []):
        add_paragraph(doc, ly.get("nom") or "Réseau", bold=True)
        add_paragraph(doc, f"Surface concernée : {fmt_surface(ly.get('surface_m2'))} m² ({fmt_pct(ly.get('pourcentage'))})")
        add_objects_table(doc, ly.get("objets") or [])
    if not layers_by_article.get("6"):
        add_paragraph(doc, "Aucune donnée d'équipement disponible.", italic=True)

    # --- Article 7 : Informations utiles ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article SEPT – Informations utiles")
    for ly in layers_by_article.get("7", []):
        add_paragraph(doc, ly.get("nom") or "Information complémentaire", bold=True)
        add_objects_table(doc, ly.get("objets") or [])
    if not layers_by_article.get("7"):
        add_paragraph(doc, "Aucune information complémentaire détectée.", italic=True)

    # --- Article 8 : Taxes et participations ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article HUIT – Taxes et participations")
    add_paragraph(doc,
        "Les taxes suivantes pourront être exigées à compter de l'obtention d'un permis "
        "ou d'une décision de non opposition à une déclaration préalable."
    )
    add_kv_table(doc, [
        ("Taxe d'Aménagement", ""),
        ("Part communale - Taux en %", "5%"),
        ("Part départementale - Taux en %", "2,5 %"),
        ("Redevance d'Archéologie Préventive - Taux en %", "0,68 %")
    ])
    add_paragraph(doc, "Participations :", bold=True)
    add_paragraph(doc,
        "Les participations ci-dessous pourront être exigées à l'occasion d'un permis de construire "
        "ou d'une décision de non opposition à une déclaration préalable. Si tel est le cas elles "
        "seront mentionnées dans l'arrêté de permis ou dans un arrêté pris dans les deux mois "
        "suivant la date du permis tacite ou de la décision de non opposition à une déclaration préalable."
    )
    add_paragraph(doc, "Participations susceptibles d'être exigées à l'occasion de l'opération :")
    add_paragraph(doc, "- contribution aux dépenses de réalisation des équipements publics.")
    add_paragraph(doc, "- financement de branchements des équipements propres (article L332-15 du CU).")

    # --- Article 9 : Droit de préemption ---
    ensure_page_space_for_article(doc)
    add_article_title(doc, "Article NEUF – Droit de préemption")
    
    # Détection du type de préemption
    has_dpu = bool(layers_by_article.get("9"))
    
    if has_dpu:
        add_paragraph(doc,
            "Le bien est situé dans un périmètre de DPU (Droit de Préemption Urbain) délimité "
            "au PLU – annexe 6-1. Toute aliénation à titre onéreux est soumise à DIA "
            "(Déclaration d'Intention d'Aliéner (C. urb. L211-1 s.). La commune dispose d'un "
            "délai de 2 mois pour se prononcer, délai suspendu en cas de demande unique de "
            "pièces/visite (C. urb. L213-2). Silence = renonciation (C. urb. R213-4 s.). "
            "En cas de désaccord sur le prix, saisine du juge de l'expropriation (C. urb. L213-4)."
        )
        for ly in layers_by_article.get("9", []):
            add_paragraph(doc, ly.get("nom") or "Zone de préemption", bold=True)
            add_paragraph(doc, f"Surface concernée : {fmt_surface(ly.get('surface_m2'))} m² ({fmt_pct(ly.get('pourcentage'))})")
            add_objects_table(doc, ly.get("objets") or [])
    else:
        add_paragraph(doc,
            "Le terrain n'est pas situé dans une zone de droit de préemption. Aucune DIA "
            "(Déclaration d'Intention d'Aliéner) au titre du DPU (Droit de Préemption Urbain) "
            "n'est requise."
        )

    # --- Signature ---
    doc.add_page_break()
    add_paragraph(doc, f"Fait à {commune.title()}, le {datetime.now().strftime('%d/%m/%Y')}")
    add_paragraph(doc, "Le Maire,")
    if signature_logo and os.path.exists(signature_logo):
        try:
            doc.add_paragraph().add_run().add_picture(signature_logo, width=Cm(3))
        except Exception:
            pass
    
    # --- Informations finales (petit texte) ---
    doc.add_paragraph()
    p_info = doc.add_paragraph()
    r_info = p_info.add_run(
        "INFORMATIONS À LIRE ATTENTIVEMENT\n\n"
        "Le (ou les) demandeur(s) peut contester la légalité de la décision dans les deux mois qui suivent "
        "la date de sa notification. A cet effet il peut saisir le tribunal administratif territorialement "
        "compétent d'un recours contentieux.\n\n"
        "Durée de validité : Le certificat d'urbanisme a une durée de validité de 18 mois. Il peut être prorogé "
        "par périodes d'une année si les prescriptions d'urbanisme, les servitudes d'urbanisme de tous ordres "
        "et le régime des taxes et participations n'ont pas évolué. Vous pouvez présenter une demande de prorogation "
        "en adressant une demande sur papier libre, accompagnée du certificat pour lequel vous demandez la prorogation "
        "au moins deux mois avant l'expiration du délai de validité.\n\n"
        "A défaut de notification d'une décision expresse portant prorogation du certificat d'urbanisme dans le délai "
        "de deux mois suivant la réception en mairie de la demande, le silence gardé par l'autorité compétente vaut "
        "prorogation du certificat d'urbanisme. La prorogation prend effet au terme de la validité de la décision "
        "initiale (Art. R. 410-17-1)\n\n"
        "Effets du certificat d'urbanisme : le certificat d'urbanisme est un acte administratif d'information, qui "
        "constate le droit applicable en mentionnant les possibilités d'utilisation de votre terrain et les différentes "
        "contraintes qui peuvent l'affecter. Il n'a pas valeur d'autorisation pour la réalisation des travaux ou d'une "
        "opération projetée.\n\n"
        "Le certificat d'urbanisme crée aussi des droits à votre égard. Si vous déposez une demande d'autorisation "
        "(par exemple une demande de permis de construire) dans le délai de validité du certificat, les nouvelles "
        "dispositions d'urbanisme ou un nouveau régime de taxes ne pourront pas vous être opposées, sauf exceptions "
        "relatives à la préservation de la sécurité ou de la salubrité publique.\n\n"
        "QR Code : Le QR code permet d'accéder à une Carte interactive des règles applicables (zonage, SUP, risques, "
        "prescriptions, obligations, informations). Affichage informatif ; en cas de divergence, les pièces écrites "
        "et le règlement en vigueur font foi. Cette solution vous est proposée par KERELIA, société immatriculée "
        "944 763 275 au R.C.S. de Bordeaux"
    )
    r_info.font.size = Pt(8)

    # --- Ajout des annexes à la fin ---
    if annexes:
        add_annexes_section(doc, annexes)
        print(f"📎 {len(annexes)} annexes ajoutées en fin de CUA (règlements PLU).")

    Path(output_docx).parent.mkdir(parents=True, exist_ok=True)
    doc.save(output_docx)
    print(f"\n✅ CUA DOCX généré : {output_docx}")


# ====================== CLI ======================

def main():
    ap = argparse.ArgumentParser(description="CUA Builder v5 (avec header + QR code)")
    ap.add_argument("--cerfa-json", required=True)
    ap.add_argument("--intersections-json", required=True)
    ap.add_argument("--wkt-path", help="Chemin WKT unité foncière")
    ap.add_argument("--catalogue-json", required=True)
    ap.add_argument("--output", default="CUA_final.docx")
    ap.add_argument("--logo-first-page", default="")
    ap.add_argument("--signature-logo", default="")
    ap.add_argument("--qr-url", default="https://www.kerelia.com/carte")
    ap.add_argument("--plu-nom", default="PLU en vigueur")
    ap.add_argument("--plu-date-appro", default="13/02/2017")
    args = ap.parse_args()

    cerfa = read_json(args.cerfa_json)
    inters = read_json(args.intersections_json)
    catalogue = read_json(args.catalogue_json)

    build_cua_docx(
        cerfa, inters, catalogue, args.output,
        wkt_path=args.wkt_path,  # ✅ Passage du WKT
        logo_first_page=args.logo_first_page or None,
        signature_logo=args.signature_logo or None,
        qr_url=args.qr_url,
        plu_nom=args.plu_nom,
        plu_date_appro=args.plu_date_appro,
    )

if __name__ == "__main__":
    main()