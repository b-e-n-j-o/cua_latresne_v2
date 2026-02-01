#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orchestrateur CERFA complet (Mistral)
------------------------------------
Combine :
- infos générales CERFA
- références cadastrales
⚠️ Ne logge QUE les données métier (aucun coût / token)
"""

import json
from pathlib import Path

from CERFA_ANALYSE.mistral_cerfa_info_extractor import extraire_info_cerfa
from CERFA_ANALYSE.mistral_cerfa_parcelles_extractor import extraire_parcelles_cerfa


def analyser_cerfa_complet(pdf_path: str, output_path: str = "cerfa_complet.json"):
    """
    Analyse complète CERFA : infos + parcelles
    
    Returns:
        dict { success, data, metadata }
    """

    pdf_name = Path(pdf_path).name
    print(f"📄 Analyse CERFA : {pdf_name}\n")

    # ============================================================
    # 1️⃣ Infos générales
    # ============================================================
    print("1️⃣  Extraction des informations générales...")
    info_result = extraire_info_cerfa(pdf_path)

    if not info_result.get("success"):
        return {"success": False, "error": f"Infos générales : {info_result.get('error')}"}

    data_info = info_result["data"]

    # ============================================================
    # 2️⃣ Parcelles cadastrales
    # ============================================================
    print("\n2️⃣  Extraction des parcelles cadastrales...")
    parcelles_result = extraire_parcelles_cerfa(pdf_path)

    if not parcelles_result.get("success"):
        return {"success": False, "error": f"Parcelles : {parcelles_result.get('error')}"}

    data_parcelles = parcelles_result["data"]
    parcelles = data_parcelles.get("references_cadastrales", [])

    # ============================================================
    # 🧾 LOG MÉTIER – PARCELLES
    # ============================================================
    print("\n📍 Parcelles détectées :")
    for p in parcelles:
        sec = p.get("section")
        num = p.get("numero")
        surf = p.get("surface_m2")
        surf_str = f"{surf} m²" if surf else "surface inconnue"
        print(f"  • {sec} {num} ({surf_str})")

    # ============================================================
    # 3️⃣ Fusion des données
    # ============================================================
    print("\n3️⃣  Fusion des données CERFA...\n")

    cerfa_complet = {
        "cerfa_reference": data_info.get("cerfa_reference"),
        "commune_nom": data_info.get("commune_nom"),
        "commune_insee": data_info.get("commune_insee"),
        "departement_code": data_info.get("departement_code"),
        "numero_cu": data_info.get("numero_cu"),
        "type_cu": data_info.get("type_cu"),
        "date_depot": data_info.get("date_depot"),
        "demandeur": data_info.get("demandeur"),
        "adresse_terrain": data_info.get("adresse_terrain"),
        "references_cadastrales": parcelles,
        "superficie_totale_m2": data_parcelles.get("superficie_totale_m2"),
        "header_cu": data_info.get("header_cu"),
    }

    # ============================================================
    # 🧾 LOG MÉTIER – SYNTHÈSE
    # ============================================================
    print("🧩 Synthèse CERFA extraite :")
    print(f"Commune      : {cerfa_complet['commune_nom']} ({cerfa_complet['commune_insee']})")
    print(f"N° CU        : {cerfa_complet['numero_cu']}")
    print(f"Type         : {cerfa_complet['type_cu']}")
    print(f"Parcelles    : {len(parcelles)}")
    print(
        f"Superficie   : {cerfa_complet['superficie_totale_m2']:,} m²"
        .replace(",", " ")
        if cerfa_complet.get("superficie_totale_m2") else
        "Superficie   : inconnue"
    )

    # ============================================================
    # JSON FINAL (SANS COÛTS / TOKENS)
    # ============================================================
    result = {
        "success": True,
        "data": cerfa_complet,
        "metadata": {
            "source_file": pdf_name,
            "parcelles_stats": parcelles_result.get("stats")
        }
    }

    # Sauvegarde
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Résultat sauvegardé : {output_path}")
    print("✅ Analyse CERFA terminée\n")

    return result


# ============================================================
# CLI de test local
# ============================================================
if __name__ == "__main__":
    pdf = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    res = analyser_cerfa_complet(pdf, "cerfa_complet.json")

    if not res.get("success"):
        print(f"❌ Erreur : {res.get('error')}")
