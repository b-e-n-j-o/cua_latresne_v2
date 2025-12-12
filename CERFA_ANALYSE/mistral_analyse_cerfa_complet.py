"""
Orchestrateur CERFA complet : combine info générale + parcelles
"""

import json
from pathlib import Path
from mistral_cerfa_info_extractor import extraire_info_cerfa
from mistral_cerfa_parcelles_extractor import extraire_parcelles_cerfa


def analyser_cerfa_complet(pdf_path: str, output_path: str = "cerfa_complet.json"):
    """
    Analyse complète CERFA : infos + parcelles
    
    Args:
        pdf_path: Chemin PDF CERFA
        output_path: Fichier JSON sortie
        
    Returns:
        JSON complet combiné
    """
    
    print(f"📄 Analyse CERFA : {Path(pdf_path).name}\n")
    
    # 1. Infos générales (pages 1-4)
    print("1️⃣  Extraction infos générales...")
    info_result = extraire_info_cerfa(pdf_path)
    
    if not info_result["success"]:
        return {"success": False, "error": f"Info: {info_result['error']}"}
    
    # 2. Parcelles (pages 2+4)
    print("\n2️⃣  Extraction parcelles cadastrales...")
    parcelles_result = extraire_parcelles_cerfa(pdf_path)
    
    if not parcelles_result["success"]:
        return {"success": False, "error": f"Parcelles: {parcelles_result['error']}"}
    
    # 3. Fusion
    print("\n3️⃣  Fusion des données...\n")
    
    data_info = info_result["data"]
    data_parcelles = parcelles_result["data"]
    
    # Combine
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
        "references_cadastrales": data_parcelles.get("references_cadastrales"),
        "superficie_totale_m2": data_parcelles.get("superficie_totale_m2"),
        "header_cu": data_info.get("header_cu")
    }
    
    # Métadonnées
    result = {
        "success": True,
        "data": cerfa_complet,
        "metadata": {
            "source_file": Path(pdf_path).name,
            "usage": {
                "info_tokens": info_result.get("usage", {}).get("total_tokens", 0),
                "parcelles_tokens": parcelles_result.get("stats", {}).get("tokens", 0),
                "total_tokens": (
                    info_result.get("usage", {}).get("total_tokens", 0) +
                    parcelles_result.get("stats", {}).get("tokens", 0)
                ),
                "total_cost_usd": (
                    info_result.get("usage", {}).get("cost_total_usd", 0) +
                    parcelles_result.get("stats", {}).get("cost_usd", 0)
                )
            },
            "parcelles_stats": parcelles_result.get("stats")
        }
    }
    
    # Sauvegarde
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    # Résumé
    print("✅ Analyse complète\n")
    print(f"Commune      : {cerfa_complet['commune_nom']} ({cerfa_complet['commune_insee']})")
    print(f"N° CU        : {cerfa_complet['numero_cu']}")
    print(f"Parcelles    : {len(cerfa_complet['references_cadastrales'])}")
    print(f"Superficie   : {cerfa_complet['superficie_totale_m2']:,} m²".replace(",", " "))
    print(f"\nTokens total : {result['metadata']['usage']['total_tokens']:,}".replace(",", " "))
    print(f"Coût total   : ${result['metadata']['usage']['total_cost_usd']:.6f}")
    print(f"\n💾 {output_path}")
    
    return result


if __name__ == "__main__":
    pdf = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf"
    
    result = analyser_cerfa_complet(pdf, "cerfa_complet.json")
    
    if not result["success"]:
        print(f"❌ {result['error']}")