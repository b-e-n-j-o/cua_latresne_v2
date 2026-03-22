import os
import json
import logging
import time
from pathlib import Path
from INTERSECTIONS.pipeline_from_parcelles import run_pipeline_from_parcelles

# 1. Configuration du Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler("batch_process.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("CUA_Batch")

def run_batch_latresne():
    # --- CONFIGURATION DES CHEMINS ---
    input_json = "liste_parcelles_latresne.json"
    base_out_dir = Path("./OUTPUT_CUA_INDIVIDUELS")
    code_insee_latresne = "33234"
    
    # Vérification du fichier d'entrée
    if not os.path.exists(input_json):
        logger.error(f"❌ Fichier {input_json} introuvable.")
        return

    with open(input_json, "r", encoding="utf-8") as f:
        parcelles = json.load(f)

    total = len(parcelles)
    logger.info(f"🚀 Lancement de la génération pour {total} parcelles.")

    def is_parcelle_already_processed(out_dir: Path) -> bool:
        """
        Vérifie si une parcelle a déjà été traitée en vérifiant la présence
        du fichier CUA final : CUA_unite_fonciere.docx
        """
        if not out_dir.exists():
            return False
        
        # Vérifier la présence du fichier CUA final (indique que le pipeline est complètement terminé)
        cua_file = out_dir / "CUA_unite_fonciere.docx"
        return cua_file.exists()

    # 2. BOUCLE DE TRAITEMENT SÉQUENTIELLE
    skipped_count = 0
    processed_count = 0
    
    for index, p in enumerate(parcelles, 1):
        # --- NETTOYAGE ET FORMATAGE DES DONNÉES ---
        # Section en majuscules (ex: ac -> AC)
        section = str(p['section']).upper().strip()
        
        # Numéro forcé sur 4 chiffres avec des zéros devant (ex: 796 -> 0796)
        # C'est indispensable pour que le flux WFS de l'IGN réponde.
        numero = str(p['numero']).strip().zfill(4)
        
        parcelle_ref_display = f"{section} {numero}"
        parcelle_id_folder = f"{section}_{numero}"
        
        # Création du dossier de sortie spécifique
        current_out_dir = base_out_dir / parcelle_id_folder

        # Vérifier si la parcelle a déjà été traitée
        if is_parcelle_already_processed(current_out_dir):
            skipped_count += 1
            logger.info(f"[{index}/{total}] ⏭️  Déjà traitée, skip : {parcelle_ref_display}")
            continue

        # Créer le dossier si nécessaire
        current_out_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"[{index}/{total}] Traitement : {parcelle_ref_display}")

        try:
            # Préparation de l'objet attendu par le pipeline
            # On passe les données formatées (avec le 0 devant)
            reference_formattee = [{"section": section, "numero": numero}]
            
            start_time = time.time()
            
            # Appel direct de la fonction du pipeline
            result = run_pipeline_from_parcelles(
                parcelles=reference_formattee,
                code_insee=code_insee_latresne,
                commune_nom="Latresne",
                out_dir=str(current_out_dir)
            )
            
            duration = round(time.time() - start_time, 2)
            processed_count += 1
            logger.info(f"✅ Succès pour {parcelle_ref_display} ({duration}s)")

        except Exception as e:
            # En cas d'erreur, on logue et on passe à la suivante
            logger.error(f"❌ Erreur sur {parcelle_ref_display} : {str(e)}")
            continue

    logger.info("🏁 Fin du traitement par lots.")
    logger.info(f"📊 Résumé : {processed_count} traitées, {skipped_count} déjà traitées, {total - processed_count - skipped_count} en erreur")

if __name__ == "__main__":
    run_batch_latresne()