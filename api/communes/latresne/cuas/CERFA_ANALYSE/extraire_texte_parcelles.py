import PyPDF2
import pdfplumber
import os

def decouper_et_extraire_pdf(chemin_entree, chemin_sortie="pdf_reduit.pdf"):
    """
    Prend un PDF en entrée, extrait les pages 2 et 4, crée un nouveau PDF,
    et extrait le texte de ce nouveau PDF.

    Args:
        chemin_entree (str): Le chemin vers le fichier PDF d'entrée.
        chemin_sortie (str): Le chemin pour enregistrer le nouveau PDF de deux pages.

    Returns:
        str: Le texte extrait du nouveau PDF.
    """
    # Vérification de l'existence du fichier d'entrée
    if not os.path.exists(chemin_entree):
        return f"Erreur : Le fichier d'entrée n'existe pas à '{chemin_entree}'"

    # --- 1. Découpage et création du nouveau PDF (PyPDF2) ---
    print(f"🔄 Traitement du fichier : {chemin_entree}")
    
    # Créer un objet Writer pour le nouveau PDF
    pdf_writer = PyPDF2.PdfWriter()

    try:
        # Lire le PDF d'entrée
        with open(chemin_entree, 'rb') as file_input:
            pdf_reader = PyPDF2.PdfReader(file_input)

            # PyPDF2 utilise des index basés sur 0, donc :
            # Page 2 -> index 1
            # Page 4 -> index 3
            
            # Vérifier que le document a au moins 4 pages
            if len(pdf_reader.pages) < 4:
                return "Erreur : Le PDF doit contenir au moins 4 pages pour extraire les pages 2 et 4."

            # Ajouter la page 2 (index 1)
            pdf_writer.add_page(pdf_reader.pages[1])
            print("   -> Page 2 ajoutée.")

            # Ajouter la page 4 (index 3)
            pdf_writer.add_page(pdf_reader.pages[3])
            print("   -> Page 4 ajoutée.")

            # Écrire le nouveau PDF dans le fichier de sortie
            with open(chemin_sortie, 'wb') as file_output:
                pdf_writer.write(file_output)
            
            print(f"✅ Nouveau PDF de 2 pages créé : {chemin_sortie}")

    except Exception as e:
        return f"Une erreur s'est produite lors du découpage PyPDF2 : {e}"
    
    # --- 2. Extraction du texte du nouveau PDF (pdfplumber) ---
    texte_extrait = ""
    try:
        # Ouvrir le nouveau PDF avec pdfplumber
        with pdfplumber.open(chemin_sortie) as pdf:
            # Parcourir les pages du nouveau PDF (pages 1 et 2)
            for i, page in enumerate(pdf.pages):
                texte_page = page.extract_text()
                texte_extrait += f"\n--- Contenu de la Page {i+1} du PDF réduit ---\n"
                texte_extrait += texte_page if texte_page else "[Aucun texte lisible extrait sur cette page]"
        
        print("✅ Extraction de texte terminée.")
        return texte_extrait

    except Exception as e:
        return f"Une erreur s'est produite lors de l'extraction de texte (pdfplumber) : {e}"

# --- EXÉCUTION DU SCRIPT ---
# ⚠️ REMPLACEZ 'mon_document.pdf' par le nom de votre fichier PDF
NOM_FICHIER_ENTREE = "/Volumes/T7/Travaux_Freelance/KERELIA/CUAs/INTERSECTION_PIPELINE/LATRESNE/cua_latresne_v4/cerfa_CU_13410-2024-07-19.pdf" 
NOM_FICHIER_SORTIE = "mon_document_extrait.pdf" # Nom du nouveau PDF de deux pages

# Assurez-vous d'avoir installé les bibliothèques nécessaires :
# pip install PyPDF2 pdfplumber

resultat_extraction = decouper_et_extraire_pdf(NOM_FICHIER_ENTREE, NOM_FICHIER_SORTIE)

print("\n" + "="*50)
print("📚 RÉSULTAT DE L'EXTRACTION DE TEXTE")
print("="*50)
print(resultat_extraction)
print("="*50)