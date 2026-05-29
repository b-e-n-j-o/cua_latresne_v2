from google import genai
import time

# Initialisation du client
# Assurez-vous que votre variable d'environnement GEMINI_API_KEY est définie
client = genai.Client()

def count_pdf_tokens(file_path, model_name="gemini-2.0-flash"):
    """
    Téléverse un PDF et compte les jetons associés.
    """
    print(f"Téléversement du fichier : {file_path}...")
    
    # 1. Téléverser le fichier via l'API Files
    uploaded_file = client.files.upload(path=file_path)
    
    # 2. Attendre que le fichier soit traité (nécessaire pour les fichiers volumineux)
    while uploaded_file.state.name == "PROCESSING":
        print("Traitement du fichier en cours...")
        time.sleep(2)
        uploaded_file = client.files.get(name=uploaded_file.name)
        
    if uploaded_file.state.name == "FAILED":
        raise ValueError("Le traitement du fichier a échoué.")

    print(f"Fichier prêt. État : {uploaded_file.state.name}")

    # 3. Compter les jetons
    # On passe le fichier directement dans la liste 'contents'
    tokens = client.models.count_tokens(
        model=model_name,
        contents=[uploaded_file]
    )
    
    print(f"Nombre total de jetons : {tokens.total_tokens}")
    
    # Optionnel : Nettoyage (supprimer le fichier après comptage)
    client.files.delete(name=uploaded_file.name)
    
    return tokens.total_tokens

# Utilisation
pdf_path = "votre_document.pdf" # Remplacez par le chemin de votre fichier
count_pdf_tokens(pdf_path)