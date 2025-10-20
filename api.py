from fastapi import FastAPI, Query
from intersections import main  # ton module
app = FastAPI()

@app.get("/intersections")
def run_intersections(section: str, numero: str):
    result_path = main(section=section, numero=numero)
    return {"message": "Rapport généré", "fichier": result_path}
