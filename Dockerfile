FROM python:3.11-slim

WORKDIR /app

# Définir les variables d'environnement
ENV PYTHONUNBUFFERED=1

# Installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . .

# Commande de démarrage
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]