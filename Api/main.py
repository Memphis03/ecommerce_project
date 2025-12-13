# main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import numpy as np
import logging

from schemas import CustomerFeatures, ChurnPredictionResponse
from utils import load_artifacts

# ---------------------------
# FastAPI
# ---------------------------
app = FastAPI(
    title="Churn Prediction API",
    description="API pour prédire le churn des clients e-commerce",
    version="1.0.0"
)

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

# ---------------------------
# CORS
# ---------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------
# Chargement des artefacts
# ---------------------------
print("Chargement du scaler et du modèle...")
scaler, model = load_artifacts()
print("Scaler et modèle chargés avec succès.")

@app.get("/")
def read_root():
    return {
        "message": "Bienvenue sur l'API Churn Prediction. Utilisez /health pour vérifier l'état et /predict pour prédire le churn."
    }
# ---------------------------
# Health check
# ---------------------------
@app.get("/health")
def health_check():
    return {"status": "ok", "message": "API opérationnelle"}

# ---------------------------
# Prédiction churn
# ---------------------------
@app.post("/predict", response_model=ChurnPredictionResponse)
def predict_churn(features: CustomerFeatures):
    try:
        logger.info(f"Requête reçue : {features.model_dump()}")

        # ⚠️ Attention à l’ordre des features
        X = np.array([[
            features.Chiffre_daffaires_total,
            features.Nombre_total_d_articles,
            features.Prix_moyen_par_article,
            features.Nombre_de_commandes
        ]])

        # Scaling
        X_scaled = scaler.transform(X)

        # Prédiction
        churn_pred = model.predict(X_scaled)[0]
        churn_prob = model.predict_proba(X_scaled)[0, 1]

        return ChurnPredictionResponse(
            churn_prediction=int(churn_pred),
            churn_probability=float(round(churn_prob, 4))
        )

    except Exception as e:
        logger.error(f"Erreur API : {e}")
        raise HTTPException(status_code=500, detail=str(e))
