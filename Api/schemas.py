from pydantic import BaseModel, Field

# ==============================
# Requête pour prédiction churn
# ==============================
class CustomerFeatures(BaseModel):
    Chiffre_daffaires_total: float = Field(..., example=350.5)
    Nombre_de_commandes: int = Field(..., example=12)
    Nombre_total_d_articles: int = Field(..., example=42)
    Prix_moyen_par_article: float = Field(..., example=9.8)

class ChurnPredictionRequest(CustomerFeatures):
    pass

# ==============================
# Réponse prédiction churn
# ==============================
class ChurnPredictionResponse(BaseModel):
    churn_probability: float
    churn_prediction: int # 1 = churn, 0 = pas churn

# ==============================
# Analyse RFM pour Silver
# ==============================
class RFMAnalysisResponse(BaseModel):
    customer_id: str = Field(..., example="12345")
    recency_days: float = Field(..., example=45)
    frequency: float = Field(..., example=12)
    monetary: float = Field(..., example=350.5)
    total_items: float = Field(..., example=42)
    avg_price: float = Field(..., example=9.8)
    churn: int = Field(..., example=1)

# ==============================
# Données Gold par client
# ==============================
class GoldFeaturesResponse(BaseModel):
    customer_id: str = Field(..., example="12345")
    Chiffre_daffaires_total: float = Field(..., example=1500.75)
    Total_retours: float = Field(..., example=200.0)
    Nombre_total_d_articles: int = Field(..., example=300)
    Prix_moyen_par_article: float = Field(..., example=10.5)

# ==============================
# Erreur
# ==============================
class ErrorResponse(BaseModel):
    detail: str = Field(..., example="Une erreur est survenue lors du traitement de la requête.")
