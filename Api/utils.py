import joblib
import os
import logging
import numpy as np
import pandas as pd

MODEL_DIR = "/home/mountah_lodia/ecommerce_project/ecommerce_project/ml/model"

logger = logging.getLogger("api")

# 🔑 FEATURES pour le modèle (FR)
FEATURES = [
    "Chiffre d'affaires total",
    "Nombre de commandes",
    "Nombre total d'articles",
    "Prix moyen par article"
]

def load_artifacts():
    scaler = joblib.load(os.path.join(MODEL_DIR, "scaler.pkl"))
    model = joblib.load(os.path.join(MODEL_DIR, "random_forest_churn.pkl"))
    logger.info("Scaler et modèle chargés avec succès.")
    return scaler, model

def preprocess_input(data: pd.DataFrame, scaler):
    if not all(col in data.columns for col in FEATURES):
        missing = list(set(FEATURES) - set(data.columns))
        raise ValueError(f"Colonnes manquantes pour le modèle : {missing}")
    X = data[FEATURES]
    X_scaled = scaler.transform(X)
    return X_scaled

def predict_churn(X_scaled, model):
    prob = model.predict_proba(X_scaled)[:, 1]
    pred = (prob >= 0.5).astype(int)
    return prob, pred

def rfm_analysis(df_silver, churn_threshold):
    df_rfm = df_silver.groupby("CustomerID").agg({
        "InvoiceDate": lambda x: (pd.Timestamp.today() - pd.to_datetime(x.max())).days,
        "InvoiceNo": "count",
        "line_total": "sum",
        "Quantity": "sum",
        "UnitPrice": "mean"
    }).rename(columns={
        "InvoiceDate": "recency_days",
        "InvoiceNo": "frequency",
        "line_total": "monetary",
        "Quantity": "total_items",
        "UnitPrice": "avg_price"
    }).reset_index()

    df_rfm['churn'] = (df_rfm['recency_days'] > churn_threshold).astype(int)
    logger.info("Analyse RFM effectuée avec succès.")
    return df_rfm

def log_prediction(customer_id, chiffre_affaires, nb_commandes, total_articles, prix_moyen, churn_prob, churn_pred):
    logger.info(f"Prédiction pour CustomerID {customer_id} : "
                f"Chiffre_affaires={chiffre_affaires}, Nb_commandes={nb_commandes}, "
                f"Total_articles={total_articles}, Prix_moyen={prix_moyen} => "
                f"Churn_Prob={churn_prob:.4f}, Churn_Pred={churn_pred}")

if __name__ == "__main__":
    scaler, model = load_artifacts()

    data = pd.DataFrame({
        "CustomerID": [12345],
        "Chiffre d'affaires total": [250.0],
        "Nombre de commandes": [10],
        "Nombre total d'articles": [15],
        "Prix moyen par article": [16.67]
    })

    X_scaled = preprocess_input(data, scaler)
    probabilities, predictions = predict_churn(X_scaled, model)

    for i, row in data.iterrows():
        log_prediction(
            row["CustomerID"],
            row["Chiffre d'affaires total"],
            row["Nombre de commandes"],
            row["Nombre total d'articles"],
            row["Prix moyen par article"],
            probabilities[i],
            predictions[i]
        )

    churn_threshold = 60
    df_rfm = rfm_analysis(data, churn_threshold)
    print(df_rfm)
    logger.info("Test des fonctions utilitaires terminé avec succès.")
