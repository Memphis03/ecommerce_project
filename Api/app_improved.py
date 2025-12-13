import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import joblib

# -----------------------
@st.cache_data
def load_data():
    df_silver = pd.read_parquet("/home/mountah_lodia/ecommerce_project/ecommerce_project/data/silver/cleaned_ecommerce_data.parquet")
    df_gold = pd.read_parquet("/home/mountah_lodia/ecommerce_project/ecommerce_project/data/gold/ecommerce_features.parquet")
    return df_silver, df_gold

@st.cache_resource
def load_model():
    model = joblib.load("/home/mountah_lodia/ecommerce_project/ecommerce_project/ml/model/random_forest_churn.pkl")
    scaler = joblib.load("/home/mountah_lodia/ecommerce_project/ecommerce_project/ml/model/scaler.pkl")
    return model, scaler

df_silver, df_gold = load_data()
model, scaler = load_model()

st.title("📊 Pipeline ETL & Prédiction Churn - E-commerce")
menu = ["Exploration RFM", "Visualisation Gold", "Prédiction Churn"]
choice = st.sidebar.selectbox("Menu", menu)

# -----------------------
if choice == "Exploration RFM":
    st.header("Analyse RFM - Données Silver")
    churn_threshold = st.slider("Seuil churn (jours depuis dernier achat)", 30, 365, 60, 5)

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
    st.subheader("Aperçu des données RFM")
    st.dataframe(df_rfm.head())

elif choice == "Visualisation Gold":
    st.header("Données Gold - Features par client")
    st.dataframe(df_gold.head())

elif choice == "Prédiction Churn":
    st.header("Prédiction Churn")
    file_upload = st.file_uploader("Uploader un fichier CSV (optionnel)", type=['csv'])

    required_features = [
        "Chiffre d'affaires total",
        "Nombre de commandes",
        "Nombre total d'articles",
        "Prix moyen par article"
    ]

    if file_upload:
        df_input = pd.read_csv(file_upload)
        st.subheader("Aperçu des données")
        st.dataframe(df_input.head())
        if not all(col in df_input.columns for col in required_features):
            st.error(f"Le fichier CSV doit contenir : {required_features}")
        else:
            X = df_input[required_features]
            X_scaled = scaler.transform(X)
            preds = model.predict(X_scaled)
            probs = model.predict_proba(X_scaled)[:, 1]
            df_input['churn_pred'] = preds
            df_input['churn_prob'] = probs
            st.subheader("Prédictions Churn")
            st.dataframe(df_input)

    else:
        st.subheader("Prédiction d'un client individuel")
        Chiffre_daffaires_total = st.number_input("Chiffre d'affaires total", 0.0)
        Nombre_de_commandes = st.number_input("Nombre de commandes", 0)
        Nombre_total_d_articles = st.number_input("Nombre total d'articles", 0)
        Prix_moyen_par_article = st.number_input("Prix moyen par article", 0.0)

        if st.button("Prédire churn"):
            X = np.array([[Chiffre_daffaires_total, Nombre_de_commandes, Nombre_total_d_articles, Prix_moyen_par_article]])
            X_scaled = scaler.transform(X)
            pred = model.predict(X_scaled)[0]
            prob = model.predict_proba(X_scaled)[0,1]
            st.success(f"Churn prédiction : {pred} (probabilité : {prob:.2f})")
