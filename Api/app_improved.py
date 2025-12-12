import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import joblib

# -----------------------
# 1️⃣ Chargement des données et du modèle
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

# -----------------------
# 2️⃣ Page d'accueil
# -----------------------
st.title("📊 Pipeline ETL & Prédiction Churn - E-commerce")
menu = ["Exploration RFM", "Visualisation Gold", "Prédiction Churn"]
choice = st.sidebar.selectbox("Menu", menu)

# -----------------------
# 3️⃣ Exploration RFM (Silver)
# -----------------------
if choice == "Exploration RFM":
    st.header("Analyse RFM - Données Silver")

    # Slider pour le seuil de churn
    churn_threshold = st.slider("Seuil churn (jours depuis dernier achat)", min_value=30, max_value=365, value=60, step=5)
    
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

    # Graphiques interactifs
    st.subheader("Distributions RFM")
    fig, axes = plt.subplots(1,3,figsize=(15,4))
    sns.histplot(df_rfm['recency_days'], bins=30, kde=True, ax=axes[0])
    axes[0].set_title("Récence (jours)")
    sns.histplot(df_rfm['monetary'], bins=30, kde=True, ax=axes[1])
    axes[1].set_title("Chiffre d'affaires")
    sns.histplot(df_rfm['frequency'], bins=30, kde=True, ax=axes[2])
    axes[2].set_title("Fréquence")
    st.pyplot(fig)

    st.subheader("Churn clients")
    fig2, ax2 = plt.subplots()
    sns.countplot(x='churn', data=df_rfm, ax=ax2)
    ax2.set_title(f"Clients churn vs actifs (seuil={churn_threshold} jours)")
    st.pyplot(fig2)

    # Filtres interactifs
    st.subheader("Filtrer les clients RFM")
    recency_filter = st.slider("Récence max (jours)", 0, int(df_rfm['recency_days'].max()), int(df_rfm['recency_days'].max()/2))
    frequency_filter = st.slider("Fréquence min", 0, int(df_rfm['frequency'].max()), 1)
    monetary_filter = st.slider("Monetary min", 0, int(df_rfm['monetary'].max()), 10)

    df_filtered = df_rfm[(df_rfm['recency_days'] <= recency_filter) &
                         (df_rfm['frequency'] >= frequency_filter) &
                         (df_rfm['monetary'] >= monetary_filter)]
    st.write(f"Nombre de clients filtrés : {len(df_filtered)}")
    st.dataframe(df_filtered)

# -----------------------
# 4️⃣ Visualisation Gold
# -----------------------
elif choice == "Visualisation Gold":
    st.header("Données Gold - Features par client")
    st.dataframe(df_gold.head())

    st.subheader("Total dépensé par client")
    fig, ax = plt.subplots(figsize=(8,4))
    sns.histplot(df_gold['total_spent'], bins=30, kde=True, ax=ax)
    st.pyplot(fig)

# -----------------------
# 5️⃣ Prédiction Churn (single ou batch)
# -----------------------
elif choice == "Prédiction Churn":
    st.header("Prédiction Churn")

    # Option pour upload CSV
    file_upload = st.file_uploader("Uploader un fichier CSV (optionnel)", type=['csv'])
    
    if file_upload:
        df_input = pd.read_csv(file_upload)
        st.subheader("Aperçu des données")
        st.dataframe(df_input.head())
        
        X = df_input[['recency_days','frequency','monetary','total_items','avg_price']]
        X_scaled = scaler.transform(X)
        preds = model.predict(X_scaled)
        df_input['churn_pred'] = preds
        st.subheader("Prédictions Churn")
        st.dataframe(df_input)
    else:
        st.subheader("Prédiction d'un client individuel")
        recency_days = st.number_input("Récence (jours depuis dernier achat)", min_value=0)
        frequency = st.number_input("Nombre de commandes", min_value=0)
        monetary = st.number_input("Chiffre d'affaires total", min_value=0.0)
        total_items = st.number_input("Quantité totale achetée", min_value=0)
        avg_price = st.number_input("Prix moyen par produit", min_value=0.0)

        if st.button("Prédire churn"):
            X = np.array([[recency_days, frequency, monetary, total_items, avg_price]])
            X_scaled = scaler.transform(X)
            pred = model.predict(X_scaled)[0]
            prob = model.predict_proba(X_scaled)[0,1]
            st.success(f"Churn prédiction: {pred} (probabilité: {prob:.2f})")
