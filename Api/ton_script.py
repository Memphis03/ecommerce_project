import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import requests
import time

# -------------------------------------------------------
# Configuration API
# -------------------------------------------------------
API_URL = "http://localhost:8000"

# -------------------------------------------------------
# Vérification disponibilité API
# -------------------------------------------------------
def wait_for_api(url, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/health", timeout=2)
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            time.sleep(1)
    return False


st.set_page_config(page_title="Churn Prediction", layout="wide")

st.info("⏳ Vérification de la disponibilité de l’API FastAPI...")

if not wait_for_api(API_URL):
    st.error("❌ L’API FastAPI n’est pas disponible. Lance d’abord Uvicorn.")
    st.stop()

st.success("✅ API FastAPI connectée avec succès")

# -------------------------------------------------------
# Chargement des données
# -------------------------------------------------------
@st.cache_data
def load_data():
    df_silver = pd.read_parquet(
        "/home/mountah_lodia/ecommerce_project/ecommerce_project/data/silver/cleaned_ecommerce_data.parquet"
    )
    df_gold = pd.read_parquet(
        "/home/mountah_lodia/ecommerce_project/ecommerce_project/data/gold/ecommerce_features.parquet"
    )
    return df_silver, df_gold


df_silver, df_gold = load_data()

# -------------------------------------------------------
# Page d'accueil
# -------------------------------------------------------
st.title("📊 Pipeline ETL & Prédiction Churn - E-commerce")

menu = ["Exploration RFM", "Visualisation Gold", "Prédiction Churn"]
choice = st.sidebar.selectbox("Menu", menu)

# -------------------------------------------------------
# Exploration RFM
# -------------------------------------------------------
if choice == "Exploration RFM":

    st.header("Analyse RFM - Données Silver")

    seuil_churn = st.slider(
        "Seuil churn (jours depuis dernier achat)",
        min_value=30,
        max_value=365,
        value=60,
        step=5
    )

    df_rfm = df_silver.groupby("CustomerID").agg({
        "InvoiceDate": lambda x: (pd.Timestamp.today() - pd.to_datetime(x.max())).days,
        "InvoiceNo": "count",
        "line_total": "sum",
        "Quantity": "sum",
        "UnitPrice": "mean"
    }).rename(columns={
        "InvoiceDate": "Recence_jours",
        "InvoiceNo": "Nombre_de_commandes",
        "line_total": "Chiffre_daffaires_total",
        "Quantity": "Nombre_total_d_articles",
        "UnitPrice": "Prix_moyen_par_article"
    }).reset_index()

    df_rfm["Churn"] = (df_rfm["Recence_jours"] > seuil_churn).astype(int)

    st.subheader("Aperçu des données RFM")
    st.dataframe(df_rfm.head())

    st.subheader("Distributions RFM")
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))

    sns.histplot(df_rfm["Recence_jours"], bins=30, kde=True, ax=axes[0])
    axes[0].set_title("Récence (jours)")

    sns.histplot(df_rfm["Chiffre_daffaires_total"], bins=30, kde=True, ax=axes[1])
    axes[1].set_title("Chiffre d'affaires")

    sns.histplot(df_rfm["Nombre_de_commandes"], bins=30, kde=True, ax=axes[2])
    axes[2].set_title("Nombre de commandes")

    st.pyplot(fig)

    st.subheader("Churn clients")
    fig2, ax2 = plt.subplots()
    sns.countplot(x="Churn", data=df_rfm, ax=ax2)
    ax2.set_title(f"Clients churn vs actifs (seuil={seuil_churn} jours)")
    st.pyplot(fig2)

# -------------------------------------------------------
# Visualisation Gold
# -------------------------------------------------------
elif choice == "Visualisation Gold":

    st.header("Données Gold - Features par client")
    st.dataframe(df_gold.head())

    st.subheader("Total dépensé par client")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(df_gold["monetary"], bins=30, kde=True, ax=ax)
    st.pyplot(fig)

# -------------------------------------------------------
# Prédiction Churn
# -------------------------------------------------------
elif choice == "Prédiction Churn":

    st.header("Prédiction Churn")

    fichier_csv = st.file_uploader("Uploader un fichier CSV (optionnel)", type=["csv"])

    colonnes_obligatoires = [
        "Chiffre_daffaires_total",
        "Nombre_de_commandes",
        "Nombre_total_d_articles",
        "Prix_moyen_par_article"
    ]

    if fichier_csv:
        df_input = pd.read_csv(fichier_csv)
        st.dataframe(df_input.head())

        if not all(col in df_input.columns for col in colonnes_obligatoires):
            st.error(f"Colonnes requises : {colonnes_obligatoires}")
        else:
            results = []

            for _, row in df_input.iterrows():
                payload = {
                    "Chiffre_daffaires_total": float(row["Chiffre_daffaires_total"]),
                    "Nombre_de_commandes": int(row["Nombre_de_commandes"]),
                    "Nombre_total_d_articles": int(row["Nombre_total_d_articles"]),
                    "Prix_moyen_par_article": float(row["Prix_moyen_par_article"])
                }

                response = requests.post(f"{API_URL}/predict", json=payload)

                row_dict = row.to_dict()

                if response.status_code == 200:
                    res = response.json()
                    row_dict["Churn_pred"] = res["churn_prediction"]
                    row_dict["Churn_prob"] = res["churn_probability"]
                else:
                    row_dict["Churn_pred"] = None
                    row_dict["Churn_prob"] = None

                results.append(row_dict)

            st.subheader("Résultats de prédiction")
            st.dataframe(pd.DataFrame(results))

    else:
        chiffre_affaires = st.number_input("Chiffre d'affaires total", min_value=0.0)
        nb_commandes = st.number_input("Nombre de commandes", min_value=0)
        nb_articles = st.number_input("Nombre total d'articles", min_value=0)
        prix_moyen = st.number_input("Prix moyen par article", min_value=0.0)

        if st.button("Prédire churn"):
            payload = {
                "Chiffre_daffaires_total": chiffre_affaires,
                "Nombre_de_commandes": nb_commandes,
                "Nombre_total_d_articles": nb_articles,
                "Prix_moyen_par_article": prix_moyen
            }

            response = requests.post(f"{API_URL}/predict", json=payload)

            if response.status_code == 200:
                res = response.json()
                st.success(
                    f"Churn : {res['churn_prediction']} "
                    f"(probabilité : {res['churn_probability']:.2f})"
                )
            else:
                st.error(response.text)
