📊 E-commerce Churn Prediction & RFM Analysis
Description
Ce projet consiste en un pipeline ETL complet pour un site e-commerce, incluant :
Extraction, transformation et chargement (ETL) des données depuis des fichiers source.
Génération de features gold par client.
Analyse RFM (Recency, Frequency, Monetary) sur les données silver.
Prédiction du churn client via un modèle de machine learning (RandomForestClassifier) avec prétraitement (StandardScaler).
Visualisation interactive avec Streamlit.
API REST pour la prédiction de churn via FastAPI.

🗂 Structure du projet
ecommerce_project/
├── Api/                   # API FastAPI
├── airflow/               # Scripts Airflow / ETL
├── data/
│   ├── silver/            # Données nettoyées
│   └── gold/              # Données features
├── ml/                    # Modèles et scalers
├── notebooks/             # Notebooks exploratoires
├── streamlit_app.py       # Interface utilisateur Streamlit
├── main.py                # Point d’entrée API
├── requirements.txt
└── README.md

Cloner le dépôt :
git clone <repo_url>
cd ecommerce_project


🔧 Notes techniques
Les noms des colonnes doivent correspondre exactement à ceux utilisés lors de l’entraînement du modèle.
Les modèles et scalers sont sérialisés avec joblib et stockés dans ml/.
Streamlit utilise @st.cache_data et @st.cache_resource pour optimiser le chargement des données et du modèle.
Spark peut être utilisé pour construire les features gold à partir des données silver.

📊 Visualisations
RFM Analysis : distributions de récence, fréquence et montant, filtres interactifs.
Churn Prediction : probabilités et labels prévus par le modèle.
Gold Features : histogrammes des montants dépensés par client.

👨‍💻 Auteur
Mouhamadou Mountaga Diallo
