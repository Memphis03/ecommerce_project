E-commerce Churn Prediction & RFM Analysis
Description
Ce projet est un pipeline complet ETL pour un site e-commerce, conçu pour analyser le comportement des clients et prédire le churn. Il combine traitement de données, machine learning, orchestration de workflow et visualisation interactive.

Les principales fonctionnalités incluent :
Extraction, Transformation et Chargement (ETL) des données depuis des fichiers sources silver.
Génération de features gold par client, calculées à partir des données silver.
Analyse RFM (Recency, Frequency, Monetary) pour segmenter les clients selon leur comportement d’achat.
Prédiction du churn des clients via un modèle de machine learning (RandomForestClassifier) avec prétraitement (StandardScaler).
Visualisations interactives des analyses et prédictions avec Streamlit.
API REST permettant la prédiction du churn en temps réel via FastAPI.
Orchestration ETL avec Airflow, planification et automatisation des pipelines de traitement des données.

Structure du projet
ecommerce_project/
├── Api/                   # API FastAPI pour la prédiction du churn
├   ├── mon_script.py       # Interface utilisateur Streamlit pour visualisation
├   ├── main.py                # Point d’entrée de l’API FastAPI
├── airflow/               # DAGs et scripts Airflow pour l’ETL et l’automatisation
├── analysis/              # Analyse des comportement des clients
├── data/                  # Données nettoyées issues de la collecte
│   ├── silver/           
│   └── gold/              # Features calculées pour chaque client
├── ml/                    # Modèles ML sérialisés et scalers
└── src            
└── README.md              # Documentation du projet

Notes techniques
Correspondance des colonnes : les noms des colonnes dans les données d’entrée doivent correspondre exactement aux noms utilisés lors de l’entraînement du modèle.
Sérialisation : les modèles et scalers sont stockés dans ml/ au format joblib.
Optimisation Streamlit : utilisation de @st.cache_data et @st.cache_resource pour accélérer le chargement des données et du modèle.
ETL avancé avec Airflow : Airflow orchestre les pipelines ETL, exécute les DAGs automatiquement et gère les dépendances entre tâches.
Spark peut être utilisé pour transformer les données silver et générer les features gold à grande échelle.

Visualisations incluses
RFM Analysis : distributions de la récence (recency_days), fréquence (frequency) et montant dépensé (monetary) avec possibilité de filtrer les clients.
Churn Prediction : visualisation des probabilités de churn et des labels prédits par le modèle.
Gold Features : histogrammes des montants dépensés par client, nombre total d’articles achetés, prix moyen par article et nombre de commandes.

🚀 Lancement de l’application
Pour que l’interface Streamlit fonctionne correctement, l’API FastAPI doit être lancée en premier :
Activer l’environnement virtuel (si nécessaire)
conda activate venv_py312  # ou source <env_name>/bin/activate
Lancer l’API FastAPI
cd Api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
main:app correspond au fichier main.py et à l’instance FastAPI app.
L’API sera disponible sur : http://localhost:8000
Lancer l’application Streamlit
streamlit run ../streamlit_app.py
Streamlit se connectera automatiquement à l’API pour récupérer les prédictions.
L’interface sera accessible dans ton navigateur à http://localhost:8501.
⚠️ Important : Toujours lancer l’API avant Streamlit, sinon l’interface ne pourra pas récupérer les données de prédiction.

« Le projet est entièrement fonctionnel en local.
Le déploiement cloud sera effectué ultérieurement afin de respecter les bonnes pratiques de sécurité et de configuration. »

👨‍💻 Auteur
Mouhamadou Mountaga Diallo
