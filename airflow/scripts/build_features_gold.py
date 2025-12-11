from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg
import os

def run_features(input_path, output_path):
    spark = SparkSession.builder.appName("FeatureGold").getOrCreate()

    # Lecture des données Silver
    df = spark.read.parquet(f"{input_path}/silver/silver_data.parquet")

    # Vérifier que les colonnes nécessaires existent
    required_columns = ["CustomerID", "line_total", "Quantity", "UnitPrice", "InvoiceNo"]
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Colonnes manquantes dans le fichier Silver : {missing_cols}")

    # Agrégation pour créer les features Gold
    df_features = df.groupBy("CustomerID").agg(
        _sum("line_total").alias("total_spent"),
        _sum("Quantity").alias("total_items_purchased"),
        avg("UnitPrice").alias("avg_price"),
        count("InvoiceNo").alias("num_orders")
    )

    # Création du dossier gold si inexistant
    os.makedirs(f"{output_path}/gold", exist_ok=True)

    # Écriture Parquet
    features_path = f"{output_path}/gold/features_data.parquet"
    df_features.write.mode("overwrite").parquet(features_path)
    print(f"[INFO] Features Gold générées avec succès : {features_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_features(sys.argv[1], sys.argv[2])
