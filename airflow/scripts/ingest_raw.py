from pyspark.sql import SparkSession
import os

def run_ingestion(input_path, output_path):
    spark = SparkSession.builder.appName("IngestionRaw").getOrCreate()

    # Lecture CSV
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        encoding="ISO-8859-1",
        mode="PERMISSIVE"  # Ignore les lignes mal formées
    )

    # Créer le dossier bronze s'il n'existe pas
    os.makedirs(f"{output_path}/bronze", exist_ok=True)

    # Écriture Parquet
    parquet_path = f"{output_path}/bronze/raw_data.parquet"
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"[INFO] Ingestion terminée, fichier Parquet créé ici : {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_ingestion(sys.argv[1], sys.argv[2])
