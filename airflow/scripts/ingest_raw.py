from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, split
import os

def run_ingestion(input_path, output_path, start_date=None):
    spark = SparkSession.builder.appName("IngestionRaw").getOrCreate()

    # Lecture CSV RAW
    df = spark.read.csv(
        input_path,
        header=True,
        inferSchema=True,
        encoding="ISO-8859-1",
        mode="PERMISSIVE"
    )

    # Extraire uniquement la partie date (avant l'espace) et convertir en date
    df = df.withColumn("InvoiceDate", split(col("InvoiceDate"), " ").getItem(0))
    df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "M/d/yyyy"))

    # Filtrer par date si start_date fourni
    if start_date:
        df = df.filter(col("InvoiceDate").isNotNull() & (col("InvoiceDate") >= start_date))

    # Créer dossier bronze
    os.makedirs(f"{output_path}/bronze", exist_ok=True)

    # Nom de fichier basé sur la date passée en argument
    if start_date:
        date_str = start_date.replace("-", "")  # 2011-02-04 -> 20110204
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")

    parquet_path = f"{output_path}/bronze/bronze_{date_str}.parquet"

    # Écriture au format Parquet
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"[INFO] Bronze généré : {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    start_date = sys.argv[3] if len(sys.argv) > 3 else None
    run_ingestion(sys.argv[1], sys.argv[2], start_date)
