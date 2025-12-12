from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg, col, to_date
import os
from datetime import datetime

def run_features(input_path, output_path, start_date=None):
    spark = SparkSession.builder.appName("FeatureGold").getOrCreate()

    # Charger tous les fichiers Silver
    silver_dir = f"{input_path}/silver"
    silver_files = [f for f in os.listdir(silver_dir) if f.endswith(".parquet")]
    silver_paths = [f"{silver_dir}/{f}" for f in silver_files]

    if not silver_paths:
        print(f"[WARN] Aucun fichier Parquet Silver trouvé dans {silver_dir}")
        spark.stop()
        return

    df = spark.read.option("mergeSchema", "true").parquet(*silver_paths)

    # S'assurer que InvoiceDate est bien en date
    df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate")))

    # Filtrage incrémental si start_date fourni
    if start_date:
        df = df.filter(col("InvoiceDate").isNotNull() & (col("InvoiceDate") >= start_date))

    # Agrégation par CustomerID
    df_features = df.groupBy("CustomerID").agg(
        _sum("line_total").alias("total_spent"),
        _sum("Quantity").alias("total_items_purchased"),
        avg("UnitPrice").alias("avg_price"),
        count("InvoiceNo").alias("num_orders")
    )

    # Sauvegarde Gold
    os.makedirs(f"{output_path}/gold", exist_ok=True)

    # Nom du fichier basé sur la date passée en argument si fournie
    if start_date:
        date_str = start_date.replace("-", "")  # 2011-02-04 -> 20110204
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")

    parquet_path = f"{output_path}/gold/ecommerce_features_{date_str}.parquet"

    df_features.write.mode("overwrite").parquet(parquet_path)
    print(f"[INFO] Gold généré : {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    start_date = sys.argv[3] if len(sys.argv) > 3 else None
    run_features(sys.argv[1], sys.argv[2], start_date)
