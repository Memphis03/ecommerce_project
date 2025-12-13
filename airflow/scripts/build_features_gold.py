from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum as _sum,
    count,
    avg,
    col,
    to_date,
    max,
    datediff,
    lit
)
from pyspark.sql.types import IntegerType, BooleanType
import os
from datetime import datetime

def run_features(input_path, output_path, start_date=None):
    spark = SparkSession.builder.appName("FeatureGold").getOrCreate()

    silver_dir = f"{input_path}/silver"
    silver_files = [f for f in os.listdir(silver_dir) if f.endswith(".parquet")]
    silver_paths = [f"{silver_dir}/{f}" for f in silver_files]

    if not silver_paths:
        print(f"[WARN] Aucun fichier Parquet Silver trouvé dans {silver_dir}")
        spark.stop()
        return

    # Lecture fichier par fichier pour caster 'is_return' avant la fusion
    dfs = []
    for path in silver_paths:
        tmp_df = spark.read.parquet(path)
        if 'is_return' in tmp_df.columns:
            # Convertir en Integer (0/1) si c'est Boolean
            tmp_df = tmp_df.withColumn("is_return", 
                                       col("is_return").cast(IntegerType()))
        else:
            # Ajouter colonne manquante avec valeur par défaut
            tmp_df = tmp_df.withColumn("is_return", lit(0))
        dfs.append(tmp_df)

    # Fusionner tous les DataFrames
    from functools import reduce
    from pyspark.sql import DataFrame
    df = reduce(DataFrame.unionByName, dfs)

    if start_date:
        df = df.filter(
            col("InvoiceDate").isNotNull() & 
            (col("InvoiceDate") >= lit(start_date))
        )

    # 🔑 Date de référence = dernière date du dataset
    ref_date = df.select(max("InvoiceDate")).collect()[0][0]

    # Agrégation comportement client (RFM)
    df_features = df.groupBy("CustomerID").agg(
        datediff(lit(ref_date), max("InvoiceDate")).alias("recency_days"),
        count("InvoiceNo").alias("frequency"),
        _sum("line_total").alias("monetary"),
        _sum("Quantity").alias("total_items"),
        avg("UnitPrice").alias("avg_price")
    )

    os.makedirs(f"{output_path}/gold", exist_ok=True)

    date_str = start_date.replace("-", "") if start_date else datetime.now().strftime("%Y%m%d")
    parquet_path = f"{output_path}/gold/ecommerce_features_{date_str}.parquet"

    df_features.write.mode("overwrite").parquet(parquet_path)
    print(f"[INFO] Gold RFM généré : {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    import sys
    start_date = sys.argv[3] if len(sys.argv) > 3 else None
    run_features(sys.argv[1], sys.argv[2], start_date)