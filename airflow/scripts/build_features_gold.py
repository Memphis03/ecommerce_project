from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg
import os

def run_features(input_path, output_path):
    spark = SparkSession.builder.appName("FeatureGold").getOrCreate()

    df = spark.read.parquet(f"{input_path}/silver/cleaned_ecommerce_data.parquet")

    df_features = df.groupBy("CustomerID").agg(
        _sum("line_total").alias("total_spent"),
        _sum("Quantity").alias("total_items_purchased"),
        avg("UnitPrice").alias("avg_price"),
        count("InvoiceNo").alias("num_orders")
    )

    os.makedirs(f"{output_path}/gold", exist_ok=True)
    df_features.write.mode("overwrite").parquet(f"{output_path}/gold/ecommerce_features.parquet")

    print(f"[INFO] Gold généré")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_features(sys.argv[1], sys.argv[2])
