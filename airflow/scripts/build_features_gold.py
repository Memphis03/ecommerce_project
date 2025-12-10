from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, avg

def run_features(input_path, output_path):
    spark = SparkSession.builder.appName("FeatureGold").getOrCreate()

    df = spark.read.parquet(f"{input_path}/silver/silver_data.parquet")

    df_features = df.groupBy("CustomerID").agg(
        _sum("line_total").alias("total_spent"),
        _sum("Quantity").alias("total_items_purchased"),
        avg("UnitPrice").alias("avg_price"),
        count("InvoiceNo").alias("num_orders")
    )

    df_features.write.mode("overwrite").parquet(
        f"{output_path}/gold/features_data.parquet"
    )

    spark.stop()


if __name__ == "__main__":
    import sys
    run_features(sys.argv[1], sys.argv[2])
