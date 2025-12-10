from pyspark.sql import SparkSession

def run_ingestion(input_path, output_path):
    spark = SparkSession.builder.appName("IngestionRaw").getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True, encoding="ISO-8859-1")

    df.write.mode("overwrite").parquet(f"{output_path}/bronze/raw_data.parquet")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_ingestion(sys.argv[1], sys.argv[2])
