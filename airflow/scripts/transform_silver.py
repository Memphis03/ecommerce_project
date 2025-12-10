from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
from pyspark.sql.types import IntegerType, DoubleType

def run_transform(input_path, output_path):
    spark = SparkSession.builder.appName("TransformSilver").getOrCreate()

    df = spark.read.parquet(f"{input_path}/bronze/raw_data.parquet")

    df = df.withColumn('Quantity', col('Quantity').cast(IntegerType()))
    df = df.withColumn('UnitPrice', col('UnitPrice').cast(DoubleType()))
    df = df.withColumn('line_total', col('Quantity') * col('UnitPrice'))
    df = df.withColumn('is_return', when(col('InvoiceNo').startswith("C"), lit(1)).otherwise(0))

    df_clean = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0) &
        (col("CustomerID").isNotNull())
    )

    df_clean = df_clean.withColumn(
        "InvoiceDate", to_date(col("InvoiceDate"), "MM/dd/yyyy HH:mm")
    )

    df_clean.write.mode("overwrite").parquet(f"{output_path}/silver/silver_data.parquet")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_transform(sys.argv[1], sys.argv[2])
