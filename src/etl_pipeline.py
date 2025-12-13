from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, sum as _sum, expr
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import count, avg, max, datediff
import os

def etl_pipeline(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("Ecommerce_ETL").getOrCreate()
    
    # Lecture Bronze
    df = spark.read.csv(input_path, header=True, inferSchema=True, encoding="ISO-8859-1")

    # Conversion InvoiceDate en gérant les formats invalides
    df = df.withColumn(
        "InvoiceDate",
        expr("try_to_timestamp(InvoiceDate, 'M/d/yyyy H:mm')")
    ).withColumn("InvoiceDate", col("InvoiceDate").cast("date"))

    # Colonnes dérivées
    df = df.withColumn("line_total", col("Quantity") * col("UnitPrice")) \
           .withColumn("is_return", when(col("InvoiceNo").startswith("C"), True).otherwise(False))

    # Nettoyage Silver
    df_cleaned = df.dropna(subset=['InvoiceNo', 'StockCode', 'Description', 'InvoiceDate']) \
                   .filter((col('Quantity') > 0) & (col('UnitPrice') > 0) & col('CustomerID').isNotNull()) \
                   .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
                   .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))

    # Filtre sur période
    df_cleaned = df_cleaned.filter(
        (col('InvoiceDate') >= to_date(lit('2010-12-01'))) &
        (col('InvoiceDate') <= to_date(lit('2011-12-31')))
    ).cache()

    # Créer dossiers si inexistant
    os.makedirs(os.path.join(output_path, "silver"), exist_ok=True)
    os.makedirs(os.path.join(output_path, "gold"), exist_ok=True)

    # Sauvegarde Silver
    df_cleaned.write.mode("overwrite").parquet(os.path.join(output_path, "silver", "cleaned_ecommerce_data.parquet"))

    # Gold — Features
    # Référence date : dernière date du dataset
    ref_date = df_cleaned.select(max("InvoiceDate")).collect()[0][0]

    df_features = df_cleaned.groupBy("CustomerID").agg(
        datediff(lit(ref_date), max("InvoiceDate")).alias("recency_days"),
        count("InvoiceNo").alias("frequency"),
        _sum("line_total").alias("monetary"),
        _sum("Quantity").alias("total_items"),
        avg("UnitPrice").alias("avg_price"),
        _sum(when(col("is_return") == True, col("line_total")).otherwise(0)).alias("total_returns")
    )

    df_features.write.mode("overwrite").parquet(os.path.join(output_path, "gold", "ecommerce_features.parquet"))

    spark.stop()


if __name__ == "__main__":
    input_path = "/home/mountah_lodia/ecommerce_project/ecommerce_project/data/raw/data.csv"
    output_path = "/home/mountah_lodia/ecommerce_project/ecommerce_project/data"
    etl_pipeline(input_path, output_path)
