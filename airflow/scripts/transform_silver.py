from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType
import os

def run_transform(input_path, output_path):
    spark = SparkSession.builder.appName("TransformSilver").getOrCreate()

    # Lecture Bronze
    df = spark.read.csv(f"{input_path}/bronze/data.csv")

    # Casting des colonnes
    df = df.withColumn('Quantity', col('Quantity').cast(IntegerType()))
    df = df.withColumn('UnitPrice', col('UnitPrice').cast(DoubleType()))
    df = df.withColumn('line_total', col('Quantity') * col('UnitPrice'))
    df = df.withColumn('is_return', when(col('InvoiceNo').startswith("C"), lit(1)).otherwise(0))

    # Filtrage des lignes invalides
    df_clean = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0) &
        (col("CustomerID").isNotNull())
    )

    # Conversion sécurisée des dates
    df_clean = df_clean.withColumn(
        "InvoiceDate",
        to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
    )
    df_clean = df_clean.withColumn("InvoiceDate", col("InvoiceDate").cast("date"))

    # Création du dossier silver si inexistant
    os.makedirs(f"{output_path}/silver", exist_ok=True)

    # Sauvegarde Silver
    df_clean.write.mode("overwrite").parquet(f"{output_path}/silver/cleaned_ecommerce_data.parquet")
    print(f"[INFO] Transformation Silver terminée avec succès : {output_path}/silver/cleaned_ecommerce_data.parquet")

    spark.stop()


if __name__ == "__main__":
    import sys
    run_transform(sys.argv[1], sys.argv[2])
