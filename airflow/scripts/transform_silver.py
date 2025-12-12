from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, to_date
from pyspark.sql.types import IntegerType, DoubleType
import os

def parse_invoice_date(df):
    """
    Harmonise la colonne InvoiceDate en date uniquement, en ignorant l'heure.
    Compatible avec les anciens fichiers Parquet où InvoiceDate peut être BINARY.
    """
    # Forcer lecture en string
    df = df.withColumn("InvoiceDate", col("InvoiceDate").cast("string"))
    # Extraire uniquement la date (10 premiers caractères) et convertir en type date
    df = df.withColumn("InvoiceDate", to_date(col("InvoiceDate").substr(1, 10), "yyyy-MM-dd"))
    return df

def run_transform(input_path, output_path, start_date=None):
    spark = SparkSession.builder.appName("TransformSilver").getOrCreate()

    # Charger tous les fichiers Bronze correspondant au format bronze_*.parquet
    bronze_dir = f"{input_path}/bronze"
    bronze_files = [f"{bronze_dir}/{f}" for f in os.listdir(bronze_dir)
                    if f.startswith("bronze_") and f.endswith(".parquet")]

    if not bronze_files:
        print(f"[WARN] Aucun fichier Parquet trouvé dans {bronze_dir}")
        spark.stop()
        return

    df = spark.read.option("mergeSchema", "true").parquet(*bronze_files)

    # Parsing des dates
    df = parse_invoice_date(df)

    # Filtrage incrémental si start_date fourni
    if start_date:
        df = df.filter(col("InvoiceDate").isNotNull() & (col("InvoiceDate") >= start_date))

    # Casting
    df = df.withColumn("Quantity", col("Quantity").cast(IntegerType()))
    df = df.withColumn("UnitPrice", col("UnitPrice").cast(DoubleType()))

    # Calculs
    df = df.withColumn("line_total", col("Quantity") * col("UnitPrice"))
    df = df.withColumn("is_return", when(col("InvoiceNo").startswith("C"), lit(1)).otherwise(0))

    # Nettoyage
    df_clean = df.filter(
        (col("Quantity") > 0) &
        (col("UnitPrice") > 0) &
        (col("CustomerID").isNotNull())
    )

    # Sauvegarde Silver
    os.makedirs(f"{output_path}/silver", exist_ok=True)

    # Nom de fichier basé sur la date passée en argument si fournie
    if start_date:
        date_str = start_date.replace("-", "")  # 2011-02-04 -> 20110204
    else:
        from datetime import datetime
        date_str = datetime.now().strftime("%Y%m%d")

    parquet_path = f"{output_path}/silver/cleaned_ecommerce_{date_str}.parquet"

    df_clean.write.mode("overwrite").parquet(parquet_path)
    print(f"[INFO] Silver généré : {parquet_path}")

    spark.stop()

if __name__ == "__main__":
    import sys
    start_date = sys.argv[3] if len(sys.argv) > 3 else None
    run_transform(sys.argv[1], sys.argv[2], start_date)
