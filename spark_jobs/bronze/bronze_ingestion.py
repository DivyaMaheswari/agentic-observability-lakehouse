from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = (
    SparkSession.builder
    .appName("Bronze Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


df = spark.read.csv("/opt/data/raw/UberDataset.csv", header = True)
df = df.withColumn("creation_timestamp", current_timestamp())
df.write.format("delta") \
    .mode("overwrite") \
    .save("/opt/delta_tables/bronze/trips")

print("✅ Bronze ingestion written as Delta Table successfully!")