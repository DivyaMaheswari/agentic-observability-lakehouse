from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

df = spark.read.csv("/opt/data/raw/UberDataset.csv", header = True)
df = df.withColumn("creation_timestamp", current_timestamp())
df.write.mode("overwrite").parquet("/opt/delta_tables/bronze/uber_data.parquet")
print(f"Bronze ingestion completed with {df.count()} records.")