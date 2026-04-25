from pathlib import Path

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent / "data_lake"

spark = SparkSession.builder \
    .appName("check_part_2_outputs") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

gold_path = BASE_DIR / "gold" / "avg_stats"

gold_df = spark.read.parquet(str(gold_path))

print("FINAL GOLD TABLE: avg_stats")
print(f"Rows: {gold_df.count()}")

gold_df.show(30, truncate=False)

spark.stop()