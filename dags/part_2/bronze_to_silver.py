import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import StringType

TABLES = ["athlete_bio", "athlete_event_results"]

BASE_DIR = Path(__file__).resolve().parent / "data_lake"
BRONZE_DIR = BASE_DIR / "bronze"
SILVER_DIR = BASE_DIR / "silver"


def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', "", str(text))


clean_text_udf = udf(clean_text, StringType())


def main():
    spark = SparkSession.builder \
        .appName("bronze_to_silver") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        # Етап 2 читання таблиці з bronze
        input_path = BRONZE_DIR / table
        df = spark.read.parquet(str(input_path))

        # Етап 2 очищення текстових колонок
        text_columns = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StringType)
        ]

        for col_name in text_columns:
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))
            df = df.withColumn(col_name, trim(col(col_name)))

        # Етап 2 дедублікація рядків
        df = df.dropDuplicates()

        # Етап 2 запис у silver
        output_path = SILVER_DIR / table
        df.write.mode("overwrite").parquet(str(output_path))

        print(f"SILVER TABLE: {table}")
        print(f"Rows: {df.count()}")
        df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()