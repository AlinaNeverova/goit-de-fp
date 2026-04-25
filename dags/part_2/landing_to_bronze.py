import os
from pathlib import Path

import requests
from pyspark.sql import SparkSession

BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]

BASE_DIR = Path(__file__).resolve().parent / "data_lake"
LANDING_DIR = BASE_DIR / "landing"
BRONZE_DIR = BASE_DIR / "bronze"

ROW_LIMIT = int(os.getenv("ROW_LIMIT", "0"))


def download_data(table_name):
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    file_path = LANDING_DIR / f"{table_name}.csv"
    url = f"{BASE_URL}{table_name}.csv"

    print(f"Завантаження файлу: {url}")

    response = requests.get(url, timeout=60)

    if response.status_code != 200:
        raise Exception(f"Не вдалося завантажити файл. Status code: {response.status_code}")

    with open(file_path, "wb") as file:
        file.write(response.content)

    print(f"Файл збережено: {file_path}")
    return file_path


def main():
    spark = SparkSession.builder \
        .appName("landing_to_bronze") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        # Етап 1 завантаження CSV у landing zone
        csv_path = download_data(table)

        # Етап 1 читання CSV через Spark
        df = spark.read.csv(
            str(csv_path),
            header=True,
            inferSchema=True
        )

        if ROW_LIMIT > 0:
            df = df.limit(ROW_LIMIT)

        # Етап 1 запис у bronze у форматі parquet
        output_path = BRONZE_DIR / table

        df.write.mode("overwrite").parquet(str(output_path))

        print(f"BRONZE TABLE: {table}")
        print(f"Rows: {df.count()}")
        df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()