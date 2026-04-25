from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent / "data_lake"
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR = BASE_DIR / "gold"


def main():
    spark = SparkSession.builder \
        .appName("silver_to_gold") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Етап 3 читання таблиць із silver
    bio_df = spark.read.parquet(str(SILVER_DIR / "athlete_bio"))
    results_df = spark.read.parquet(str(SILVER_DIR / "athlete_event_results"))

    # Етап 3 підготовка біологічних даних
    bio_df = bio_df.select(
        F.col("athlete_id").cast("long").alias("athlete_id"),
        F.col("sex"),
        F.col("country_noc"),
        F.col("height").cast("double").alias("height"),
        F.col("weight").cast("double").alias("weight")
    )

    bio_df = bio_df.filter(
        F.col("height").between(100, 250)
        & F.col("weight").between(30, 250)
    )

    # Етап 3 підготовка результатів змагань
    results_df = results_df.select(
        F.col("athlete_id").cast("long").alias("athlete_id"),
        F.col("sport"),
        F.col("medal")
    )

    medal_clean = F.trim(F.col("medal"))

    results_df = results_df.withColumn(
        "medal",
        F.when(
            F.col("medal").isNull()
            | (medal_clean == "")
            | F.lower(medal_clean).isin("na", "nan", "none", "null"),
            F.lit("No medal")
        ).otherwise(medal_clean)
    )

    # Етап 3 join за athlete_id
    joined_df = results_df.join(bio_df, on="athlete_id", how="inner")

    # Етап 3 агрегація для gold layer
    gold_df = joined_df.groupBy(
        "sport",
        "medal",
        "sex",
        "country_noc"
    ).agg(
        F.round(F.avg("weight"), 2).alias("avg_weight"),
        F.round(F.avg("height"), 2).alias("avg_height")
    ).withColumn(
        "timestamp",
        F.current_timestamp()
    )

    # Етап 3 запис у gold
    output_path = GOLD_DIR / "avg_stats"
    gold_df.write.mode("overwrite").parquet(str(output_path))

    print("GOLD TABLE: avg_stats")
    print(f"Rows: {gold_df.count()}")
    gold_df.show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()