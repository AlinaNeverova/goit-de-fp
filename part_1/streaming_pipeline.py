from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from config import kafka_config, jdbc_url, jdbc_user, jdbc_password

MYSQL_JAR = str(ROOT_DIR / "mysql-connector-j-8.0.32.jar")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

INPUT_TOPIC = "athlete_event_results_alina_n_v2"
OUTPUT_TOPIC = "athlete_stats_enriched_alina_n_v2"
OUTPUT_TABLE = "athlete_stats_enriched_alina_n_v2"

CHECKPOINT_DIR = str(ROOT_DIR / "checkpoints" / "part_1_streaming")


def get_kafka_options():
    return {
        "kafka.bootstrap.servers": ",".join(kafka_config["bootstrap_servers"]),
        "kafka.security.protocol": kafka_config["security_protocol"],
        "kafka.sasl.mechanism": kafka_config["sasl_mechanism"],
        "kafka.sasl.jaas.config": (
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        ),
    }


spark = SparkSession.builder \
    .appName("part_1_streaming_pipeline") \
    .config("spark.jars", MYSQL_JAR) \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Етап 1 читання фізичних даних атлетів з MySQL
athlete_bio_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()

# Етап 2 фільтрація порожнього та некоректного зросту і ваги
athlete_bio_clean_df = athlete_bio_df \
    .withColumn("height_str", F.trim(F.col("height").cast("string"))) \
    .withColumn("weight_str", F.trim(F.col("weight").cast("string"))) \
    .filter(
        F.col("height_str").rlike(r"^[0-9]+(\.[0-9]+)?$")
        & F.col("weight_str").rlike(r"^[0-9]+(\.[0-9]+)?$")
    ) \
    .select(
        F.col("athlete_id").cast("long").alias("athlete_id"),
        F.col("sex"),
        F.col("country_noc"),
        F.col("height_str").cast("double").alias("height"),
        F.col("weight_str").cast("double").alias("weight")
    )

# Етап 3 читання результатів змагань з Kafka топіку
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .options(**get_kafka_options()) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

event_schema = StructType([
    StructField("athlete_id", LongType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
])

# Етап 3 перетворення JSON у DataFrame
events_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) AS json_value") \
    .select(F.from_json("json_value", event_schema).alias("data")) \
    .select("data.*") \
    .filter(F.col("athlete_id").isNotNull()) \
    .filter(F.col("sport").isNotNull())

medal_clean = F.trim(F.col("medal"))

events_stream_df = events_stream_df.withColumn(
    "medal",
    F.when(
        F.col("medal").isNull()
        | (medal_clean == "")
        | F.lower(medal_clean).isin("na", "none", "null"),
        F.lit("No medal")
    ).otherwise(medal_clean)
)

# Етап 4 об'єднання Kafka даних з біологічними даними
enriched_stream_df = events_stream_df.join(
    athlete_bio_clean_df,
    on="athlete_id",
    how="inner"
)


def write_batch(batch_df, batch_id):
    # Етап 5 розрахунок середнього зросту і ваги
    stats_df = batch_df.groupBy(
        "sport",
        "medal",
        "sex",
        "country_noc"
    ).agg(
        F.round(F.avg("height"), 2).alias("avg_height"),
        F.round(F.avg("weight"), 2).alias("avg_weight")
    ).withColumn(
        "calculation_timestamp",
        F.current_timestamp()
    ).cache()

    rows_count = stats_df.count()

    if rows_count == 0:
        print(f"Batch {batch_id}: немає даних")
        stats_df.unpersist()
        return

    print(f"Batch {batch_id}: розраховано рядків: {rows_count}")
    stats_df.show(20, truncate=False)

    # Етап 6 запис у вихідний Kafka-топік
    stats_df.select(
        F.to_json(F.struct("*")).alias("value")
    ).write \
        .format("kafka") \
        .options(**get_kafka_options()) \
        .option("topic", OUTPUT_TOPIC) \
        .save()

    # Етап 6 запис у MySQL
    stats_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", OUTPUT_TABLE) \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

    print(f"Batch {batch_id}: записано у Kafka і MySQL")

    stats_df.unpersist()


# Етап 6 запуск стримінгу через foreachBatch
query = enriched_stream_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(availableNow=True) \
    .start()

query.awaitTermination()

print("Стримінг завершено")

spark.stop()