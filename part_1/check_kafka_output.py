from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from config import kafka_config

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

OUTPUT_TOPIC = "athlete_stats_enriched_alina_n_v2"


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
    .appName("part_1_check_kafka_output") \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Перевірка результатів з вихідного Kafka топіку
kafka_df = spark.read \
    .format("kafka") \
    .options(**get_kafka_options()) \
    .option("subscribe", OUTPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

schema = StructType([
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("avg_height", DoubleType(), True),
    StructField("avg_weight", DoubleType(), True),
    StructField("calculation_timestamp", StringType(), True),
])

result_df = kafka_df \
    .selectExpr("CAST(value AS STRING) AS json_value") \
    .select(F.from_json("json_value", schema).alias("data")) \
    .select("data.*")

result_df.show(30, truncate=False)

spark.stop()