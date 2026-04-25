from pathlib import Path
import sys

from pyspark.sql import SparkSession, functions as F

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from config import kafka_config, jdbc_url, jdbc_user, jdbc_password

MYSQL_JAR = str(ROOT_DIR / "mysql-connector-j-8.0.32.jar")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
INPUT_TOPIC = "athlete_event_results_alina_n_v2"


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
    .appName("part_1_mysql_to_kafka") \
    .config("spark.jars", MYSQL_JAR) \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# читання результатів змагань з MySQL
events_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="(SELECT * FROM athlete_event_results LIMIT 10000) AS events",
    user=jdbc_user,
    password=jdbc_password
).load()

events_df = events_df.withColumn("athlete_id", F.col("athlete_id").cast("long"))

rows_count = events_df.count()
print(f"Зчитано рядків з MySQL: {rows_count}")

# запис результатів змагань у Kafka-топік
kafka_df = events_df.repartition(4).select(
    F.to_json(F.struct("*")).alias("value")
)

kafka_df.write \
    .format("kafka") \
    .options(**get_kafka_options()) \
    .option("topic", INPUT_TOPIC) \
    .save()

print(f"Записано рядків у Kafka топік {INPUT_TOPIC}: {rows_count}")

spark.stop()