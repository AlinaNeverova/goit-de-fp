from pathlib import Path
import sys

from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from config import jdbc_url, jdbc_user, jdbc_password

MYSQL_JAR = str(ROOT_DIR / "mysql-connector-j-8.0.32.jar")

OUTPUT_TABLE = "athlete_stats_enriched_alina_n_v2"

spark = SparkSession.builder \
    .appName("part_1_check_mysql_output") \
    .config("spark.jars", MYSQL_JAR) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Перевірка результатів у MySQL
query = f"""
(
    SELECT *
    FROM {OUTPUT_TABLE}
    ORDER BY calculation_timestamp DESC
    LIMIT 30
) AS result_table
"""

result_df = spark.read.format("jdbc").options(
    url=jdbc_url,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=query,
    user=jdbc_user,
    password=jdbc_password
).load()

result_df.show(30, truncate=False)

spark.stop()