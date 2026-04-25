from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "alina",
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="alina_part_2_datalake_etl",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["goit", "final_project", "part_2"],
) as dag:

    base_path = "/opt/airflow/dags/part_2"

    common_conf = {
        "spark.driver.memory": "1g",
        "spark.executor.memory": "1g",
        "spark.sql.shuffle.partitions": "4",
    }

    common_env = {
        "ROW_LIMIT": "10000",
    }

    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{base_path}/landing_to_bronze.py",
        name="landing_to_bronze",
        conn_id="spark_default",
        conf=common_conf,
        env_vars=common_env,
        verbose=True,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{base_path}/bronze_to_silver.py",
        name="bronze_to_silver",
        conn_id="spark_default",
        conf=common_conf,
        env_vars=common_env,
        verbose=True,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{base_path}/silver_to_gold.py",
        name="silver_to_gold",
        conn_id="spark_default",
        conf=common_conf,
        env_vars=common_env,
        verbose=True,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold