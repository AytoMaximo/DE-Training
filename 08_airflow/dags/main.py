from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import DataFrame, SparkSession


def run_spark_job() -> None:
    spark: SparkSession = SparkSession.builder \
        .appName("Airflow_PySpark_Example") \
        .master("local") \
        .getOrCreate()

    data: list[tuple[str, int]] = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]

    df: DataFrame = spark.createDataFrame(data, ["name", "id"])

    df.show()
    spark.stop()


with DAG(
        dag_id="main",
        start_date=datetime(2023, 10, 1),
        schedule_interval="@daily",
        catchup=False,
) as dag:
    run_pyspark_task: PythonOperator = PythonOperator(
        task_id="run_pyspark_task",
        python_callable=run_spark_job,
    )

run_pyspark_task
