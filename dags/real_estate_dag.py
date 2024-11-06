import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime
from scrap_and_stream import scrap_stream
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

load_dotenv()

package=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.0",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0",
        "org.apache.commons:commons-pool2:2.11.1",
        "org.mongodb:mongodb-driver-sync:5.2.0"]
with DAG(
    'dag_real_estate',
    start_date=datetime(2024, 11, 4),
    schedule_interval='@daily',
    catchup=False
) as dag:

    scrap_stream_task = PythonOperator(
        task_id='scrap_and_stream',
        python_callable=scrap_stream,
        op_kwargs={'topic': os.getenv("topic")}
    )

    data_dump_task = SparkSubmitOperator(
    task_id="pyspark_job",
    conn_id="spark-conn",
    application="dags/pyspark_job.py",
    packages=",".join(package)

)

    scrap_stream_task >> data_dump_task