from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import boto3

default_args = {"owner": "acgallagher"}


@dag(
    dag_id="main_dag_v09.11",
    default_args=default_args,
    start_date=datetime.today(),
    # schedule_interval="5 * * * *",
    catchup=False,
)
def main_dag():

    extract_monitoring_station_raw = BashOperator(
        task_id="extract_monitoring_station_raw",
        bash_command="python /opt/airflow/tasks/extract_monitoring_station_raw.py",
    )

    load_monitoring_station_raw_s3 = BashOperator(
        task_id="load_monitoring_station_raw_s3",
        bash_command="python /opt/airflow/tasks/load_monitoring_station_raw_s3.py",
    )

    load_spark_scripts = BashOperator(
        task_id="load_spark_scripts",
        bash_command="python /opt/airflow/tasks/load_spark_scripts.py",
    )

    run_monitors_spark_job = BashOperator(
        task_id="run_monitors_spark_job",
        bash_command="python /opt/airflow/tasks/emr_serverless.py \
            --job-role-arn arn:aws:iam::188237326080:role/AmazonEMR-ExecutionRole-1680804797888 \
            --s3-bucket pct-air-quality \
            --script pctMonitoringStations.py \
            --input-location data/stations/monitoring_station_raw.dat \
            --output-location data/stations/pctMonitoringStations.parquet/",
    )

    (
        extract_monitoring_station_raw
        >> load_monitoring_station_raw_s3
        >> load_spark_scripts
        >> run_monitors_spark_job
    )


main_dag()
