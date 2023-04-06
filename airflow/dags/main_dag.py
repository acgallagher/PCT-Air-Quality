from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {"owner": "acgallagher"}


@dag(
    dag_id="main_dag_v0.0.1",
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

    extract_forecasts = BashOperator(
        task_id="extract_forecasts",
        bash_command="python /opt/airflow/tasks/emr_serverless.py \
            --job-role-arn arn:aws:iam::188237326080:role/AmazonEMR-ExecutionRole-1680804797888 \
            --s3-bucket pct-air-quality \
            --script extractForecasts.py \
            --input-location data/stations/pctMonitoringStations.parquet \
            --output-location data/forecasts/forecasts.json",
    )

    transform_forecasts = BashOperator(
        task_id="transform_forecasts",
        bash_command="python /opt/airflow/tasks/emr_serverless.py \
            --job-role-arn arn:aws:iam::188237326080:role/AmazonEMR-ExecutionRole-1680804797888 \
            --s3-bucket pct-air-quality \
            --script transformForecasts.py \
            --input-location data/forecasts/forecasts.json \
            --output-location data/forecasts/forecasts.parquet/",
    )

    extract_current_observations = BashOperator(
        task_id="extract_current_observations",
        bash_command="python /opt/airflow/tasks/emr_serverless.py \
            --job-role-arn arn:aws:iam::188237326080:role/AmazonEMR-ExecutionRole-1680804797888 \
            --s3-bucket pct-air-quality \
            --script extractCurrentObservations.py \
            --input-location data/stations/pctMonitoringStations.parquet \
            --output-location data/observations/currentObservations.json",
    )

    transform_current_observations = BashOperator(
        task_id="transform_current_observations",
        bash_command="python /opt/airflow/tasks/emr_serverless.py \
            --job-role-arn arn:aws:iam::188237326080:role/AmazonEMR-ExecutionRole-1680804797888 \
            --s3-bucket pct-air-quality \
            --script transformCurrentObservations.py \
            --input-location data/observations/observations.json \
            --output-location data/observations/observations.parquet/",
    )

    (
        extract_monitoring_station_raw
        >> load_monitoring_station_raw_s3
        >> load_spark_scripts
        >> run_monitors_spark_job
        >> extract_current_observations
        >> transform_current_observations
        >> extract_forecasts
        >> transform_forecasts
    )


main_dag()
