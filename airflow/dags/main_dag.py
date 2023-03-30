from airflow.decorators import dag, task
from datetime import datetime
import os
import boto3

default_args = {"owner": "acgallagher"}


@dag(
    dag_id="main_dag_v06",
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval="5 * * * *",
    catchup=False,
)
def main_dag():
    @task()
    def set_aws_keys():
        os.environ["AWS_ACCESS_KEY"] = "AKIASXU6RU4ADYBMFV6V"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "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"

    @task()
    def extract_monitoring_station_raw():
        os.system(
            "curl https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2023/20230316/reporting_area_locations_V2.dat > /opt/airflow/data/reporting_area_locations_V2.dat"
        )

    @task()
    def load_monitoring_station_raw_s3():
        s3 = boto3.resource(
            "s3",
            aws_access_key_id="AKIASXU6RU4ADYBMFV6V",
            aws_secret_access_key="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",
        )

        s3.meta.client.upload_file(
            Filename="/opt/airflow/data/reporting_area_locations_V2.dat",
            Bucket="pct-air-quality",
            Key="data/stations/monitoring_station_raw.dat",
        )

    @task()
    def load_spark_scripts():
        s3 = boto3.resource(
            "s3",
            aws_access_key_id="AKIASXU6RU4ADYBMFV6V",
            aws_secret_access_key="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",
        )
        for file in os.listdir("/opt/airflow/scripts"):
            s3.meta.client.upload_file(
                Filename=f"/opt/airflow/scripts/{file}",
                Bucket="pct-air-quality",
                Key=f"scripts/{file}",
            )

    @task()
    def run_spark_job(script: str, *args):
        """
        EMR Serverless spark job template
        https://github.com/aws-samples/emr-serverless-samples/tree/main/examples/python-api
        """
        emr_serverless_client = boto3.client(
            "emr-serverless",
            region_name="us-east-1",
            aws_access_key_id="AKIASXU6RU4ADYBMFV6V",
            aws_secret_access_key="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",
        )

        response = emr_serverless_client.create_application(
            name="pct-aq-spark", releaseLabel="emr-6.6.0", type="SPARK"
        )

        print(
            "Created application {name} with application id {applicationId}. Arn: {arn}".format_map(
                response
            )
        )

        applicationId = response["applicationId"]

        emr_serverless_client.start_application(applicationId=applicationId)

        try:
            response = emr_serverless_client.start_job_run(
                applicationId=applicationId,
                executionRoleArn="arn:aws:iam::188237326080:role/EMRServerlessS3RuntimeRole",
                jobDriver={
                    "sparkSubmit": {
                        "entryPoint": f"s3://pct-air-quality/scripts/{script}",
                        "entryPointArguments": args,
                        "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
                    }
                },
            )

            job_run_id = response["jobRunId"]

        finally:
            # Shut down and delete your application
            """
            job_done = False
            while not job_done:
                jr_response = emr_serverless_client.get_job_run(
                    applicationId=applicationId, jobRunId=job_run_id
                )
                job_done = jr_response.get("state") in [
                    "SUCCESS",
                    "FAILED",
                    "CANCELLING",
                    "CANCELLED",
                ]
            """

            emr_serverless_client.stop_application(applicationId=applicationId)
            emr_serverless_client.delete_application(applicationId=applicationId)

    task1 = set_aws_keys()

    task2 = extract_monitoring_station_raw()

    task3 = load_monitoring_station_raw_s3()

    task4 = load_spark_scripts()

    task5 = run_spark_job(
        "pctMonitoringStations.py",
        "s3://pct-air-quality/data/stations/monitoring_station_raw.dat",
        "s3://pct-air-quality/data/stations/pctMonitoringStations.parquet/",
    )

    task1 >> task2 >> task3 >> task4 >> task5


main_dag()
