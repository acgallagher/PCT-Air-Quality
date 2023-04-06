import boto3
import os
from dotenv import dotenv_values

# load config
config = dotenv_values("/opt/airflow/configs/configs.env")


def load_spark_scripts():
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
    )

    for file in os.listdir("/opt/airflow/spark_scripts"):
        s3.meta.client.upload_file(
            Filename=f"/opt/airflow/spark_scripts/{file}",
            Bucket="pct-air-quality",
            Key=f"spark_scripts/{file}",
        )


if __name__ == "__main__":
    load_spark_scripts()
