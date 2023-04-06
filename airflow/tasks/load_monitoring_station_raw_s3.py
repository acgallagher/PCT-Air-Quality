import boto3
from dotenv import dotenv_values

# load config
config = dotenv_values("/opt/airflow/configs/configs.env")


def load_monitoring_station_raw_s3():
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
    )

    s3.meta.client.upload_file(
        Filename="/opt/airflow/data/reporting_area_locations_V2.dat",
        Bucket="pct-air-quality",
        Key="data/stations/monitoring_station_raw.dat",
    )


if __name__ == "__main__":
    load_monitoring_station_raw_s3()
