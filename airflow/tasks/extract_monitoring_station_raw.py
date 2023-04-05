import os


def extract_monitoring_station_raw():
    os.system(
        "curl https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2023/20230316/reporting_area_locations_V2.dat > /opt/airflow/data/reporting_area_locations_V2.dat"
    )


if __name__ == "__main__":
    extract_monitoring_station_raw()
