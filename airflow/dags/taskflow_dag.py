from airflow import dag, task

from pyspark.sql import SparkSession
from pyspark.sql.functions import StructType, col
from pyspark.sql.types import *
import os
from urllib.request import urlopen
import json
from datetime import datetime

default_args = {"owner": "acgallagher"}


@dag(
    dag_id="main_dag_v01",
    default_args=default_args,
    start_date=datetime.today(),
    schedule_interval="5 * * * *",
    catchup=False,
)
def taskflow_dag():
    @task()
    def extract_monitoring_stations():
        os.system(
            "curl https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/2023/20230316/reporting_area_locations_V2.dat > /opt/data/reporting_area_locations_V2.dat"
        )

        spark = SparkSession.builder.master("local[*]").appName("spark").getOrCreate()

        schema = StructType(
            [
                StructField("Reporting area", StringType(), True),
                StructField("State code", StringType(), True),
                StructField("Country code", StringType(), True),
                StructField("Forecasts", StringType(), True),
                StructField("Action Day name", StringType(), True),
                StructField("Latitude", FloatType(), True),
                StructField("Longitude", FloatType(), True),
                StructField("GMT offset", FloatType(), True),
                StructField("Daylight savings", StringType(), True),
                StructField("Standard time zone label", StringType(), True),
                StructField("Daylight savings time zone label", StringType(), True),
                StructField("TWC code", StringType(), True),
                StructField("USA Today", StringType(), True),
                StructField("Forecast Source", StringType(), True),
            ]
        )

        station_df = (
            spark.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .schema(schema)
            .option("delimiter", "|")
            .load("/opt/data/reporting_area_locations_V2.dat")
        )

        station_df.write.mode("overwrite").parquet(
            "/opt/data/monitoringStations.parquet"
        )

    @task()
    def filter_pct_monitoring_stations():
        spark = SparkSession.builder.master("local[*]").appName("spark").getOrCreate()

        station_df = spark.read.parquet("/opt/data/monitoringStations.parquet")

        pct_stations_df = station_df.filter(
            (col("State code") == "CA")
            | (col("State code") == "OR")
            | (col("State code") == "WA")
        )

        pct_stations_df.write.mode("overwrite").parquet(
            "/opt/data/pctMonitoringStations.parquet"
        )

    @task()
    def extract_current_observations():
        spark = SparkSession.builder.master("local[*]").appName("spark").getOrCreate()

        pct_stations_df = spark.read.parquet("/opt/data/pctMonitoringStations.parquet")
        locations = pct_stations_df.select(
            "TWC code", "Latitude", "Longitude"
        ).collect()

        with open("opt/data/currentObservations.json", "w") as f:
            f.write("[\n")

        for row in locations:
            url = f"https://www.airnowapi.org/aq/observation/latLong/current/?format=application/json&latitude={round(row['Latitude'],4)}&longitude={round(row['Longitude'],4)}&date=2023-03-21&distance=1&API_KEY=407E7656-D069-4405-AF67-31DB45B36481"

            json_data = urlopen(url).read().decode("utf-8")
            json_data = json.loads(json_data)
            for entry in json_data:
                with open("opt/data/currentObservations.json", "a") as f:
                    entry = json.dumps(entry) + ",\n"
                    f.write(entry)

        with open("opt/data/currentObservations.json", "a") as f:
            f.write("{}\n")
            f.write("]")

        schema = StructType(
            [
                StructField("DateObserved", StringType(), True),
                StructField("HourObserved", StringType(), True),
                StructField("LocalTimeZone", StringType(), True),
                StructField("ReportingArea", StringType(), True),
                StructField("StateCode", StringType(), True),
                StructField("Latitude", FloatType(), True),
                StructField("Longitude", FloatType(), True),
                StructField("ParameterName", StringType(), True),
                StructField("AQI", FloatType(), True),
            ]
        )

        currentObservations_df = (
            spark.read.option("multiline", "true")
            .schema(schema)
            .json("opt/data/currentObservations.json")
        )

        currentObservations_df.write.mode("overwrite").parquet(
            "opt/data/currentObservations.parquet"
        )

    @task()
    def extract_forecasts():
        spark = SparkSession.builder.master("local[*]").appName("spark").getOrCreate()

        pct_stations_df = spark.read.parquet("/opt/data/pctMonitoringStations.parquet")
        locations = pct_stations_df.select(
            "TWC code", "Latitude", "Longitude"
        ).collect()

        with open("opt/data/forecasts.json", "w") as f:
            f.write("[\n")

        for row in locations:
            url = f"https://www.airnowapi.org/aq/forecast/latLong/?format=application/json&latitude={round(row['Latitude'],4)}&longitude={round(row['Longitude'],4)}&date=2023-03-21&distance=1&API_KEY=407E7656-D069-4405-AF67-31DB45B36481"

            json_data = urlopen(url).read().decode("utf-8")
            json_data = json.loads(json_data)
            for entry in json_data:
                with open("opt/data/forecasts.json", "a") as f:
                    entry = json.dumps(entry) + ",\n"
                    f.write(entry)

        with open("opt/data/forecasts.json", "a") as f:
            f.write("{}\n")
            f.write("]")

        schema = StructType(
            [
                StructField("DateIssue", StringType(), True),
                StructField("DateForecast", StringType(), True),
                StructField("ReportingArea", StringType(), True),
                StructField("StateCode", StringType(), True),
                StructField("Latitude", FloatType(), True),
                StructField("Longitude", FloatType(), True),
                StructField("ParameterName", StringType(), True),
                StructField("AQI", FloatType(), True),
            ]
        )

        forecasts_df = (
            spark.read.option("multiline", "true")
            .schema(schema)
            .json("opt/data/forecasts.json")
        )

        forecasts_df.write.mode("overwrite").parquet("opt/data/forecasts.parquet")

    extract_monitoring_stations()
    filter_pct_monitoring_stations()
    extract_current_observations()
    extract_forecasts()


taskflow_dag()
