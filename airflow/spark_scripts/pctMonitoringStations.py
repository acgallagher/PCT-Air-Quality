from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import StructType, col
import sys

if __name__ == "__main__":

    spark = SparkSession.builder.appName("run_monitors_spark_job").getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

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
        .load(input_path)
    )

    pct_df = station_df.filter(
        (col("State code") == "CA")
        | (col("State code") == "OR")
        | (col("State code") == "WA")
    )

    pct_df.write.mode("overwrite").parquet(output_path)

    spark.stop()
