from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

from urllib.request import urlopen
import json

if __name__ == "__main__":

    spark = SparkSession.builder.appName("transformForecasts").getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

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
        .json(input_path, schema=schema)
    )

    forecasts_df.write.mode("overwrite").parquet(output_path)

    spark.stop()
