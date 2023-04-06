from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

from urllib.request import urlopen
import json

if __name__ == "__main__":

    spark = SparkSession.builder.appName("extractForecasts").getOrCreate()

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    pct_df = spark.read.load(input_path)
    locations = pct_df.select("TWC code", "Latitude", "Longitude").collect()

    with open(output_path, "w") as f:
        f.write("[\n")

    for row in locations:
        url = f"https://www.airnowapi.org/aq/forecast/latLong/?format=application/json&latitude={round(row['Latitude'],4)}&longitude={round(row['Longitude'],4)}&date=2023-03-21&distance=1&API_KEY=407E7656-D069-4405-AF67-31DB45B36481"

        json_data = urlopen(url).read().decode("utf-8")
        json_data = json.loads(json_data)
        for entry in json_data:
            with open(output_path, "a") as f:
                entry = json.dumps(entry) + ",\n"
                f.write(entry)

    with open(output_path, "a") as f:
        f.write("{}\n")
        f.write("]")

    spark.stop()
