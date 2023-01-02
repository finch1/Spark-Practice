from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("tempreture dataframe").getOrCreate()

schema = StructType([
                        StructField("stationsID",StringType(),True),
                        StructField("date",IntegerType(),True),
                        StructField("measure_type",StringType(),True),
                        StructField("temp",FloatType(),True)
])

# Read file as dataframe
df = spark.read.schema(schema=schema).csv("/home/vboxuser/SparkCourse/1800.csv")
print(df.printSchema())

# filter out all TMIN entires
df_min = df.filter(df.measure_type == "TMIN")

# Select only stationID and tempreture
stationTemp = df_min.select("stationsID", "temp")

# Aggregate to find min temp of every station
minTempByStation = stationTemp.groupBy("stationsID").min("temp")
minTempByStation.show()

# Convert temp
# withColumn creates a new columns "temperature"
# then select the station ID and new tempreture columns 
minTempByStationC = minTempByStation.withColumn("temperature", func.round(func.col("min(temp)")*0.1, 2)).select("stationsID", "temperature").sort("temperature")

# collect and print results
results = minTempByStationC.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))

spark.stop()