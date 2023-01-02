from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("popular movies").getOrCreate()

# Create schema when reading data
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("teimestamp", LongType(), True)
])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema=schema).csv("/home/vboxuser/SparkCourse/ml-100k/u.data")

# sort movies by popularity in one line
topMoviesID = moviesDF.groupBy("movieID").count().orderBy(func.desc("count")) # sort desc by number of times the movie was mentioned

# grab top 10
topMoviesID.show(10)

# stop session
spark.stop()