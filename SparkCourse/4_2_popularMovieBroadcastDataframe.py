from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def load_movie_names():
    movieNames = {}
    with codecs.open("/home/vboxuser/SparkCourse/ml-100k/u.item", 'r', encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("popular movies broadcast").getOrCreate()

namedict = spark.sparkContext.broadcast(load_movie_names())

# Create schema when reading data
schema = StructType([
    StructField("userID", IntegerType(), True),
    StructField("movieID", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load up movie data as dataframe
moviesDF = spark.read.option("sep", "\t").schema(schema=schema).csv("/home/vboxuser/SparkCourse/ml-100k/u.data")

# sort movies by popularity in one line
topMoviesID = moviesDF.groupBy("movieID").count()

# Create a user defined function to look up movie names from the broadcasted dictionary
def lookup_name(movieID):
    return namedict.value[movieID]

# this way how we convert user defined functions into spark usable functions. 
lookupNameUDF = func.udf(lookup_name)

# add movie title column. The movieID is first passed to the sparkFunction and the result output is the movie title. this line loops over the dataframe. 
moviesWithNames = topMoviesID.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

# sort the results
sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

sortedMoviesWithNames.show(truncate=False)

# stop session
spark.stop()