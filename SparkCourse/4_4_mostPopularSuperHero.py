from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("popular super hero").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create schema when reading data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.schema(schema=schema).option("sep", " ").csv("/home/vboxuser/SparkCourse/marvelNames.txt")
lines = spark.read.text("/home/vboxuser/SparkCourse/marvelGraph.txt")

names.show(10)
lines.show(10)

# data is defined in each line in the text file. the first column is the character ID. The rest are its relations.
# split the line by space and take the zeroth match as the character
# split again, remove the last (right) space as its being counted as a connection.
# count the connections by len of array split
# aggregate each character
connections = lines.withColumn("id", func.split(func.col('value'), " ")[0]).withColumn("connections", func.size(func.split(func.rtrim(func.col('value')), " "))-1).groupBy("id").agg(func.sum("connections").alias("connections"))

connections.show(10)

mostPopular = connections.sort(func.col("connections").desc()).first() # returns row type
print(mostPopular)

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first() # returns row type
print(mostPopularName)

print()
print(f"{mostPopularName[0]} has {mostPopular[1]} apperances.")

