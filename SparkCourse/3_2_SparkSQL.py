from pyspark.sql import SparkSession, Row

# create a sparksession
spark = SparkSession.builder.appName("firstSparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID = int(fields[0]), name = str(fields[1]).encode("utf-8"), age = int(fields[2]), numOfFriends = int(fields[3]))

lines = spark.sparkContext.textFile("/home/vboxuser/SparkCourse/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema and register the dataframe as a table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table
teenagers = spark.sql("select * from people where age between 13 and 19")

# The results of SQL queries are RDDs and support all the normal RDD operations
for teen in teenagers.collect():
    print(teen)

# We can also use functions instead of SQL queries
schemaPeople.groupBy("age").count().orderBy("age").show()

# stop session
spark.stop()