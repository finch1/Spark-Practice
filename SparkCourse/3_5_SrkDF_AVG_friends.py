from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("avg friends").getOrCreate()

# having headers make it easier to extract data and infer schema
people = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/vboxuser/SparkCourse/fakefriendsHeader.csv")

print("Here is our infered schema")
people.printSchema()

print("Group by age")
people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friendsAvg")).orderBy("age").show(100)

spark.stop()