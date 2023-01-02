from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark SQL CSV").getOrCreate()

# having headers make it easier to extract data and infer schema
people = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/vboxuser/SparkCourse/fakefriendsHeader.csv")

print("Here is our infered schema")
people.printSchema()

print("Display the name column")
people.select("name").show()

print("Filter anyone over 21")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("Make everyone 10 year older")
people.select(people.age, people.age + 10).show()

spark.stop()