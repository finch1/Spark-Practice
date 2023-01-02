from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("quickMaps")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/vboxuser/SparkCourse/string.txt")

results = lines.map(lambda x: x.upper())

for up in results.collect():
    print("UPPER: " + up)

results = lines.flatMap(lambda x: x.split())

for splitting in results.collect():
    print("SPLIT: " + splitting)