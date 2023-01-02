from pyspark.sql import SparkSession, functions as func

spark = SparkSession.builder.appName("word count DF").getOrCreate()

# read each line of the book into the DF
inputDF = spark.read.text("/home/vboxuser/SparkCourse/book.txt")

# split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words = words.filter(words.word != "")

# normalize everything to lower case
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# count the occurances of each word
wordsCountSorted = lowercaseWords.groupBy("word").count().sort("count")

# show results
wordsCountSorted.show(wordsCountSorted.count())

spark.stop()