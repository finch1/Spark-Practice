from pyspark import SparkConf, SparkContext
import re
import collections

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) # split words, eliminate punctuation

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/vboxuser/SparkCourse/book.txt")

rdd = lines.flatMap(normalizeWords)
wordCount = rdd.countByValue()

for word, count in wordCount.items():
    cleanWord = word.encode('ascii', 'ignore') # removes bad characters
    if cleanWord:
        print(cleanWord, count)
