from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("wordcountsorted")
sc = SparkContext(conf=conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("/home/vboxuser/SparkCourse/book.txt")
words = input.flatMap(normalizeWords)

# CountByValue but done by hand
# for every unique word, add it together to increment the count
# keys are the unique words and the values are the counts
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x,y: (y,x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word + ':\t\t' + count)