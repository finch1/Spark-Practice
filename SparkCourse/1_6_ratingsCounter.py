from pyspark import SparkConf, SparkContext
import collections
import os

'''
set the Master node as the local machine on a single thread, single process
set app name to see the results
'''
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

print("Working Directory: " + os.getcwd())

'''
load data file. textFile method breaks down the data line by line so each line corresponds to one string value in the RDD
lines is iterated to a lambda function. the line is split by delimiter and 3rd column is extracted. In this case, its the rating of the movie, from 1 to 5 stars
the map results is assigned to a new RDD caled ratings
count by value groups the ratings and counts their occurance
link to dataset:https://files.grouplens.org/datasets/movielens/ml-100k.zip
'''
lines = sc.textFile("/home/vboxuser/SparkCourse/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
results = ratings.countByValue()

'''
sort Results by key and print
'''
sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

'''
To execute in terminal, type spark-submit 1_6_ratingsCounter.py
'''