from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Key_Value_RDD")
sc = SparkContext(conf=conf)

'''
looking into fake social network data and figure out number of friends broken down by age
Key is age, value is number of friends
Instead of just a list of ages, or a list of number of friends, we can store (age, #friends)

Storing Key/Value pairs in RDD is like a giant NoSQL DB. Can aggrigate by key
'''

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("/home/vboxuser/SparkCourse/fakefriends.csv")
# work on each line by calling the function
rdd = lines.map(parseLine)

'''
first - alter the values, changing to a tuple
second - combine keys -> add number of friends + number of key occurance. Running total to quickly compute average
nothing happenes until the first action is called: reducebyKey
'''
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y:(x[0] + y[0], x[1] + y[1]))
# results: totalsByAge = (33, (351, 3)) --> averageByAge = (33, 117)
averageByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

'''
collect and display the results
'''

results = averageByAge.collect()

results.sort(key=lambda a: a[0])

for result in results:
    print(result)

