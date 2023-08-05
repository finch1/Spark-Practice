# Installing Spark on Ubuntu
https://sparkbyexamples.com/spark/spark-installation-on-linux-ubuntu

Apache Spark Installation on Ubuntu
Use wget command to download the Apache Spark to your Ubuntu server
Once your download is complete, rename the folder to Spark
<br></br>

# What is Spark
Spark scripts can be in Python, Java or Scala. Scripts are run by the driver and distributed. Spark can run on top of Hadoop cluster. Spark doesn't so anything until its told to. It uses DAG to optimize tasks.

<div>
    <img src="Fig A.png" style="max-width: 30%;"/>        
</div>
<br></br>

# What is RDD
Resilient Distributed Dataset is fundumentaly a dataset and it's an abstraction for a giant set of data. Need to know how to set up RDD objects and loading them up with big data sets and then calling various methods on the RDD object to distribute the processing of that data. The resilience and distribution is managed by the cluster manager. So RDD object transforms one set of data to another or perform actions to get results.

The *SparkContext* gives us methods to create an RDD.
<br></br>

# Boilerplate
```python
from pyspark import SparkConf, SparkContext
import collections
import os

'''
set the Master node as the local machine on a single thread, single process
set app name to see the results
'''
conf = SparkConf().setMaster("local[*]").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

print("Working Directory: " + os.getcwd())
```
<br></br>

# Transformations
### Map
Map has a 1:1 relationship, i.e. every row in the dataset goes through the function. Map allows you to take a set of data and transform it into another set of data, given a function that operates on the RDD. For ex. if I want to square all the numbers in an RDD, have a *map* that *points* to a *function* that multiplies everything in an RDD by itself.

rdd = sc.parallelize([1,2,3,4])
rdd.map(lambda X: X*X) # square all numbers

Ans: 1,4,8,16

### Flatmap
Has the capability to produce multiple values for every input value that you have in your original RDD. The original RDD might be larger or smaller than the resultant RDD.

> With key/value data, use **mapValues()** and **flatMapValues()** if transformations do not effect the keys. These are more efficient.

### Filter
Get rid of information we do not need. Ex. Filter a lof file for errors only.

### Distinct
Unique values.

### Sample
Small junk from the original dataset.

### Merge
Union, intersection, subtract, cartesian

## Actions

### collect
dump RDD values and get results. 
### count
count all values
### countByValue
count breakdown by unique value
### take
sample few values
### top
sample few values
### reduce
combine different values for given key value

**Nothing actually happenes in your driver program until an action is called!**

## Spark can do special stuff with KEY/VALUE

### reduceByKey
combine values with the same key using some function. rdd.reduceByKey(lambda x, y: x+y) # adds them up

### groupByKey
group values with the same key

### sortByKey
sort RDD by key 

### keys(), values()
create an RDD of just the keys or the values

### join, rightOuterjoin, leftOuterjoin, cogroup, subtractByKey

<div>
    <img src="Fig B.png" style="max-width: 30%;"/>        
</div>
<div>
    <img src="Fig C.png" style="max-width: 30%;"/>        
</div>

## DataFrames
- Extend RDD to a DataFrame object
- Contain row object
- Can run SQL Queries
- Can have a schema (leading to more efficient storage)
- Read and write to JSON, Hive, Parquet, csv ...
- Communicate with JDBC/ODBC, Tableau

```python
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("SparkSQL").getOrCreate() # get or create cause we might already have a persistent session we want to reuse
inputData = spark.read.json("DataFile")
inputData.createOrReplaceTempView("myStructuredStuff") # to expose data, give the dataset a name to look like a database table
myResultDataframe = spark.sql("select * from")

myResultDataframe.show() # show results and how many rows
myResultDataframe.select("someFieldName") # select column
myResultDataframe.filter(myResultDataframe.select("someFieldName") > 200) 
myResultDataframe.groupby(myResultDataframe.select("someFieldName")).mean()
myResultDataframe.rdd().map(mapperFunction) # for like mapreduce, change to rdd

from pyspark.sql import functions as func
func.explode() # similar to flatMap - explodes columns into rows
func.split()
func.lower()

# passing columns as parameters
func.split(inputDF.value, '\\W+')
filter(wordsDF.word!="")
func.col("colName") # Can also do to refer to a columns name
```


## Broadcast Variables
Spark doesn't like joining dataframes as we do in SQL. So we load a lookup dictionary and broadcast it to the executor node. As spark distributes work load, broadcasting data means such lookups are available for all nodes in the cluster.         
- Broadcast objects to the executors, such that they are always there whenever needed
- Just use sc.broadcast() to ship of whatever you want
- Then use .value() to get the object back
- Use the broadcasted object however you want - map functions, UDFs...
