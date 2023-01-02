from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("customerOrders")
sc = SparkContext(conf=conf)

lines = sc.textFile("/home/vboxuser/SparkCourse/customer-orders.csv")
ordersPerCustomer = lines.map(lambda x: (int(x.split(',')[0]), float(x.split(',')[2])))

results = sorted(ordersPerCustomer.reduceByKey(add).collect(), key=lambda a: a[1], reverse=True)

print("RESULTS")

for result in results:
    print("{:3}\t{:.2f}".format(result[0],result[1]))
    #print(result)

