from pyspark.sql import SparkSession, functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType


spark = SparkSession.builder.appName("amountSpent customer orders").master("local[*]").getOrCreate()

schema = StructType([                        
                        StructField("customerID",IntegerType(),True),
                        StructField("orderID",IntegerType(),True),
                        StructField("amountSpent",FloatType(),True)
])


orders = spark.read.schema(schema=schema).csv("/home/vboxuser/SparkCourse/customer-orders.csv")

# MINES
orders.printSchema()
orders.show()

ordersPerCustomer = orders.groupBy("customerID").sum("amountSpent")
#ordersPerCustomer.show()

sortedOrders = ordersPerCustomer.withColumn("totalSpent", func.round(func.col("sum(amountSpent)"), 2)).select("customerID", "totalSpent")
sortedOrders.sort(sortedOrders.totalSpent.desc()).show(truncate=False)


# HIS
ordersPerCustomer = orders.groupBy("customerID").agg(func.round(func.sum("amountSpent"),2).alias("totalSpent"))

ordersPerCustomer.sort(ordersPerCustomer.totalSpent.desc()).show(truncate=False)