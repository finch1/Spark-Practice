from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("filterRDD")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    stationsID = fields[0]
    entryType = fields[2]
    tempreture = float(fields[3])
    return (stationsID, entryType, tempreture)

lines = sc.textFile("/home/vboxuser/SparkCourse/1800.csv")
# work on each line by calling the function
rdd = lines.map(parseLine)

# we want to filter on entryType. Since the function is returning the entry type in pos 1 then [1]
minTemps = rdd.filter(lambda x: "TMIN" in x[1])

# we do not need the entryType any longer. lambda returns tuple. This results in the min tempreture per day
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# find the minimum tempretureby stationID
yearLow = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = yearLow.collect()

for result in results:
    print(result[0] + "\t{:.2f} C".format(result[1])) # 2 decimal points

