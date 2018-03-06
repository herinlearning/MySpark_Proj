from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Reading Weather Data to extract Max Weather").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','4')])
sc = SparkContext(conf=conf)

input_location = sys.argv[2]
input_file = sys.argv[3]

weatherMap = sc.textFile(input_location+input_file)

weatherMapFilter = weatherMap.map(lambda x: (x.split(",")[0], x.split(",")[1],x.split(",")[2],x.split(",")[3]) )
weatherMapFilterMax = weatherMapFilter.filter(lambda fi: fi[2] == "TMAX")

weatherMapFilterMaxClean = weatherMapFilterMax.map(lambda x : (x[0], (float(x[3]) * 0.1 * (9.0 / 5.0) + 32.0)))

weatherbyMaxTemp = weatherMapFilterMaxClean.reduceByKey(lambda x, y: max(x, y))

for i in weatherbyMaxTemp.take(10):
    print i