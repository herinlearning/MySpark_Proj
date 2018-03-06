# Sort data by product category in ascending order and then product price descending
# this comprises of 2 fields - composite key sorting - one asc, one desc
# if the key are numbers, negate the value of key having with descending


from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("017_sortByKey_CompositeKey").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

products = sc.textFile(input_file)
productsMap = products.\
    filter(lambda p: (p.split(",")[4]) != ""). \
    map(lambda p : ((int(p.split(",")[1]), -float(p.split(",")[4])),p))

productSorted = productsMap.sortByKey()
#map(lambda x: x[1]) -- to get the original tuple

for i in productSorted.take(10):
    print i