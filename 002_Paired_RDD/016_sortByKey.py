# Sort data by product category and then product price descending
# filter out the key to sort and attach it to original RDD
# sort inline to key and use map to obtain the sorted RDD


from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("016_sortByKey").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

products = sc.textFile(input_file)
productsMap = products.\
    filter(lambda p: (p.split(",")[4]) != "").\
    map(lambda p : (float(p.split(",")[4]),p))

productSorted = productsMap.sortByKey(ascending=False).map(lambda p: p[1])

for i in productSorted.take(10):
    print i
