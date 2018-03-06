# per Group Ranking
# Get top N products by price in each category id

from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("019_perKeyRanking_perGroupRanking").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","8G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]


products = sc.textFile(input_file)
productFiltered = products.\
    filter(lambda p: (p.split(",")[4]) != "")

# Convert into Paired RDD to get K,V
productMap = productFiltered.map(lambda x: (int(x.split(",")[1]), x))

# Group the Key resulting in K, Iter(V)
productGroupPerCategory = productMap.groupByKey()

# to sort the collection in by price descending & fetch first 3 elements per category
# use flatMap to flatten out the collection iter(V) into individual record and use python collection function sorted (mentioning key)

topNProductByCategory = productGroupPerCategory.flatMap(lambda p: sorted(p[1], key=lambda p: float(p.split(",")[4]), reverse=True)[:3])


for i in topNProductByCategory.take(10):
    print i