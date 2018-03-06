# get the order id in sorted manner by highest amount

from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("013_PerGrpAgg_groupByKey_Sorted").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

orderItems = sc.textFile(input_file)

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), oi))

orderItemsGrpBy = orderItemsMap.groupByKey()

for i in orderItemsGrpBy.take(2):
    print list(i[1])
    print sorted(i[1], key = lambda k: float(k.split(",")[4]), reverse=True)


orderItemsSortedMap = orderItemsGrpBy.map(lambda oig: sorted(oig[1], key = lambda k: float(k.split(",")[4]), reverse = True))

for i in orderItemsSortedMap.take(10):
    print i


# Transposing the flattened map iterable to individual row - Use flatMap

orderItemsSorted = orderItemsGrpBy.flatMap(lambda oig: sorted(oig[1], key = lambda k: float(k.split(",")[4]), reverse = True))

for i in orderItemsSorted.take(10):
    print i
