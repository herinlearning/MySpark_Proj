from pyspark import SparkConf, SparkContext
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName("012_PerGroupAggregation_groupByKey").setMaster(sys.argv[1]).setAll([("spark.executors.core","4"),("spark.executors.memory","4G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

orderItems = sc.textFile(input_file)

orderItemsMap = orderItems.map(lambda oi : (int(oi.split(",")[1]), float(oi.split(",")[4])))

for i in orderItemsMap.take(10):
    print i

orderItemsRevenuePerIdgroup = orderItemsMap.groupByKey()

################################################################
# understand what comes as output of groupByKey
# first print type of output --- it shows ResultIterable
# ResultIterable means it is a tuple containing key,value as  key,RDD type list
# Output is as  (1, [299.98])
#               (2, [199.99, 250.0, 129.99])
#               (4, [49.98, 299.95, 150.0, 199.92])



for i in orderItemsRevenuePerIdgroup.take(10):
    print i

# (1, <pyspark.resultiterable.ResultIterable object at 0x0000000007183320>)
# (2, <pyspark.resultiterable.ResultIterable object at 0x00000000071A98D0>)
# (4, <pyspark.resultiterable.ResultIterable object at 0x00000000071A9B70>)

# to convert Spark Iterable and see the values inside it
l = orderItemsRevenuePerIdgroup.first()
print l[0]          # --- 1
print list(l[1])    # --- [199.99, 250.0, 129.99]


orderItemsRevenuePerId = orderItemsRevenuePerIdgroup.map(lambda ori : (ori[0], sum(ori[1])))

for i in orderItemsRevenuePerId.take(10):
    print i

# (1, 299.98)
# (2, 579.98)
# (4, 699.85)
# (5, 1129.8600000000001)
# (7, 579.9200000000001)
# (8, 729.8399999999999)
# (9, 599.96)
# (10, 651.9200000000001)
# (11, 919.79)
# (12, 1299.8700000000001)