# Get revenue per Order Id


from pyspark import SparkConf, SparkContext
import os
import sys
from operator import add

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName("014_PerGrpAgg_reduceByKey").setMaster(sys.argv[1]).setAll([("spark.executors.core","4"),("spark.executors.memory","4G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]
orderItems = sc.textFile(input_file)


orderItemsMap = orderItems.map(lambda oi: ( int(oi.split(",")[1]),float(oi.split(",")[4]) ))
revenuePerOrderId = orderItemsMap.reduceByKey(lambda x,y : x + y)
for i in revenuePerOrderId.take(10):
    print i


#Alternate way to perform reduceByKey using add operator from operator library
revenuePerOrderId = orderItemsMap.reduceByKey(add)
for i in revenuePerOrderId.take(10):
    print i


# Get minimum revenue per Order Id
minRevenuePerOrderId = orderItemsMap.reduceByKey(lambda x,y: x if(x<y) else y)
for i in minRevenuePerOrderId.take(10):
    print i


# Get revenue per Order Id with full record for OrderId
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), oi))
minRevenuePerOrderIdFull = orderItemsMap.reduceByKey(lambda x, y: x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y)
for i in minRevenuePerOrderIdFull.take(10):
    print i
