import sys
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setMaster(sys.argv[1]).setAppName("010_Aggregations_02_Min_OrderId").setAll([("spark.executors.memory","4G"),("spark.executors.core","4")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

orderItems = sc.textFile(input_file)


orderItemsFilter = orderItems.filter( lambda oi: int(oi.split(",")[1]) == 2)
# 2,2,1073,1,199.99,199.99
# 3,2,502,5,250.0,50.0
# 4,2,403,1,129.99,129.99

orderItemsMap = orderItemsFilter.reduce(lambda x,y: x if (float(x.split(",")[4]) < float(y.split(",")[4])) else y)
# 4,2,403,1,129.99,129.99

print orderItemsMap
