from pyspark import SparkConf, SparkContext
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName("009 Aggregations : RDD Actions - Count and Reduce ").setMaster(sys.argv[1]).setAll([("spark.executors.memory","4G"),("spark.executors.core","4")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

orderItems = sc.textFile(input_file)
# 1,1,957,1,299.98,299.98
# 2,2,1073,1,199.99,199.99
# 3,2,502,5,250.0,50.0
# 4,2,403,1,129.99,129.99

orderItemsforRevenueId = orderItems.filter(lambda oi : int(oi.split(",")[1]) == 4)

# 2,2,1073,1,199.99,199.99
# 3,2,502,5,250.0,50.0
# 4,2,403,1,129.99,129.99


orderItemsRevenueMap = orderItemsforRevenueId.map(lambda oir : float(oir.split(",")[4]))

#199.99
#250.0
#129.99

orderItemsRev = orderItemsRevenueMap.reduce(lambda x, y : x + y)
print orderItemsRev

#579.98

