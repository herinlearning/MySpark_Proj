# Get revenue and count of each items for each order id
# combiner logic --- computing intermediate logic
# reducer logic - computing final values using intermediate values
# initialization value

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
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

revenueCountPerItem = orderItemsMap.aggregateByKey \
        (
                      (0.0, 0),
                      lambda x, y: (x[0] + y, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1])
        )


for i in revenueCountPerItem.take(10):
    print i
