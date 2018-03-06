import sys
import os
from pyspark import SparkConf, SparkContext

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setMaster(sys.argv[1]).setAppName("010_Aggregations_02_Min_OrderId").setAll([("spark.executors.memory","4G"),("spark.executors.core","4")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

orders = sc.textFile(input_file)

ordersFilter = orders.map(lambda o : (o.split(",")[3],1))

ordersCount = ordersFilter.countByKey()

for i,v in ordersCount.items():
    print i,v

# COMPLETE 22899
# PAYMENT_REVIEW 729
# PROCESSING 8275
# CANCELED 1428
# PENDING 7610
# CLOSED 7556
# PENDING_PAYMENT 15030
# SUSPECTED_FRAUD 1558
# ON_HOLD 3798