from pyspark import SparkConf, SparkContext
import sys
import os

os.environ['HADOOP_HOME'] = "C:\winutils"
os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
#sys.path.append("C:/Mine/Spark/hadoop-2.6.0/bin")

conf = SparkConf().setAppName("008 Paired RDD : Left & Right Outer Join").setMaster(sys.argv[1]).setAll([("spark.executors.core","4"),("spark.executors.memory","4G")])
sc = SparkContext(conf = conf)

input_path = sys.argv[2]
input_order_file = sys.argv[3]
input_order_items_file = sys.argv[4]

orders = sc.textFile(input_path + input_order_file)
orderItems = sc.textFile(input_path + input_order_items_file)

ordersMap = orders.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems.map(lambda o: (int(o.split(",")[1]), o.split(",")[4]))

# Left Outer Join & join Non matching rows
orderItemsleftJoin = ordersMap.leftOuterJoin(orderItemsMap)
orderItemsJoinLeftFilter = orderItemsleftJoin.filter(lambda of : of[1][1] == None)

for i in orderItemsJoinLeftFilter.take(20):
    print i

# Right Outer Join & join Non matching rows
orderItemsRightJoin = orderItemsMap.rightOuterJoin(ordersMap)
orderItemsJoinRightFilter = orderItemsRightJoin.filter(lambda of : of[1][0] == None)


for i in orderItemsJoinRightFilter.take(10):
    print i