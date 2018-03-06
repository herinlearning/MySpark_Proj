# set Operation - Union with distinct
from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName("021_setOperation_Union_Distinct").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","8G")])
sc = SparkContext(conf=conf)

order_input_file = sys.argv[2]
orderItems_input_file = sys.argv[3]

orders = sc.textFile(order_input_file)
orderItems = sc.textFile(orderItems_input_file)

order201312 = orders.filter(lambda oi1: (oi1.split(",")[1][:7]) == "2013-12").map(lambda oi1: (int(oi1.split(",")[0]), oi1))
order201401 = orders.filter(lambda oi2: (oi2.split(",")[1][:7]) == "2014-01").map(lambda oi2: (int(oi2.split(",")[0]), oi2))

orderItemsMap = orderItems.map(lambda oii: (int(oii.split(",")[1]), oii))

order201312Join = order201312.join(orderItemsMap).map(lambda oi: oi[1][1])
order201312Join = order201401.join(orderItemsMap).map(lambda oi: oi[1][1])

products201312 = order201312Join. \
map(lambda p: int(p.split(",")[2]))
products201401 = order201312Join. \
map(lambda p: int(p.split(",")[2]))


allproducts = products201312. \
union(products201401). \
distinct()


for i in allproducts.collect():
    print i