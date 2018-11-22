# """
#
# -- Use retail_db data set
#
# Problem Statement
# -----------------
# 1. Get daily revenue by product considering completed and closed orders.
# 2. Data need to be sorted in ascending order by date and then descending order by revenue computed for each product for each day.
# 3. Data for orders and order_items is available in HDFS
#                                         /public/retail_db/orders and /public/retail_db/order_items
# 4. Data for products is available locally under /data/retail_db/products
# 5. Final output need to be stored under
#                                         HDFS location : avro format :        /user/YOUR_USER_ID/daily_revenue_avro_python
#                                         HDFS location : text format :       /user/YOUR_USER_ID/daily_revenue_txt_python
#                                         Local location                      /home/YOUR_USER_ID/daily_revenue_python
# 6. Solution need to be stored under
#                                         /home/YOUR_USER_ID/daily_revenue_python.txt
#
# """


from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("001_Problem_Ex_1").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)



orders = sc.textFile(sys.argv[2])
for i in orders.take(10):  print(i)


orderItems = sc.textFile(sys.argv[3])
for i in orderItems.take(10):  print(i)


products = sc.textFile(sys.argv[4])
for i in products.take(10):  print(i)


for i in sc.textFile(sys.argv[2]).\
map(lambda o: o.split(",")[3]).\
distinct().\
collect():  print(i)

productMap = products.map(lambda p: (int(p.split(",")[0]), p.split(",")[2]) )
for i in productMap.take(10):  print(i)

ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETED","CLOSED"] )
for i in ordersFiltered.take(10):  print(i)

ordersMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]), o.split(",")[1]) )
for i in ordersMap.take(10):  print(i)

orderItemsMap = orderItems.map(lambda oi: ( int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4])) ) )
for i in orderItemsMap.take(10):  print(i)

ordersJoin = ordersMap.join(orderItemsMap)
for i in ordersJoin.take(10):  print(i)


ordersJoinMap = ordersJoin.map(lambda r: ((r[1][0],r[1][1][0]), float(r[1][1][1]) ))
for i in ordersJoinMap.take(10):  print(i)

dailyRevenuePerProductId = ordersJoinMap.reduceByKey(lambda x,y : x + y)
for i in dailyRevenuePerProductId.take(100):  print(i)


dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda r: (r[0][1],(r[0][0],r[1])) )
for i in dailyRevenuePerProductIdMap.take(10):  print(i)


dailyRevenuePerProductNameJoin = dailyRevenuePerProductIdMap.join(productMap)
for i in dailyRevenuePerProductNameJoin.take(100):  print(i)


dailyRevenuePerProductSortedMap = dailyRevenuePerProductNameJoin.map(lambda e: ((e[1][0][0], -float(e[1][0][1])), e[1][1])).sortByKey()
for i in dailyRevenuePerProductSortedMap.take(100):  print(i)

dailyRevenuePerProductSorted = dailyRevenuePerProductSortedMap.map(lambda x: ( x[0][0] + "," + x[1] +"," +  str(-float(x[0][1]))))
for i in dailyRevenuePerProductSorted.take(100):  print(i)

# dailyRevenuePerProductSorted.coalesce(2).saveAsTextFile(sys.argv[5])

