#Read raw orders item file and convert into RDD and row level tx on RDD - Map & flatMap

from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Row Level Tx - Map").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','1')])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]



orderItems = sc.textFile(input_file)
# 1,1,957,1,299.98,299.98
# 2,2,1073,1,199.99,199.99
# 3,2,502,5,250.0,50.0
# 4,2,403,1,129.99,129.99
# 5,4,897,2,49.98,24.99


# Split & map the row into 2 records of K,V pair
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
# (1, 299.98)
# (2, 199.99)
# (2, 250.0)
# (2, 129.99)
# (4, 49.98)
# (4, 299.95)
# (4, 150.0)
# (4, 199.92)


#Aggregage each value by Key (by K)
orderRevenuebyKey = orderItemsMap.reduceByKey(lambda x, y: x + y)

for i in orderRevenuebyKey.take(10):
    print i

# (1, 299.98)
# (2, 579.98)
# (4, 699.85)