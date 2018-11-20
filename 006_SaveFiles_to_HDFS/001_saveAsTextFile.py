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






# Convert into required text format delimeted by tab '/t'
revenuePerOrderId = orderItemsMap.reduceByKey(lambda x, y: x + y).map(lambda s: str(s[0]) + "\t" + str(round(s[1])))

#Save as textFile
revenuePerOrderId.saveAsTextFile("/user/shailee1/output_files/revenuePerOrderId")


#Save as textFile with compression
revenuePerOrderId.saveAsTextFile("/user/shailee1/output_files/revenuePerOrderId_compressed", compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")
