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



#Save as json

#Step 1 - Convert to Data Frame
#Step 2 - Use write or save api to save dataframe into different file formats


##### JSON FILE
revenuePerOrderIdDF = revenuePerOrderId.toDF(schema =["order_id","order_quantity"])
revenuePerOrderIdDF.save("/user/hdharod/output_files/revenuePerOrderId_json","json")
sqlContext.read.json("/user/shailee1/output_files/revenuePerOrderId_json").show()



##### PARQUET FILE
revenuePerOrderIdDF.saveAsParquetFile("/user/hdharod/output_files/revenuePerOrderId_parquet")
sqlContext.read.parquet("/user/shailee1/output_files/revenuePerOrderId_parquet").show()
