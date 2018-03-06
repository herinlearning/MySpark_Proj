# REPLACE map ---> sortByKey ---> map ---> take
# Replace it with ----------> Either  -------->  takeOrdered ------> OR ------> top
############################################################################################
# Sort data by product price ascending using takeOrdered
# Sort data by product price descending using top

from pyspark import SparkContext, SparkConf
import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("018_global_Ranking_takeOrdered_or_top").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","8G")])
sc = SparkContext(conf = conf)

input_file = sys.argv[2]

products = sc.textFile(input_file)
productFiltered = products.\
    filter(lambda p: (p.split(",")[4]) != "")

for i in productFiltered.take(10):
    print i

# Top N Products by ascending product price
topNProduct_byPrice = productFiltered.takeOrdered(10, key = lambda k: float(k.split(",")[4])) ### Multiply float by -1 if it is to be sorted in descending  

for i in topNProduct_byPrice:   # Since takeOrdered is action, there is no take required
    print i


# Top N Products by descending product price
topNProduct_byPrice_top = productFiltered.top(10, key = lambda k: float(k.split(",")[4]))

for i in topNProduct_byPrice_top:   # Since top is action, there is no take required
    print i