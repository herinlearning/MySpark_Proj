from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setMaster("Total Spent By Customer").setMaster(sys.argv[1]).setAll([('spark.executor.memory','4G'),('spark.executor.cores','4')])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]

customerRDD = sc.textFile(input_file)
custRDDFiltered = customerRDD.map(lambda x: (int(x.split(",")[0]),float(x.split(",")[2])))

SpendingByCust = custRDDFiltered.reduceByKey(lambda x, y : x+y).map(lambda x: (x[1],x[0])).sortByKey(ascending=Falses)


for i in SpendingByCust.take(10):
    print i