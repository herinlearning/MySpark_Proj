from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Row Level Tx - Map").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','1')])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]

order = sc.textFile(input_file)

orderCompleteClosed = order.filter(lambda x: (x.split(",")[3] in ("COMPLETE","CLOSED")))

print orderCompleteClosed.count()