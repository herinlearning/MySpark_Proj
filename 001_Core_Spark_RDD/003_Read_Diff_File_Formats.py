#Read diff file formats - text, orc, json, parquet, avro
# help(sqlContext.read.json)
# help(sqlContext.read.orc)
# help(sqlContext.read.parquet)
# help(sqlContext.read.text)

from pyspark import SparkContext, SparkConf, SQLContext
import sys


conf = SparkConf().setAppName("Read different file formats").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','1')])
sc = SparkContext(conf=conf)

# Define sqlContext on spark context
sqlContext = SQLContext(sc)

inputpath = sys.argv[2]

#Read and show the json file
sqlContext.read.json(inputpath).show()


