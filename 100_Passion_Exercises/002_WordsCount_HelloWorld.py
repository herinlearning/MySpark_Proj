from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("002_Words_Counts").setMaster(sys.argv[1]).setMaster(sys.argv[1]).setAll([('spark.executor.memory', '12G'),('spark.executor.cores','2')])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]

lines = sc.textFile(input_file)
wordsMap = lines.flatMap(lambda x: x.split())


wordsCountMap = wordsMap.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
wordsCountSort = wordsCountMap.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)

for i in wordsCountSort.collect():
    print i