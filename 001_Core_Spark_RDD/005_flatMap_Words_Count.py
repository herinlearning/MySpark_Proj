from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("005_flatMap_Words_Count").setMaster(sys.argv[1]).setAll([("spark.executors.memory", "4G"),("spark.executors.core", "4")])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]


lines = sc.textFile(input_file)
words = lines.flatMap(lambda x: x.split(" ")) #// Convert each line into list collection - it creates ['How', 'are', 'you']
wordsCountMap = words.map(lambda x: (x,1))

wordsCount = wordsCountMap.reduceByKey(lambda x, y : x + y)

for i in wordsCount.take(100):
    print i