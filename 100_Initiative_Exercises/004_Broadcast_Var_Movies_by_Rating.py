from pyspark import SparkContext, SparkConf
import sys

conf = SparkConf().setAppName("Broadcast Movie Code/Name dict and extract most rated movie").\
    setMaster(sys.argv[1]).setAll([('spark.executor.memory','4G'),('spark.executor.core','2')])

sc = SparkContext(conf=conf)

input_file = sys.argv[2]

def broadcast_movie_var():
    readfile = open(input_file).read().splitlines()
    movieDict = {}
    for lines in readfile:
        fields = lines.split("|")
        movieDict[int(fields[0])] = int(fields[1])
    return movieDict

broadcast_movie = sc.broadcast(broadcast_movie_var())

lines = sc.textFile(input_file)
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda (x, y) : (y, x))
sortedMovies = flipped.sortByKey()

sortedMovieByName = sortedMovies.map(lambda (cnt, movie): (broadcast_movie.value[movie],cnt))

for i in sortedMovieByName.take(10):
    print i