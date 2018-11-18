# Get revenue and count of each items for each order id
# combiner logic --- computing intermediate logic
# reducer logic - computing final values using intermediate values
# initialization value

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
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

revenueCountPerItem = orderItemsMap.aggregateByKey \
        (
                      (0.0, 0),
                      lambda x, y: (x[0] + y, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1])
        )

#Comment
"""
(2, 199.99)
(2, 250.0)
(2, 129.99)

initialization value - based on desired output
final output is (2, (579.98,3)) -- a tuple --- (0.0,0)

lambda 1 - combiner logic
x - datatype of tuple based on desired output  - tuple
y - datatype of input value 		 		   - float
---> (x[0] + y, x[1] + 1)

first 	iteration - (2, 199.99) -- (199.99, 1) ==> ( (0.0 + 199.99), 0+1 ) ==> (199.99,1)
second 	iteration - (2, 250.0)  -- (250.0 , 1) ==> ( (199.99 + 250.0), 1+1 ) ==> (449.99,2)
third	iteration - (2, 129.99) -- (129.99, 1) ==> ( (449.99 + 129.99), 2+1 ) ==> (579.98,3)

lambda 2 - reducer logic
x - datatype of tuple based on desired output  - tuple
y - datatype of tuple based on desired output  - tuple
---> (x[0] + y[0], x[1] + y[1])

"""


for i in revenueCountPerItem.take(10):
    print i
