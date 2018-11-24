# Compute daily revenue Per Product using data & Spark SQL

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import Row

import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName("001_dailyRevenuePerProduct_SparkSQL").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# Read file from hdfs
orders = sc.textFile(sys.argv[2])
orderItems = sc.textFile(sys.argv[3])

# Read file from local file system
productsRead = open(sys.argv[4]).read().splitlines()
products = sc.parallelize(productsRead)

# Convert RDD to dataframe
ordersDF = orders.map( lambda o:
			 Row(
					order_id          = int(o.split(",")[0]),
					order_date        = o.split(",")[1],
					order_customer_id = int(o.split(",")[2]),
					order_status      = o.split(",")[3]
				)
					 ).toDF()

order_itemsDF = orderItems.map( lambda oi:
			Row(
					order_item_order_id   = int(oi.split(",")[1]),
					order_item_product_id = int(oi.split(",")[2]),
					order_item_subtotal   = float(oi.split(",")[4])
			   )
								).toDF()

productsDF = products.map(lambda p:
			Row(
					product_id   = int(p.split(",")[0]),
					product_name = p.split(",")[2]
			   )
						 ).toDF()


# Register dataframe as temporary tables in hive

ordersDF.registerTempTable("ordersDF")
order_itemsDF.registerTempTable("order_itemsDF")
productsDF.registerTempTable("productsDF")

# Query hive stored temporary dataframe tables

sqlContext.sql("select * from ordersDF").show()
sqlContext.sql("select * from order_itemsDF").show()
sqlContext.sql("select * from productsDF").show()

# By default sparkSQL fires configured threads which is by default 200. Suppress the shuffling for low volume query.
sqlContext.setConf("spark.sql.shuffle.partitions", "2")


# Perform complex query to extract daily revenyue per product by date and in highest revenue order

sqlContext.sql("select \
						o.order_date, p.product_name, round(sum(oi.order_item_subtotal),2) as revenuePerProduct  \
						from \
						ordersDF o join order_itemsDF oi \
						on o.order_id = oi.order_item_order_id \
						join productsDF p \
						on oi.order_item_product_id = p.product_id \
						where o.order_status in ('CLOSED','COMPLETED') \
						group by o.order_date, p.product_name \
						order by o.order_date, revenuePerProduct desc \
").show()
