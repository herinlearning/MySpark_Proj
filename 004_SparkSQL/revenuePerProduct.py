# Compute daily revenue Per Product using dataframes & Spark SQL

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import sys
import os

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"


conf = SparkConf().setAppName("001_dailyRevenuePerProduct_SparkSQL").setMaster(sys.argv[1]).setAll([("spark.core.executors","4"),("spark.core.memory","4G")])
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)


from pyspark.sql import Row



orders = sc.textFile(sys.argv[2])
orderItems = sc.textFile(sys.argv[3])



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
												order_item_order_id = 	int(oi.split(",")[1]),
												order_item_product_id = int(oi.split(",")[2]),
												order_item_subtotal =   float(oi.split(",")[4])
											    )
								).toDF()


productsRead = open(sys.argv[4]).read().splitlines()
products = sc.parallelize(productsRead)

productsDF = products.map(lambda p:
									Row(
										product_id   = int(p.split(",")[0]),
										product_name = p.split(",")[2]
									   )
						 ).toDF()



ordersDF.registerTempTable("ordersDF")
order_itemsDF.registerTempTable("order_itemsDF")
productsDF.registerTempTable("productsDF")

sqlContext.sql("select * from ordersDF").show()
sqlContext.sql("select * from order_itemsDF").show()
sqlContext.sql("select * from productsDF").show()

sqlContext.setConf("spark.sql.shuffle.partitions", "2")

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
