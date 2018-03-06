from pyspark import SparkConf, SparkContext
import os
import sys
import itertools as it

os.environ['SPARK_HOME'] = "C:\opt\spark\spark-1.6.3-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\winutils"

conf = SparkConf().setAppName('020_topNPrice_PerCategory').setMaster(sys.argv[1]).setAll([("spark.executors.core","4"),("spark.executors.memory","8G")])
sc = SparkContext(conf=conf)

input_file = sys.argv[2]


def getTopNPricedProductsPerCategoryId(productGroupPerCat, topN):
    productRDDSorted = sorted(productGroupPerCat[1], key=lambda k: float(k.split(',')[4]), reverse=True)
    productPrices = map(lambda pr: float(pr.split(',')[4]), productRDDSorted)
    topNPrice = sorted(set(productPrices),reverse=True)[:topN]
    return it.takewhile(lambda ps : float(ps.split(",")[4]) in topNPrice,productRDDSorted)


products = sc.textFile(input_file)

productCleaned = products.filter(lambda x: (x.split(",")[4]) != "")

# Convert into Paired RDD to get K,V - Category as key, ProductRDD as value
productMap = productCleaned.map(lambda x: (int(x.split(",")[1]), x))

# Group the Key resulting in K, Iter(V)
productGroupPerCategory = productMap.groupByKey()


# =============================
# Logic for defining function
# =============================
# t = productGroupPerCategory.filter(lambda p : p[0] == 59).first()
# lst = list(t[1])
# lst_sorted = sorted(t[1], key=lambda k: float(k.split(',')[4]), reverse=True)
# productPrices = map(lambda pr: float(pr.split(',')[4]), lst_sorted)
# topNPrice = sorted(set(productPrices),reverse=True)[:3]
# productPricePerCategory = it.takewhile(lambda ps : float(ps.split(",")[4]) in topNPrice,lst_sorted)
# for i in list(getTopNPricedProductsPerCategoryId(t, 3)):
#     print i


topNPricedProductsPerCategory = productGroupPerCategory. \
flatMap(lambda p: getTopNPricedProductsPerCategoryId(p, 3))

for i in topNPricedProductsPerCategory.collect():
    print(i)