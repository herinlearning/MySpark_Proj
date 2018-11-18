#Read raw file - convert into RDD - perform transformation & actions on RDD

# from pyspark import SparkConf, SparkContext
from pyspark import SparkConf, SparkContext
import sys

#Define conf and spark context
conf = SparkConf().setAppName("Simple RDD Read text File").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','1')])
sc = SparkContext(conf=conf)

#Parameterize the input path
inputpath = sys.argv[2]

#Read the raw file in RDD
orderItems = sc.textFile(inputpath+"order_items")

#Create RDD collection by mapping the fields
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(',')[1]), float(oi.split(',')[4])))

#Print the RDD
for k in orderItemsMap.take(10):
    print k









#C:\Users\tejasvi\Anaconda2\python.exe C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/001_Spark_RDD.py local C:\Users\tejasvi\Documents\Working_Datafiles\Durga\data_master\retail_db\ C:\Users\tejasvi\Documents\outputfiles\SQL\
#Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
#18/02/04 10:32:15 INFO SparkContext: Running Spark version 1.6.3
#18/02/04 10:32:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
#18/02/04 10:32:16 INFO SecurityManager: Changing view acls to: tejasvi
#18/02/04 10:32:16 INFO SecurityManager: Changing modify acls to: tejasvi
#18/02/04 10:32:16 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(tejasvi); users with modify permissions: Set(tejasvi)
#18/02/04 10:32:16 INFO Utils: Successfully started service 'sparkDriver' on port 49768.
#18/02/04 10:32:16 INFO Slf4jLogger: Slf4jLogger started
#18/02/04 10:32:16 INFO Remoting: Starting remoting
#18/02/04 10:32:17 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriverActorSystem@192.168.1.249:49781]
#18/02/04 10:32:17 INFO Utils: Successfully started service 'sparkDriverActorSystem' on port 49781.
#18/02/04 10:32:17 INFO SparkEnv: Registering MapOutputTracker
#18/02/04 10:32:17 INFO SparkEnv: Registering BlockManagerMaster
#18/02/04 10:32:17 INFO DiskBlockManager: Created local directory at C:\Users\tejasvi\AppData\Local\Temp\blockmgr-8e406d44-c743-439d-be6d-e0205caed11d
#18/02/04 10:32:17 INFO MemoryStore: MemoryStore started with capacity 511.1 MB
#18/02/04 10:32:17 INFO SparkEnv: Registering OutputCommitCoordinator
#18/02/04 10:32:17 INFO Utils: Successfully started service 'SparkUI' on port 4040.
#18/02/04 10:32:17 INFO SparkUI: Started SparkUI at http://192.168.1.249:4040
#18/02/04 10:32:17 INFO Executor: Starting executor ID driver on host localhost
#18/02/04 10:32:17 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 49788.
#18/02/04 10:32:17 INFO NettyBlockTransferService: Server created on 49788
#18/02/04 10:32:17 INFO BlockManagerMaster: Trying to register BlockManager
#18/02/04 10:32:17 INFO BlockManagerMasterEndpoint: Registering block manager localhost:49788 with 511.1 MB RAM, BlockManagerId(driver, localhost, 49788)
#18/02/04 10:32:17 INFO BlockManagerMaster: Registered BlockManager
#18/02/04 10:32:18 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 127.4 KB, free 511.0 MB)
#18/02/04 10:32:18 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 13.9 KB, free 511.0 MB)
#18/02/04 10:32:18 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:49788 (size: 13.9 KB, free: 511.1 MB)
#18/02/04 10:32:18 INFO SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:-2
#18/02/04 10:32:18 INFO FileInputFormat: Total input paths to process : 2
#18/02/04 10:32:18 INFO SparkContext: Starting job: runJob at PythonRDD.scala:393
#18/02/04 10:32:18 INFO DAGScheduler: Got job 0 (runJob at PythonRDD.scala:393) with 1 output partitions
#18/02/04 10:32:18 INFO DAGScheduler: Final stage: ResultStage 0 (runJob at PythonRDD.scala:393)
#18/02/04 10:32:18 INFO DAGScheduler: Parents of final stage: List()
#18/02/04 10:32:18 INFO DAGScheduler: Missing parents: List()
#18/02/04 10:32:18 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[2] at RDD at PythonRDD.scala:43), which has no missing parents
#18/02/04 10:32:18 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 5.5 KB, free 511.0 MB)
#18/02/04 10:32:18 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 3.5 KB, free 511.0 MB)
#18/02/04 10:32:18 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:49788 (size: 3.5 KB, free: 511.1 MB)
#18/02/04 10:32:18 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
#18/02/04 10:32:18 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[2] at RDD at PythonRDD.scala:43)
#18/02/04 10:32:18 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
#18/02/04 10:32:18 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, partition 0,PROCESS_LOCAL, 2203 bytes)
#18/02/04 10:32:18 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
#18/02/04 10:32:18 INFO HadoopRDD: Input split: file:/C:/Users/tejasvi/Documents/Working_Datafiles/Durga/data_master/retail_db/order_items/part-00000:0+5110
#18/02/04 10:32:18 INFO deprecation: mapred.tip.id is deprecated. Instead, use mapreduce.task.id
#18/02/04 10:32:18 INFO deprecation: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
#18/02/04 10:32:18 INFO deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
#18/02/04 10:32:18 INFO deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
#18/02/04 10:32:18 INFO deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
#18/02/04 10:32:19 INFO PythonRunner: Times: total = 1038, boot = 1022, init = 0, finish = 16
#18/02/04 10:32:19 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 2319 bytes result sent to driver
#18/02/04 10:32:19 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1116 ms on localhost (1/1)
#18/02/04 10:32:19 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
#18/02/04 10:32:19 INFO DAGScheduler: ResultStage 0 (runJob at PythonRDD.scala:393) finished in 1.146 s
#18/02/04 10:32:19 INFO DAGScheduler: Job 0 finished: runJob at PythonRDD.scala:393, took 1.201074 s
#(1, 299.98)
#(2, 199.99)
#(2, 250.0)
#(2, 129.99)
#(4, 49.98)
#(4, 299.95)
#(4, 150.0)
#(4, 199.92)
#(5, 299.98)
#(5, 299.95)
#18/02/04 10:32:20 INFO SparkContext: Invoking stop() from shutdown hook
#
#Process finished with exit code 0
#