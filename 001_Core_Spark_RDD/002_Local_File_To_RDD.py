#Read raw file - convert into python List - convert List into RDD and perform actions on RDD


from pyspark import SparkContext,SparkConf
import sys

# Define sparkconf & sc
conf = SparkConf().setAppName("Convert Local File to RDD").setMaster(sys.argv[1]).setAll([('spark.executor.memory', '4G'),('spark.executor.cores','1')])
sc = SparkContext(conf=conf)

inputpath = sys.argv[2]

# Read the raw file from local windows/ unix filesystem into a python List
orderItemsRawList = open(inputpath).read().splitlines()
print type(orderItemsRawList)

#Convert Python Raw list into spark RDD
orderItemsRDD = sc.parallelize(orderItemsRawList)
print type(orderItemsRDD)

#Operation on spark RDD by applying actions
print orderItemsRDD.count()

for i in orderItemsRDD.take(5):
    print i




#=============================================================================================================================================================================================
#Output
# =============================================================================================================================================================================================

#C:\Users\tejasvi\Anaconda2\python.exe C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py local C:\Users\tejasvi\Documents\Working_Datafiles\Durga\data_master\retail_db\order_items\part-00000 C:\Users\tejasvi\Documents\outputfiles\SQL\
#Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
#18/02/04 10:28:44 INFO SparkContext: Running Spark version 1.6.3
#18/02/04 10:28:45 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
#18/02/04 10:28:45 INFO SecurityManager: Changing view acls to: tejasvi
#18/02/04 10:28:45 INFO SecurityManager: Changing modify acls to: tejasvi
#18/02/04 10:28:45 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users with view permissions: Set(tejasvi); users with modify permissions: Set(tejasvi)
#18/02/04 10:28:45 INFO Utils: Successfully started service 'sparkDriver' on port 49701.
#18/02/04 10:28:46 INFO Slf4jLogger: Slf4jLogger started
#18/02/04 10:28:46 INFO Remoting: Starting remoting
#18/02/04 10:28:46 INFO Remoting: Remoting started; listening on addresses :[akka.tcp://sparkDriverActorSystem@192.168.1.249:49715]
#18/02/04 10:28:46 INFO Utils: Successfully started service 'sparkDriverActorSystem' on port 49715.
#18/02/04 10:28:46 INFO SparkEnv: Registering MapOutputTracker
#18/02/04 10:28:46 INFO SparkEnv: Registering BlockManagerMaster
#18/02/04 10:28:46 INFO DiskBlockManager: Created local directory at C:\Users\tejasvi\AppData\Local\Temp\blockmgr-a88cac1b-b195-4546-984a-c182b0d76d96
#18/02/04 10:28:46 INFO MemoryStore: MemoryStore started with capacity 511.1 MB
#18/02/04 10:28:46 INFO SparkEnv: Registering OutputCommitCoordinator
#18/02/04 10:28:46 INFO Utils: Successfully started service 'SparkUI' on port 4040.
#18/02/04 10:28:46 INFO SparkUI: Started SparkUI at http://192.168.1.249:4040
#18/02/04 10:28:46 INFO Executor: Starting executor ID driver on host localhost
#18/02/04 10:28:46 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 49722.
#18/02/04 10:28:46 INFO NettyBlockTransferService: Server created on 49722
#18/02/04 10:28:46 INFO BlockManagerMaster: Trying to register BlockManager
#18/02/04 10:28:46 INFO BlockManagerMasterEndpoint: Registering block manager localhost:49722 with 511.1 MB RAM, BlockManagerId(driver, localhost, 49722)
#18/02/04 10:28:46 INFO BlockManagerMaster: Registered BlockManager



#<type 'list'>
#<class 'pyspark.rdd.RDD'>



#18/02/04 10:28:47 INFO SparkContext: Starting job: count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15
#18/02/04 10:28:47 INFO DAGScheduler: Got job 0 (count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15) with 1 output partitions
#18/02/04 10:28:47 INFO DAGScheduler: Final stage: ResultStage 0 (count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15)
#18/02/04 10:28:47 INFO DAGScheduler: Parents of final stage: List()
#18/02/04 10:28:47 INFO DAGScheduler: Missing parents: List()
#18/02/04 10:28:47 INFO DAGScheduler: Submitting ResultStage 0 (PythonRDD[1] at count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15), which has no missing parents
#18/02/04 10:28:47 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 4.1 KB, free 511.1 MB)
#18/02/04 10:28:47 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 2.7 KB, free 511.1 MB)
#18/02/04 10:28:47 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on localhost:49722 (size: 2.7 KB, free: 511.1 MB)
#18/02/04 10:28:47 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1006
#18/02/04 10:28:47 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (PythonRDD[1] at count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15)
#18/02/04 10:28:47 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks
#18/02/04 10:28:47 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, partition 0,PROCESS_LOCAL, 7793 bytes)
#18/02/04 10:28:47 INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
#18/02/04 10:28:48 INFO PythonRunner: Times: total = 1036, boot = 1020, init = 16, finish = 0
#18/02/04 10:28:48 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 995 bytes result sent to driver
#18/02/04 10:28:48 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1098 ms on localhost (1/1)
#18/02/04 10:28:48 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
#18/02/04 10:28:48 INFO DAGScheduler: ResultStage 0 (count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15) finished in 1.114 s
#18/02/04 10:28:48 INFO DAGScheduler: Job 0 finished: count at C:/Users/tejasvi/PycharmProjects/CCA_175/001_Core_Spark_RDD/002_Local_File_To_RDD.py:15, took 1.262813 s


#200


#18/02/04 10:28:49 INFO SparkContext: Starting job: runJob at PythonRDD.scala:393
#18/02/04 10:28:49 INFO DAGScheduler: Got job 1 (runJob at PythonRDD.scala:393) with 1 output partitions
#18/02/04 10:28:49 INFO DAGScheduler: Final stage: ResultStage 1 (runJob at PythonRDD.scala:393)
#18/02/04 10:28:49 INFO DAGScheduler: Parents of final stage: List()
#18/02/04 10:28:49 INFO DAGScheduler: Missing parents: List()
#18/02/04 10:28:49 INFO DAGScheduler: Submitting ResultStage 1 (PythonRDD[2] at RDD at PythonRDD.scala:43), which has no missing parents
#18/02/04 10:28:49 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 3.3 KB, free 511.1 MB)
#18/02/04 10:28:49 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 2.2 KB, free 511.1 MB)
#18/02/04 10:28:49 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on localhost:49722 (size: 2.2 KB, free: 511.1 MB)
#18/02/04 10:28:49 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1006
#18/02/04 10:28:49 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (PythonRDD[2] at RDD at PythonRDD.scala:43)
#18/02/04 10:28:49 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks
#18/02/04 10:28:49 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1, localhost, partition 0,PROCESS_LOCAL, 7793 bytes)
#18/02/04 10:28:49 INFO Executor: Running task 0.0 in stage 1.0 (TID 1)
#18/02/04 10:28:50 INFO PythonRunner: Times: total = 1048, boot = 1033, init = 15, finish = 0
#18/02/04 10:28:50 INFO Executor: Finished task 0.0 in stage 1.0 (TID 1). 1160 bytes result sent to driver
#18/02/04 10:28:50 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 1079 ms on localhost (1/1)
#18/02/04 10:28:50 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
#18/02/04 10:28:50 INFO DAGScheduler: ResultStage 1 (runJob at PythonRDD.scala:393) finished in 1.079 s
#18/02/04 10:28:50 INFO DAGScheduler: Job 1 finished: runJob at PythonRDD.scala:393, took 1.085566 s


#1,1,957,1,299.98,299.98
#2,2,1073,1,199.99,199.99
#3,2,502,5,250.0,50.0
#4,2,403,1,129.99,129.99
#5,4,897,2,49.98,24.99




#18/02/04 10:28:51 INFO SparkContext: Invoking stop() from shutdown hook
#
#Process finished with exit code 0
#