# Spark Ring AllReduce

## Overview
This repo is for the CSC2222 term project "Survey and Improvement of Distributed Machine Learning On Spark"


## prepare Instruction 
   Type in command ```sbt assembly``` in root folder to generate deployable jar 

##Running Instruction 
1. run Spark RingAllReduce with local mode m cores 
```
<SPARK_HOME>/bin/spark-submit --class com.ringallreduce.RingAllReduce --master spark://<SPARK_MASTER>:7077 ./target/scala-2.11/spark-ringallreduce-assembly-1.0.jar m 
```
2.run Spark RingAllReduceDC with local mode m cores 
```
<SPARK_HOME>/bin/spark-submit --class com.ringallreduce.RingAllReduceDC --master spark://<SPARK_MASTER>:7077 ./target/scala-2.11/spark-ringallreduce-assembly-1.0.jar m 
```
For this two programs you will get the same final result with similar runtime performance
, which means the divide-and-conquer approach is a possible way to solve the blocking issue 
in current Spark Barrier Scheduling. The paper will also mention a experiment results.



