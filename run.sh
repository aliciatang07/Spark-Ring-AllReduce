#!/bin/bash
#assemble the jar
if [ "$1" == "yes" ] ; then
  sbt assembly
fi
# run file
if [ "$2" == "GD_RingAllReduce" ] ; then
  spark-submit --class com.ringallreduce.$2 --master spark://localhost:7077 ./target/scala-2.11/spark-ringallreduce-assembly-1.0.jar $3 $4
else
  spark-submit --class com.ringallreduce.$2 --master spark://localhost:7077 ./target/scala-2.11/spark-ringallreduce-assembly-1.0.jar $3
fi