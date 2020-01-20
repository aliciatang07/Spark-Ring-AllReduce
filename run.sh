#!/bin/bash
spark-submit --class $1 --master spark://localhost:7077 ./target/scala-2.11/spark-ringallreduce-assembly-1.0.jar $2 