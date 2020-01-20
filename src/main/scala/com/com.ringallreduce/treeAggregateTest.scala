package com.ringallreduce
import org.apache.spark.{BarrierTaskContext, SparkConf, SparkContext, TaskContext}

object treeAggregateTest{



  def main(args: Array[String]): Unit = {
    val cores = args(0)
    val conf = new SparkConf().setAppName("treeAggregateTest")
      .setMaster(s"local[${cores}]")
    val sc = new SparkContext(conf)
    val dataSize = 12
    val n = 5
    val maxIterations = 5
    val rdd = sc.parallelize(0 until n, n).cache()
    rdd.count()
    var avgTime = 0.0
    for (i <- 1 to maxIterations) {
      val start = System.nanoTime()
      val result = rdd.treeAggregate((new Array[Double](dataSize), 0.0, 0L))(
        seqOp = (c, v) => {
          // c: (grad, loss, count)
          val l = 0.2
          (c._1, c._2 + l, c._3 + 1)
        },
        combOp = (c1, c2) => {
          // c: (grad, loss, count)
//          val c1new = new Array[Double](dataSize)
          for( w <- 0 to 11 )
          {
            c1._1(w) += 2
          }
          (c1._1, c1._2 + c2._2, c1._3 + c2._3)

        })

      avgTime += (System.nanoTime() - start) / 1e9
      assert(result._1.length == dataSize)
      print("result: ")
      for( w <- 0 to result._1.length-1)
      {
        print(s"|||| ${result._1(w)}")
      }
      print("\n")
      print(s"batchsize: ${result._3}")
    }

    println("Avg time: " + avgTime / maxIterations)

  }
}