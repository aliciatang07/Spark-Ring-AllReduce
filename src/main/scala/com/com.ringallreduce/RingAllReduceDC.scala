package com.ringallreduce

import java.util
import java.util.Collections

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{BarrierTaskContext, SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS
import org.apache.spark.mllib.util.MLUtils
import breeze.linalg.{diff, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}


import scala.collection.JavaConverters._
import scala.language.implicitConversions

// testing example

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.{Map=>MMap}
import scala.Double


/**
 * Ring Allreduce Divide and Conquer
 */
object RingAllReduceDC{

  class MapAccumulator(defaultMap: MMap[Int, Double] = MMap.empty[Int, Double].withDefaultValue(0))
    extends AccumulatorV2[(Int, Double), Map[Int, Double]] {

    private val _mmap = defaultMap

    override def reset(): Unit = _mmap.clear()

    override def add(v: (Int, Double)): Unit ={
      _mmap(v._1) += v._2
    }

    def set(v: (Int, Double)): Unit ={
      _mmap(v._1) = v._2
    }

    override def value: Map[Int, Double] = _mmap.toMap.withDefaultValue(0)

    override def copy(): AccumulatorV2[(Int, Double), Map[Int, Double]] =
      new MapAccumulator(MMap[Int, Double](value.toSeq:_*).withDefaultValue(0))

    override def isZero: Boolean = _mmap.isEmpty

    def mmap = _mmap

    override def merge(other: AccumulatorV2[(Int, Double), Map[Int, Double]]): Unit =
      other match {
        case o: MapAccumulator => o.mmap.foreach{case (k, v) => _mmap(k) += v}
        case _ => throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
      }
  }

  def main(args: Array[String]): Unit = {
    val cores = args(0)
    val conf = new SparkConf().setAppName("BarrierTest")
      .setMaster(s"local[${cores}]")
    //standalone spark://master:7077

    val sc = new SparkContext(conf)

    val v1 = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)
    val v2 = Array(9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0)
    val v3 = Array(17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0)
    val v4 = Array(25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0)
    val v5 = Array(33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0, 40.0)
    val v6 = Array(41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0)
    val v7 = Array(49.0, 50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0)
    val v8 = Array(57.0, 58.0, 59.0, 60.0, 61.0, 62.0, 63.0, 64.0)


    val n = cores.toInt/2
    val m = cores.toInt

    var rddA = sc.parallelize(Array(v1, v2, v3, v4),4)
    var rddB = sc.parallelize(Array(v5, v6, v7, v8),4)

    val MapAccumulator = new MapAccumulator()
    sc.register(MapAccumulator, "mapacc")
    var mapacc = MapAccumulator


//   rddA ring allreduce
val t0 = System.nanoTime()
    var cachearray = collection.Map[Int,Double]()

    var arr = rddA.barrier().mapPartitions(batches =>{
      var newlist = batches.toList
      val context = BarrierTaskContext.get();
      val partitionId = context.partitionId();
      val chunk_index = (partitionId+m)%m
      mapacc.add(chunk_index,newlist(0)(chunk_index))
      val chunk_index2 = (partitionId+m+m/2)%m
      mapacc.add(chunk_index2,newlist(0)(chunk_index2))
      context.barrier();
      newlist.iterator
    },preservesPartitioning = true).collect()
    rddA = sc.parallelize(arr,n)
    cachearray = mapacc.value
    //    cachearray.foreach { case (key, values) => println("key1 " + key + ":" + values)}

    //    handle case n<2 !
    if(n>=2){
      for (t <- 1 to n-1) {
        arr = rddA.barrier().mapPartitions(batches => {
          var newlist = batches.toList
          val context = BarrierTaskContext.get();
          val partitionId = context.partitionId();

          val chunk_index = (partitionId - t + m) % m
          val prev = cachearray.get(chunk_index)
          val oldval = newlist(0)(chunk_index)
          newlist(0)(chunk_index) += prev.getOrElse(newlist(0)(chunk_index))
          mapacc.add(chunk_index, oldval)

          val chunk_index2 = (partitionId - t + m+ m/2) % m
          val prev2 = cachearray.get(chunk_index2)
          val oldval2 = newlist(0)(chunk_index2)
          newlist(0)(chunk_index2) += prev2.getOrElse(newlist(0)(chunk_index2))
          mapacc.add(chunk_index2, oldval2)
          //        newlist(0)(chunk_index) =  mapacc.value.get(chunk_index)if mapacc.value.get(chunk_index)!=None else newlist(0)(chunk_index)
          context.barrier();
          newlist.iterator
        }, preservesPartitioning = true).collect()
        rddA = sc.parallelize(arr, n)
        cachearray = mapacc.value
        //        cachearray.foreach { case (key, values) => println("key1 " + key + ":" + values) }
      }
    }

    //
    val t1 = System.nanoTime()
    val scatterReducetime = t1-t0
    println(s"ScatterReducetime = ${scatterReducetime}")


    println("EEENNNDDD")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr(j)(k)}")
      }
      print("\n")
    }

    //
    //    //=======All gather=====
    //
    val t3 = System.nanoTime()
    //    here cache array should be fixed value
    val cachearray2 = mapacc.value
    var arr2 = rddA.collect()  //改进这里
    //    handle n<2
    for(t<-0 to n-2){
      arr2 = rddA.barrier().mapPartitions(batches =>{
        var newlist = batches.toList
        var iter = newlist.iterator
        val context = BarrierTaskContext.get();
        val partitionId = context.partitionId();
        val chunk_index = (partitionId-t+m)%m
        val prev = cachearray2.get(chunk_index)
        newlist(0)(chunk_index)= prev.getOrElse(newlist(0)(chunk_index))
        val chunk_index2 = (partitionId-t+m+m/2)%m
        val prev2 = cachearray2.get(chunk_index2)
        newlist(0)(chunk_index2)= prev2.getOrElse(newlist(0)(chunk_index2))
        context.barrier();
        newlist.iterator
      },preservesPartitioning = true).collect()
      rddA = sc.parallelize(arr2,n)

    }


    val t4 = System.nanoTime()
    val gathertime = t4-t3
    println(s"Gathertime = ${gathertime}")

    val totaltime = scatterReducetime+gathertime
    println(s"Totaltime = ${totaltime}")


    println("EEENNNDDD")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr2(j)(k)}")
      }
      print("\n")
    }




    //    rddB ring allreduce
    mapacc.reset()
    val t5 = System.nanoTime()
    var cachearray3 = collection.Map[Int,Double]()

    var arr3 = rddB.barrier().mapPartitions(batches =>{
      var newlist = batches.toList
      val context = BarrierTaskContext.get();
      val partitionId = context.partitionId();
      val chunk_index = (partitionId+m)%m
      mapacc.add(chunk_index,newlist(0)(chunk_index))
      val chunk_index2 = (partitionId+m+m/2)%m
      mapacc.add(chunk_index2,newlist(0)(chunk_index2))
      context.barrier();
      newlist.iterator
    },preservesPartitioning = true).collect()
    rddB = sc.parallelize(arr3,n)
    cachearray3 = mapacc.value


    //    handle case n<2 !
    if(n>=2){
      for (t <- 1 to n-1) {
        arr3 = rddB.barrier().mapPartitions(batches => {
          var newlist = batches.toList
          val context = BarrierTaskContext.get();
          val partitionId = context.partitionId();

          val chunk_index = (partitionId - t + m) % m
          val prev = cachearray3.get(chunk_index)
          val oldval = newlist(0)(chunk_index)
          newlist(0)(chunk_index) += prev.getOrElse(newlist(0)(chunk_index))
          mapacc.add(chunk_index, oldval)

          val chunk_index2 = (partitionId - t + m+ m/2) % m
          val prev2 = cachearray3.get(chunk_index2)
          val oldval2 = newlist(0)(chunk_index2)
          newlist(0)(chunk_index2) += prev2.getOrElse(newlist(0)(chunk_index2))
          mapacc.add(chunk_index2, oldval2)

          context.barrier();
          newlist.iterator
        }, preservesPartitioning = true).collect()
        rddB = sc.parallelize(arr3, n)
        cachearray3= mapacc.value

      }
    }

    //
    val t6 = System.nanoTime()
    val scatterReducetime2 = t6-t5
    println(s"ScatterReducetime2 = ${scatterReducetime2}")

    //
    //

    println("EEENNNDDD")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr3(j)(k)}")
      }
      print("\n")
    }

    //
    //    //=======All gather=====
    //
    val t7 = System.nanoTime()
    //    here cache array should be fixed value
    val cachearray4 = mapacc.value
    var arr4 = rddA.collect()  //改进这里
    //    handle n<2
    for(t<-0 to n-2){
      arr4 = rddB.barrier().mapPartitions(batches =>{
        var newlist = batches.toList
        var iter = newlist.iterator
        val context = BarrierTaskContext.get();
        val partitionId = context.partitionId();
        val chunk_index = (partitionId-t+m)%m
        val prev = cachearray4.get(chunk_index)
        newlist(0)(chunk_index)= prev.getOrElse(newlist(0)(chunk_index))
        val chunk_index2 = (partitionId-t+m+m/2)%m
        val prev2 = cachearray4.get(chunk_index2)
        newlist(0)(chunk_index2)= prev2.getOrElse(newlist(0)(chunk_index2))
        context.barrier();
        newlist.iterator
      },preservesPartitioning = true).collect()
      rddB = sc.parallelize(arr4,n)

    }


    val t8 = System.nanoTime()
    val gathertime2 = t8-t7
    println(s"Gathertime2 = ${gathertime2}")

    val totaltime2 = scatterReducetime2+gathertime2
    println(s"Totaltime2 = ${totaltime2}")


    println("EEENNNDDD")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr4(j)(k)}")
      }
      print("\n")
    }



//    merege action
//  merge rddA
    val t9 = System.nanoTime()

    for(t<-0 to n-1){
      arr2 = rddA.barrier().mapPartitions(batches =>{
        var newlist = batches.toList
        var iter = newlist.iterator
        val context = BarrierTaskContext.get();
        val partitionId = context.partitionId();
        val chunk_index = (partitionId-t+m)%m
        val prev = cachearray4.get(chunk_index)
        newlist(0)(chunk_index)+= prev.getOrElse(newlist(0)(chunk_index))
        val chunk_index2 = (partitionId-t+m+m/2)%m
        val prev2 = cachearray4.get(chunk_index2)
        newlist(0)(chunk_index2)+= prev2.getOrElse(newlist(0)(chunk_index2))
        context.barrier();
        newlist.iterator
      },preservesPartitioning = true).collect()
      rddA = sc.parallelize(arr2,n)
    }


    val t10 = System.nanoTime()
    val mergetime1 = t10-t9
    println(s"MERGETIME1 = ${mergetime1}")


    println("EEENNNDDD")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr2(j)(k)}")
      }
      print("\n")
    }

//    merge rddB

    val t11 = System.nanoTime()

    for(t<-0 to n-1){
      arr4 = rddB.barrier().mapPartitions(batches =>{
        var newlist = batches.toList
        var iter = newlist.iterator
        val context = BarrierTaskContext.get();
        val partitionId = context.partitionId();
        val chunk_index = (partitionId-t+m)%m
        val prev = cachearray2.get(chunk_index)
        newlist(0)(chunk_index)+= prev.getOrElse(newlist(0)(chunk_index))
        val chunk_index2 = (partitionId-t+m+m/2)%m
        val prev2 = cachearray2.get(chunk_index2)
        newlist(0)(chunk_index2)+= prev2.getOrElse(newlist(0)(chunk_index2))
        context.barrier();
        newlist.iterator
      },preservesPartitioning = true).collect()
      rddB = sc.parallelize(arr4,n)
    }


    val t12 = System.nanoTime()
    val mergetime2 = t12-t11
    println(s"MERGETIME2 = ${mergetime2}")

    println("FINAL RESULT")
    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr2(j)(k)}")
      }
      print("\n")
    }

    for (j <-0 to n-1) {
      for (k <- 0 to m-1) {
        print(s"|||| ${arr4(j)(k)}")
      }
      print("\n")
    }

    val totalDCtime = totaltime+totaltime2+mergetime1+mergetime2
    println(s"TotalDCtime ${totalDCtime}")






  }



  //  def gradfun(p:LabeledPoint, w: Vector) ={
  //    println("print")
  ////    val z = toBreeze(p.features)
  //    val z = new BDV[Double](p.features.toArray)
  //    val y= p.label
  ////    println("z.t :%d",z.t)
  //    val scalar = diff(z.t * w, y)
  //    System.gc()
  //    val grad = z*scalar
  //    println("grad:\n")
  //    println("z:\n")
  //    grad
  //  }


  //  implicit def toBreeze( v: Vector ): BV[Double] =
  //    v match {
  //      case dv: DenseVector => new BDV[Double](dv.values)
  //      case sv: SparseVector => new BSV[Double](sv.indices, sv.values, sv.size)
  //    }
  //
  //  implicit def fromBreeze( dv: BDV[Double] ): DenseVector =
  //    new DenseVector(dv.toArray)
  //
  //  implicit def fromBreeze( sv: BSV[Double] ): SparseVector =
  //    new SparseVector(sv.length, sv.index, sv.data)
  //
  //  implicit def fromBreeze( bv: BV[Double] ): Vector =
  //    bv match {
  //      case dv: BDV[Double] => fromBreeze(dv)
  //      case sv: BSV[Double] => fromBreeze(sv)
  //    }
}

//object BreezeConverters
//{
//
//  implicit def toBreeze( v: Vector ): BV[Double] =
//    v match {
//      case dv: DenseVector => new BDV[Double](dv.values)
//      case sv: SparseVector => new BSV[Double](sv.indices, sv.values, sv.size)
//    }
//
//  implicit def fromBreeze( dv: BDV[Double] ): DenseVector =
//    new DenseVector(dv.toArray)
//
//  implicit def fromBreeze( sv: BSV[Double] ): SparseVector =
//    new SparseVector(sv.length, sv.index, sv.data)
//
//  implicit def fromBreeze( bv: BV[Double] ): Vector =
//    bv match {
//      case dv: BDV[Double] => fromBreeze(dv)
//      case sv: BSV[Double] => fromBreeze(sv)
//    }
//}