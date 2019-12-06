package com.ringallreduce

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


// scalastyle:off println

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**
 * An example for Multilayer Perceptron Classification.
 */
object MultilayerPerceptronClassifierExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MultilayerPerceptronClassifierExample")
      .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm")
      .load("data/sample_multiclass_classification_data.txt")

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train = splits(0)
    val test = splits(1)

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 3)

    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val t0 = System.nanoTime()
    val model = trainer.fit(train)
    val t1 = System.nanoTime()
    println(s"Test time = ${t0} ${t1} ${t1-t0}")
////     compute accuracy on the test set
//    val result = model.transform(test)
//    val predictionAndLabels = result.select("prediction", "label")
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setMetricName("accuracy")
//
//    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
//    // $example off$

    spark.stop()
  }

// scalastyle:on println



}
