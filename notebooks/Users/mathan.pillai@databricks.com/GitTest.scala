// Databricks notebook source
import org.apache.spark.sql.functions.{hash, lit}
import org.apache.spark.sql._

val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5))             // rdd with values from 1 to 5
val dataset1 = rdd.toDS()                                            // creating dataset1 from rdd using toDS() method
val dataset2 = dataset1.repartition(partitionExprs = hash('value))
display(dataset2)