package rdd

import org.apache.spark.sql.SparkSession

object CreatingRDDUsingParallizeMethod {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("CreatingRDDUsingParallizeMethod")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val sc = sparkSession.sparkContext

    val numSlices = 1

    val array = Array(1, 2, 3, 4, 5, 6, 7)

    val arrayRDD = sc.parallelize(array, numSlices)

    println("****first()****")
    println(arrayRDD.first())
    println()
    println("****take()****")

    arrayRDD.take(3).foreach(println)
    println()
    println("****collect()****")

    arrayRDD.collect().foreach(println)

    println()
    println("****toDebugString****")
    println(arrayRDD.toDebugString)

    println()
    println("****getConf.getAll****")
    sc.getConf.getAll.foreach(println)

  }
}