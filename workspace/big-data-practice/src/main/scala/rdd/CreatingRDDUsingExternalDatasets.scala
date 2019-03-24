package rdd

import org.apache.spark.sql.SparkSession

object CreatingRDDUsingExternalDatasets {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CreatingRDDUsingExternalDatasets")
      .master("local")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val sc = sparkSession.sparkContext

    val path = "E:\\Hadoop\\learning\\hadoop\\data-master\\data\\retail_db\\orders\\orders.txt"
    val minPartitions = 1
    val ordersRDD = sparkSession.sparkContext.textFile(path, minPartitions)

    ordersRDD.take(10).foreach(println)
  }
}