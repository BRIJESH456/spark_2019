package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object SparkMethods extends App {
  val spark = SparkSession
    .builder()
    .appName("SparkMethods")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  
  //
  //case class + map
  
  //
  spark.sqlContext.read

  
  //
  val path = ""
  val ordersRDD = spark.sparkContext.textFile(path)
  val rows = ordersRDD.map(rec => Row(ordersRDD))
  val schema = StructType(List(
    StructField("order_id", IntegerType, false),
    StructField("order_date", StringType, false),
    StructField("order_customer_id", IntegerType, false),
    StructField("order_status", StringType, false)))

  val df = spark.sqlContext.createDataFrame(rows, schema)
  

}