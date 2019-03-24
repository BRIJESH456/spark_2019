package datamatica

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ concat_ws, collect_list, col }

object Log extends App {
  val spark = SparkSession.builder()
    .appName("Log")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.shuffle.partitions", "5")
  

  val input = "file:///G://spark_big_data_practice//data//datamatica//log.csv"
  //val input = "file:///G://spark_big_data_practice//data//log.csv"

  val options = Map("header" -> "true", "inferSchema" -> "true")
  
  import spark.implicits._

  val logDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input)

  logDF.groupBy("userid","productid")
    .agg(collect_list('action))
    .withColumnRenamed("collect_list(action)", "derived")
    .withColumn("test", concat_ws(" ",col("derived")))
    .filter(!col("test").like("%PURCHASE%"))
    .filter(col("test").like("%ADDTOCART%"))
    .select('userid,'productid)
    .show()

 /* logDF.createOrReplaceTempView("log")

  val sqlText = """
      
select userid, productid
from (
      select userid, productid , concat_ws(" ",derived) as test
from (
select userid, productid, collect_list(action) as derived
      from log
      group by userid, productid
)
) where (test not like "%PURCHASE%" and test  like "%ADDTOCART%")
     
      
      
      """

  spark.sqlContext.sql(sqlText).show()*/

  println("--done--")

}