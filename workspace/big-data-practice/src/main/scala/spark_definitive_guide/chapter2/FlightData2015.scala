package spark_definitive_guide.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max

import com.typesafe.config.ConfigFactory



object FlightData2015 {
  def main(args: Array[String]): Unit = {
    //variables
    val name = "FlightData2015"
    val options = Map("inferSchema" -> "true", "header" -> "true")
    //Configuration External parameter
    val appConf = ConfigFactory.load()

    //SparkSession creation using spark2.x
    val sparkSession = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    //println(sparkSession.sqlContext.getConf("spark.sql.shuffle.partitions")) // default:200
    //println(sparkSession.conf.get("spark.sql.shuffle.partitions")) // default:200

    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")
    //println(sparkSession.conf.get("spark.sql.shuffle.partitions")) //custom

    val flightData = appConf.getConfig(args(1)).getString("flight-data")
    val flightData2015 = flightData + "/csv/2015-summary.csv"

    val flightData2015DF = sparkSession.sqlContext.read
      .options(options)
      .csv(flightData2015)

    //flightData2015DF.sort("count")

    flightData2015DF.createOrReplaceTempView("flight_data_2015")

    val sqlWay = sparkSession.sqlContext.sql("""
      SELECT DEST_COUNTRY_NAME, count(1)
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      """)
    val dataFrameWay = flightData2015DF
      .groupBy("DEST_COUNTRY_NAME")
      .count()

    sqlWay.explain
    dataFrameWay.explain
    
    flightData2015DF.select(max("count").as("total_count")).show()
    
    //find the top five destination countries in the data
    val maxSql = sparkSession.sqlContext.sql("""
      SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
      FROM flight_data_2015
      GROUP BY DEST_COUNTRY_NAME
      ORDER BY sum(count) DESC
      LIMIT 5
      """)
      maxSql.explain
      maxSql.show()

      import org.apache.spark.sql.functions.desc
      
      flightData2015DF
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain()
  }
}