package dataset

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

case class Credits(cast: String, crew: String, id: String)

object DfvsDSPerformance extends App {
  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession.builder()
    .appName("DfvsDSPerformance")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster")) //dev
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val credits_path = appConf.getConfig(args(1)).getString("credits") //path
  var startTime: Long = 0
  var endTime: Long = 0

  val options = Map("header" -> "true", "inferSchema" -> "true")

  val creditsDF = sparkSession.sqlContext.read
    .options(options)
    .csv(credits_path)

  creditsDF.printSchema()
  startTime = System.currentTimeMillis()
  val creditsDFFilters = creditsDF.filter("id > 1000")
  println("IDs greater than 1000 :" + creditsDFFilters.count())
  endTime = System.currentTimeMillis()

  println("Time to calculate with DF:" + (endTime - startTime) / 1000.0)

  val creditsDS = sparkSession.sqlContext.read
    .options(options)
    .csv(credits_path)
    .as[Credits]

  creditsDS.printSchema()
  //creditsDS.select($"id").show()
  //creditsDS.selectExpr("id").show()
  //creditsDS.select("id").show()
  
  
  startTime = System.currentTimeMillis()
  val creditsDSFilters = creditsDF.filter("id > 1000")
  println("IDs greater than 1000 :" + creditsDSFilters.count())
  endTime = System.currentTimeMillis()

  println("Time to calculate with DS:" + (endTime - startTime) / 1000.0)
}