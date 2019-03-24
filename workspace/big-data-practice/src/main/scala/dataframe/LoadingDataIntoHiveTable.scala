package dataframe

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object LoadingDataIntoHiveTable extends App {
  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession.builder()
    .appName("LoadingDataIntoHiveTable")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster")) //dev
    //.enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val nyse_path = appConf.getConfig(args(1)).getString("nyse") //path

  val options = Map("header" -> "true", "inferSchema" -> "true")
  val nyseDF = sparkSession.read
    .options(options)
    .csv(nyse_path)
  nyseDF.printSchema()

}