package datamatica

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{ concat_ws, collect_list, col }

object LogFile {
  def main(args: Array[String]): Unit = {

    //variables
    val name = "LogFile processing"
    val options = Map("header" -> "true", "inferSchema" -> "true")

    //creating SparkSession
    val appConf = ConfigFactory.load()
    val spark = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    //
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    import spark.implicits._

    //input and output
    val logDataInputPath = appConf.getConfig(args(1)).getString("log-data-input")
    val logDataOutputPath = appConf.getConfig(args(1)).getString("log-data-output")

    val logDF = spark.read
      .options(options)
      .csv(logDataInputPath)

    //Spark Dataframe

    val processedData = logDF.groupBy("userid", "productid")
      .agg(collect_list('action))
      .withColumnRenamed("collect_list(action)", "derived")
      .withColumn("test", concat_ws(" ", col("derived")))
      .filter(!col("test").like("%PURCHASE%"))
      .filter(col("test").like("%ADDTOCART%"))
      .select('userid, 'productid)
    //.show()

    //storing
    /*processedData.write.format("csv")
      .options(options)
      .save(logDataOutputPath)*/

    processedData.write.text("file:///home/cloudera/Desktop/data/output")

  }
}