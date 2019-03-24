package spark_definitive_guide.chapter2

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object RetailDataByDay {
  def main(args: Array[String]): Unit = {
    //variables
    val name = "RetailDataByDay"
    val options = Map("inferSchema" -> "true", "header" -> "true")

    //Configuration External parameter
    val appConf = ConfigFactory.load()

    //SparkSession creation using spark2.x
    val sparkSession = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

    val retailData = appConf.getConfig(args(1)).getString("retail-data")

    val staticDataFrame = sparkSession.sqlContext.read.format("csv")
      .options(options)
      .load(retailData + "/*.csv")

    staticDataFrame.createOrReplaceTempView("retail_data")
    val staticSchema = staticDataFrame.schema

    import org.apache.spark.sql.functions.{ window, column, desc, col }
    /* staticDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .show(5)*/

    val streamingDataFrame = sparkSession.sqlContext.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(retailData + "/*.csv")

    println(streamingDataFrame.isStreaming)

    val purchaseByCustomerPerHour = streamingDataFrame
      .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")

    println("done.")

    /*    purchaseByCustomerPerHour.writeStream
      .format("memory") // memory = store in-memory table
      .queryName("customer_purchases") // the name of the in-memory table
      .outputMode("complete") // complete = all the counts should be in the table
      .start()*/

    purchaseByCustomerPerHour.writeStream
      .format("console")
      .queryName("customer_purchases_2")
      .outputMode("complete")
      .start()

  }

}