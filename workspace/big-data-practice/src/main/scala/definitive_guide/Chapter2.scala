package definitive_guide

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ expr, col, column, lit, max, desc, window }

case class Flight(
  DEST_COUNTRY_NAME:   String,
  ORIGIN_COUNTRY_NAME: String,
  count:               BigInt)

object Chapter2 {
  def main(args: Array[String]): Unit = {

    //variables
    val name = "Chapter2 : Dataframe"
    val appConf = ConfigFactory.load()

    val spark = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    spark.conf.set("spark.driver.port", "12345")
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", "5")
    //spark.conf.set("spark.sql.caseSensitive","true")
    import spark.sqlContext.implicits._

    //spark.conf.getAll.foreach(println)

    val myRange = spark.range(1000).toDF("number")
    val divisBy2 = myRange.where("number % 2 = 0")
    //divisBy2.count()

    val flightData = appConf.getConfig(args(1)).getString("csv") + "/2015-summary.csv"
    //println(flightData)

    val options = Map("header" -> "true", "inferSchema" -> "true")

    val flightData2015 = spark
      .read
      .options(options)
      .csv(flightData)

    flightData2015.printSchema()
    flightData2015.take(3)
    flightData2015.sort('count).explain(true)
    flightData2015.select(max('count)).take(1)

    flightData2015.createOrReplaceTempView("flightData2015")

    val sqlText = """
      select DEST_COUNTRY_NAME, sum(count) as destination_total
      from flightData2015
      group by DEST_COUNTRY_NAME
      order by sum(count) desc
      limit 5
      """

    val maxSql = spark.sqlContext.sql(sqlText)
    maxSql.show()

    flightData2015
      .groupBy('DEST_COUNTRY_NAME) //RelationalGroupedDataset
      .sum("count") //DataFrame
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .explain(true)

    //flightData2015.rdd.toDebugString.foreach(print)

    /*val flightParquet = appConf.getConfig(args(1)).getString("parquet")

    val flightDf = spark.read

      .parquet("file:/G:/spark_big_data_practice/software/Spark-The-Definitive-Guide-master/data/flight-data/parquet")

    val flights = flightDf.as[Flight]

    flights
      .filter(rec => rec.ORIGIN_COUNTRY_NAME != "Canada")
      .map(rec => rec)
      .take(5)*/

    val retailData = appConf.getConfig(args(1)).getString("retail-data-by-day")

    val statciDataFrame = spark.read.format("csv")
      .options(options)
      .load(retailData)

    statciDataFrame.printSchema()
    statciDataFrame.createOrReplaceTempView("statciDataFrame")
    val staticSchema = statciDataFrame.schema

    statciDataFrame
      .selectExpr(
        "CustomerID",
        "(UnitPrice * Quantity) as total_cost",
        "InvoiceDate")
      .groupBy(
        col("CustomerID"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .withColumnRenamed("sum(total_cost)", "total_cost")
      .show(5)

    val streamingDataFrame = spark.readStream
      .schema(staticSchema)
      .option("maxFilesPerTrigger", 1)
      .format("csv")
      .option("header", "true")
      .load(retailData)

    println(streamingDataFrame.isStreaming)

    val purchaseByCustomerPerHour = streamingDataFrame.selectExpr(
      "CustomerID",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
      .groupBy(
        col("CustomerID"), window(col("InvoiceDate"), "1 day"))
      .sum("total_cost")
      .withColumnRenamed("sum(total_cost)", "total_cost")

    /*purchaseByCustomerPerHour.writeStream
      .format("memory") //memory //console
      .queryName("customer-purchases")
      .outputMode("complete")
      .start()*/

    val df = spark.range(500).toDF("number")
    df.select('number,'number + 10).show()

    val a:Int = 0
    val b = Int
    
    print(a,b)
    
  }
}