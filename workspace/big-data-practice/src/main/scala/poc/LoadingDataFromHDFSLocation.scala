package poc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.typesafe.config.ConfigFactory

object LoadingDataFromHDFSLocation extends App {
  val appConf = ConfigFactory.load()
  val spark = SparkSession.builder()
    .appName("LoadingDataFromHDFSLocation")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val orders_path = appConf.getConfig(args(1)).getString("orders")
  val order_items_path = appConf.getConfig(args(1)).getString("order_items")

  val ordersSchema = StructType(List(
    StructField("order_id", IntegerType, false),
    StructField("order_date", DateType, false),
    StructField("order_customer_id", IntegerType, false),
    StructField("order_status", StringType, false)))

  val orderitemsSchema = StructType(List(
    StructField("order_item_id", IntegerType, false),
    StructField("order_item_order_id", IntegerType, false),
    StructField("order_item_product_id", IntegerType, false),
    StructField("order_item_quantity", IntegerType, false),
    StructField("order_item_subtotal", FloatType, false),
    StructField("order_item_product_price", FloatType, false)))

  val orderesDF = spark.sqlContext.read
    .schema(ordersSchema)
    .option("delimiter", ",")
    .csv(orders_path)

  val ordereItemsDF = spark.sqlContext.read
    .schema(orderitemsSchema)
    .option("delimiter", ",")
    .csv(order_items_path)

  orderesDF.createOrReplaceTempView("orders")
  ordereItemsDF.createOrReplaceTempView("orderItems")

  val query = "select " +
    "substr(o.ORDER_DATE,1,10) ORDER_DATE, " +
    "o.ORDER_STATUS, " +
    "count(o.ORDER_ID) as ORDER_ID, " +
    "round(sum(oi.ORDER_ITEM_SUBTOTAL),2) ORDER_ITEM_SUBTOTAL  " +
    "from orders o join orderItems oi on (o.ORDER_ID = oi.ORDER_ITEM_ORDER_ID ) " +
    "group by o.ORDER_STATUS,o.ORDER_DATE " +
    "order by o.ORDER_DATE desc, o.ORDER_STATUS desc,ORDER_ITEM_SUBTOTAL "

  val output = spark.sqlContext.sql(query)

  output.printSchema()
  //output.show()

  //output.write.mode("Overwrite").parquet("")
  //output.write.mode("Overwrite").format("avro").save("")
  //output.write.mode("Overwrite").json("")
  //output.write.mode("Overwrite").csv("")
  //output.write.mode("Overwrite").format("orc").save("")

  /*  val options = Map("url" -> "jdbc:oracle:thin:@localhost:1521:xe", "dbtable" -> "TESTING_TMP", "user" -> "system", "password" -> "oracle", "driver" -> "oracle.jdbc.OracleDriver")
  output.write
    .format("jdbc")
    .options(options)
    .mode("Overwrite")*/

  println("inserting")
  /*  val options = Map("url" -> "jdbc:oracle:thin:@localhost:1521:xe", "user" -> "system", "password" -> "oracle", "driver" -> "oracle.jdbc.driver.OracleDriver", "dbtable" -> "output")
  output.write
    .format("jdbc")
    .options(options)
    .mode("append")*/

  //create properties object
  val prop = new java.util.Properties
  prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
  prop.setProperty("user", "system")
  prop.setProperty("password", "oracle")

  //jdbc mysql url - destination database is named "data"
  val url = "jdbc:oracle:thin:@localhost:1521:xe"

  //destination database table
  val table = "output"

  //write data from spark dataframe to database
  output.write.mode("append").jdbc(url, table, prop)

  println("inserted")
}