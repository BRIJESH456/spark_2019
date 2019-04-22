package definitiveGuide

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.FloatType

import org.apache.spark.sql.functions.{ col, expr, lit }

object DataframePractise {

  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("DataframePractise")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

    val df = sparkSession.range(500).toDF("number")
    df.select(df.col("number") + 10).as("number") //.show(5)

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

    val categoriesSchema = StructType(List(
      StructField("category_id", IntegerType, false),
      StructField("category_department_id", IntegerType, false),
      StructField("category_name", StringType, false)))

    val customersSchema = StructType(List(
      StructField("customer_id", StringType, false),
      StructField("customer_fname", StringType, false),
      StructField("customer_lname", StringType, false),
      StructField("customer_email", StringType, false),
      StructField("customer_password", StringType, false),
      StructField("customer_street", StringType, false),
      StructField("customer_city", StringType, false),
      StructField("customer_state", StringType, false),
      StructField("customer_zipcode", StringType, false)))

    val departmentsSchema = StructType(List(
      StructField("department_id", IntegerType, false),
      StructField("department_name", StringType, false)))

    val productsSchema = StructType(List(
      StructField("product_id", IntegerType, false),
      StructField("product_category_id", IntegerType, false),
      StructField("product_name", StringType, false),
      StructField("product_description", StringType, false),
      StructField("product_price", FloatType, false),
      StructField("product_image", StringType, false)))

    val options = Map("inferSchema" -> "true", "header" -> "true")

    //How to read text file from local/source

    val ordersPath = appConf.getConfig(args(1)).getString("orders")
    val ordersDF = sparkSession.read
      .options(options)
      .schema(ordersSchema)
      .csv(ordersPath)

    ordersDF //.show(5)

    /*ordersDF.printSchema()
    ordersDF.schema.foreach(print)
    */

    //--Columns as expressions

    //--
    //(((col("someCol") + 5) * 200) - 6) < col("otherCol")

    //--Accessing a DataFrame’s columns
    //ordersDF.columns.foreach(println)

    //--first
    //ordersDF.first()

    //DataFrame Transformations
    val viewName = "Orders"
    ordersDF.createOrReplaceTempView(viewName)

    val sqlText = """
      select * from Orders      
      """
    sparkSession.sqlContext.sql(sqlText).show(1)

    ordersDF.select("order_id", "order_customer_id").show(1)

    //way to access dataframe column
    import sparkSession.sqlContext.implicits._
    ordersDF.select(
      ordersDF.col("order_id"),
      col("order_id"),
      'order_id,
      $"order_id",
      expr("order_id")).show(2)

    //ordersDF.select(expr("order_id AS ORDER_ID")).show(2)

    ordersDF.select(expr("order_id")).alias("testing").show(2)

    ordersDF.selectExpr("order_id as ORDER_ID", "order_id").show(2)

    ordersDF.selectExpr(
      "*", // include all original columns
      "(order_id = order_customer_id) as checking")
      .show(2)

    ordersDF.selectExpr("count(order_id) as count", "count(distinct(order_id)) as distinctCount").show(2)

    ordersDF.select(expr("*"), lit(1).as("One")).show(2)

    //--Adding Columns or rename column name

    ordersDF.withColumn("numberOne", lit(1)).show(2) //ADDING COLUMN AND RENAME COLUMN NAME
    ordersDF.withColumnRenamed("order_id", "ORDER_ID").columns.foreach(println) //RENAME COLUMN NAME

    sparkSession.conf.set("spark.sql.caseSensitive", "true")

    //Removing Columns
    println()
    ordersDF.drop("order_id").columns.foreach(println)

    //Changing a Column’s Type (cast)
    println()
    ordersDF.withColumn("order_id2", col("order_id").cast("long")).printSchema()

    //Filtering Rows

    df.filter(col("count") < 2).show(2)
    df.where("count < 2").show(2)

    //--Sorting Rows

    df.sort("count").show(5)
    df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

    import org.apache.spark.sql.functions.{ desc, asc }
    df.orderBy(expr("count desc")).show(2)
    df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

    sparkSession.read.format("json").load("/data/flight-data/json/*-summary.json")
      .sortWithinPartitions("count")

    //--limit

    //--Repartition and Coalesce
    ordersDF.rdd.getNumPartitions
    ordersDF.repartition(5)
    ordersDF.repartition(col("order_status"))

    ordersDF.repartition(5, col("order_status")).coalesce(2)

  }
}