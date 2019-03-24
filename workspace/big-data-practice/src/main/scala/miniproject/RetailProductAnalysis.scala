package miniproject

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType

object RetailProductAnalysis {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("RetailProductAnalysis")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.implicits._

    val orders_path = appConf.getConfig(args(1)).getString("orders")
    val order_items_path = appConf.getConfig(args(1)).getString("order_items")
    val categories_path = appConf.getConfig(args(1)).getString("categories")
    val customers_path = appConf.getConfig(args(1)).getString("customers")
    val departments_path = appConf.getConfig(args(1)).getString("departments")
    val products_path = appConf.getConfig(args(1)).getString("products")

    //sparkSession.sparkContext.getConf.getAll.foreach(println)

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

    val orderesDF = sparkSession.sqlContext.read
      .schema(ordersSchema)
      .option("delimiter", ",")
      .csv(orders_path)

    val ordereItemsDF = sparkSession.sqlContext.read
      .schema(orderitemsSchema)
      .option("delimiter", ",")
      .csv(order_items_path)

    val categoriesDF = sparkSession.sqlContext.read
      .schema(categoriesSchema)
      .option("delimiter", ",")
      .csv(categories_path)

    val customersDF = sparkSession.sqlContext.read
      .schema(customersSchema)
      .option("delimiter", ",")
      .csv(customers_path)

    val departmentsDF = sparkSession.sqlContext.read
      .schema(departmentsSchema)
      .option("delimiter", ",")
      .csv(departments_path)

    val productsDF = sparkSession.sqlContext.read
      .schema(productsSchema)
      .option("delimiter", ",")
      .csv(products_path)

    orderesDF.createOrReplaceTempView("order")
    ordereItemsDF.createOrReplaceTempView("orderItems")
    categoriesDF.createOrReplaceTempView("categories")
    customersDF.createOrReplaceTempView("customers")
    departmentsDF.createOrReplaceTempView("departments")
    productsDF.createOrReplaceTempView("products")

    val query = "select " +
      "o.order_date as order_date, " +
      "CONCAT(CONCAT(cm.customer_fname,' '),cm.customer_lname) as full_name, " +
      "p.product_name as product_name, " +
      "oi.order_item_quantity as order_item_quantity, " +
      "oi.order_item_product_price, " +
      "oi.order_item_subtotal as order_item_subtotal, " +
      "o.order_status as order_status " +
      "from customers cm " +
      "left outer join order o on (cm.customer_id = o.order_customer_id) " +
      "left outer join orderItems oi on (o.order_id = oi.order_item_order_id) " +
      "left outer join products p on (oi.order_item_product_id = p.product_id) " +
      "left outer join categories c on (p.product_category_id = c.category_id) " +
      "left outer join departments d on (c.category_department_id = d.department_id) " +
      "where o.order_status ='COMPLETE' " +
      "order by oi.order_item_subtotal desc"

    val output = sparkSession.sqlContext.sql(query)
    output.show()
  }
}