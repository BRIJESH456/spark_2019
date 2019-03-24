package transformation

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkTransformationByKey {
  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("SparkTransformationByKey")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val ordersPath = appConf.getConfig(args(1)).getString("orders")
    val ordersRDD = sparkSession.sparkContext.textFile(ordersPath, 2)

    val order_items_path = appConf.getConfig(args(1)).getString("order_items")
    val orderItemsRDD = sparkSession.sparkContext.textFile(order_items_path, 2)

    val products_path = appConf.getConfig(args(1)).getString("products")
    val productsRDD = sparkSession.sparkContext.textFile(products_path, 2)

    val orderItems = orderItemsRDD
      .map(orderItem => (orderItem.split(",")(1).toInt, orderItem.split(",")(4).toFloat))

    //Transformation: reduceByKey
    orderItems.reduceByKey((total, orderItemSubtotal) => total + orderItemSubtotal)
      .take(100)
      .foreach(println)

    //Transformation: aggregateByKey
    orderItems.
      aggregateByKey((0.0, 0))(
        (iTotal, oisubtotal) => (iTotal._1 + oisubtotal, iTotal._2 + 1),
        (fTotal, iTotal) => (fTotal._1 + iTotal._1, fTotal._2 + iTotal._2)).
        take(100).
        foreach(println)

    //Transformation: groupByKey
    val productsGroupByCategory = productsRDD.
      filter(product => product.split(",")(4) != "").
      map(product => {
        val p = product.split(",")
        (p(1).toInt, product)
      }).
      groupByKey

    //Transformation: sortByKey
    productsGroupByCategory.
      sortByKey().
      flatMap(rec => {
        rec._2.toList.sortBy(r => -r.split(",")(4).toFloat).take(5)
      }).
      take(100).
      foreach(println)
  }
}