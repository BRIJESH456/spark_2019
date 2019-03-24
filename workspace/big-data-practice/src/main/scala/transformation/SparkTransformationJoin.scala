package transformation

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object SparkTransformationJoin {

  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("SparkTransformationJoin")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val ordersPath = appConf.getConfig(args(1)).getString("orders")
    val ordersRDD = sparkSession.sparkContext.textFile(ordersPath, 2)

    val order_items_path = appConf.getConfig(args(1)).getString("order_items")
    val orderItemsRDD = sparkSession.sparkContext.textFile(order_items_path, 2)

    val products_path = appConf.getConfig(args(1)).getString("products")
    val productsRDD = sparkSession.sparkContext.textFile(products_path, 2)

    val orders = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec))
    val orderItems = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec))

    //Transformation: join
    println("--------------------------Transformation: join--------------------------")
    val ordersJoin = orders.join(orderItems).take(10).foreach(println)

    //Transformation: leftOuterJoin
    println("--------------------------Transformation: leftOuterJoin--------------------------")
    val ordersLeftOuter = orders.leftOuterJoin(orderItems).take(10).foreach(println)

    //Transformation: rightOuterJoin
    println("--------------------------Transformation: rightOuterJoin--------------------------")
    val ordersRightOuter = orders.rightOuterJoin(orderItems).take(10).foreach(println)

    //Transformation: cogroup
    println("--------------------------Transformation: cogroup--------------------------")
    val ordersCogroup = orders.cogroup(orderItems).take(10).foreach(println)

    //Transformation: cartesian
    println("--------------------------Transformation: cartesian--------------------------")
    val a = sparkSession.sparkContext.parallelize(List(1, 2, 3, 4))
    val b = sparkSession.sparkContext.parallelize(List("Hello", "World"))

    a.cartesian(b).foreach(println)

  }
}