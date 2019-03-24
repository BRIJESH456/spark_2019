package action

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SparkTransformation {
  def main(args: Array[String]): Unit = {

    val name = ("sparkSession1", "sparkSession2")
    val logLevel = "ERROR"

    val appConf = ConfigFactory.load()
    println("-------sparkSession1-------")
    val sparkSession1 = SparkSession.builder()
      .appName(name._1)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster")) //dev
      .getOrCreate()

    //sparkSession1.sparkContext.getConf.getAll.foreach(println)
    sparkSession1.sparkContext.setLogLevel(logLevel)

    /*println("-------sparkSession2-------")
    val sparkSession2 = SparkSession.builder()
      .appName(name._2)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster")) //dev
      .getOrCreate()

    sparkSession2.sparkContext.getConf.getAll.foreach(println)*/

    import sparkSession1.implicits._
    //import sparkSession1.sqlContext.implicits._

    val ordersPath = appConf.getConfig(args(1)).getString("orders") //path
    //println(ordersPath)

    val minPartitions = 2

    val ordersRDD = sparkSession1.sparkContext.textFile(ordersPath, minPartitions)

    /*println(ordersRDD.getClass)
    println(ordersRDD.getStorageLevel)
    println(ordersRDD.getNumPartitions)*/

    //Spark Action:
    //converting to array and preview data – collect, take, first, takeSample
    //ordersRDD.first().foreach(print)
    //ordersRDD.collect().foreach(println)
    //ordersRDD.take(10).foreach(println)
    //ordersRDD.takeSample(true, 10).foreach(println)
    //ordersRDD.takeOrdered(10)(Ordering[Int].reverse.on(x=>x.split(",")(0).toInt)).foreach(println)
    //println(ordersRDD.count())

    /* val statusCount = ordersRDD.map(rec => (rec.split(",")(3), " ")).countByKey()
    //val Count : Map[String,String] = Map()
    statusCount.keys.foreach({
      key => println(key, statusCount(key))
    })*/

    /*ordersRDD.reduce((agg, ele) => {
      if (agg.split(",")(2).toInt < ele.split(",")(2).toInt) agg else ele
    }).foreach(print)*/

    //Spark Transformation : filter
    val order_items_path = appConf.getConfig(args(1)).getString("order_items")
    val orderItemsRDD = sparkSession1.sparkContext.textFile(order_items_path, minPartitions)
    val orderItems = orderItemsRDD.map(orderItem => (orderItem.split(",")(1).toInt, orderItem.split(",")(4).toFloat))

    // Compute revenue for each order
    /* orderItems.
      reduceByKey((total, orderItemSubtotal) => total + orderItemSubtotal).
      take(100).
      foreach(println)*/

    // Compute revenue and number of items for each order using aggregateByKey
    /* orderItems.
      aggregateByKey((0.0, 0))(
        (iTotal, oisubtotal) => (iTotal._1 + oisubtotal, iTotal._2 + 1),
        (fTotal, iTotal) => (fTotal._1 + iTotal._1, fTotal._2 + iTotal._2)).
        take(100).
        foreach(println)*/

    // Compute revenue and number of items for each order using reduceByKey
    /* orderItemsRDD.
      map(orderItem => (orderItem.split(",")(1).toInt, (orderItem.split(",")(4).toFloat, 1))).
      reduceByKey((total, element) => (total._1 + element._1, total._2 + element._2)).
      take(100).
      foreach(println)
*/
    //to get the minimum priced product per product category.
    val products_path = appConf.getConfig(args(1)).getString("products")
    val productsRDD = sparkSession1.sparkContext.textFile(products_path, minPartitions)

    /*   val minPricedProductsByCategory = productsRDD.
      filter(product => product.split(",")(4) != "").
      map(product => {
        val p = product.split(",")
        (p(1).toInt, product)
      }).
      reduceByKey((agg, product) => {
        if (agg.split(",")(4).toFloat < product.split(",")(4).toFloat)
          agg
        else
          product
      }).
      map(rec => rec._2).collect.foreach(println)*/

    val orders = ordersRDD.map(rec => (rec.split(",")(0).toInt, rec))
    val orderItems2 = orderItemsRDD.map(rec => (rec.split(",")(1).toInt, rec))

    //val ordersJoin = orders.join(orderItems2).take(10).foreach(println)
    val ordersLeftOuter = orders.leftOuterJoin(orderItems2)

    //ordersLeftOuter.filter(rec => rec._2._2 == None).take(10).foreach(println)
    /*ordersLeftOuter.
      filter(rec => rec._2._2 == None).
      map(rec => rec._2._1).
      take(10).
      foreach(println)*/

    val ordersCogroup = orders.cogroup(orderItems2)
    //ordersCogroup.take(10).foreach(println)

    val a = sparkSession1.sparkContext.parallelize(List(1, 2, 3, 4))
    val b = sparkSession1.sparkContext.parallelize(List("Hello", "World"))
    //a.cartesian(b).foreach(println)

    val orders201312 = ordersRDD.
      filter(order => order.split(",")(1).contains("2013-12")).
      map(order => (order.split(",")(0).toInt, order.split(",")(1)))

    val orderItems3 = orderItemsRDD.
      map(rec => (rec.split(",")(1).toInt, rec.split(",")(2).toInt))

    val distinctProducts201312 = orders201312.
      join(orderItems3).
      map(order => order._2._2).
      distinct

    val orders201401 = ordersRDD.
      filter(order => order.split(",")(1).contains("2014-01")).
      map(order => (order.split(",")(0).toInt, order.split(",")(1)))

    val products201312 = orders201312.
      join(orderItems3).
      map(order => order._2._2)

    val products201401 = orders201401.
      join(orderItems3).
      map(order => order._2._2)

    products201312.union(products201401).count
    products201312.union(products201401).distinct.count

    products201312.intersection(products201401).count

    // orders sorted by status
    ordersRDD.
      map(order => {
        val o = order.split(",")
        (o(3), order)
      }).
      sortByKey().
      map(_._2).
      take(100).
      foreach(println)

    // orders sorted by status and date in descending order
    ordersRDD.
      map(order => {
        val o = order.split(",")
        ((o(3), o(1)), order)
      }).
      sortByKey(false).
      map(_._2).
      take(100).
      foreach(println)

    // let us get top 5 products in each category from products

    val productsGroupByCategory = productsRDD.
      filter(product => product.split(",")(4) != "").
      map(product => {
        val p = product.split(",")
        (p(1).toInt, product)
      }).
      groupByKey

    productsGroupByCategory.
      sortByKey().
      flatMap(rec => {
        rec._2.toList.sortBy(r => -r.split(",")(4).toFloat).take(5)
      }).
      take(100).
      foreach(println)

  }
}