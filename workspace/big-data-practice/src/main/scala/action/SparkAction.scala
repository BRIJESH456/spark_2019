package action

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SparkAction {
  def main(args: Array[String]): Unit = {

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("SparkAction")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val ordersPath = appConf.getConfig(args(1)).getString("orders")
    val ordersRDD = sparkSession.sparkContext.textFile(ordersPath, 2)

    //Action: first
    ordersRDD.first().foreach(print)

    //Action: collect
    ordersRDD.collect().foreach(println)

    //Action: take
    ordersRDD.take(10).foreach(println)

    //Action: takeSample
    ordersRDD.takeSample(true, 10).foreach(println)

    //Action: takeOrdered
    ordersRDD.takeOrdered(10)(Ordering[Int].reverse.on(x => x.split(",")(0).toInt)).foreach(println)

    //Action: countByKey
    val statusCount = ordersRDD.map(rec => (rec.split(",")(3), " ")).countByKey()
    statusCount.keys.foreach({
      key => println(key, statusCount(key))
    })

    //Action: reduce
    ordersRDD.reduce((agg, ele) => {
      if (agg.split(",")(2).toInt < ele.split(",")(2).toInt) agg else ele
    }).foreach(print)

  }

}