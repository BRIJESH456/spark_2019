package datamatica

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object LogFileHive {
    def main(args: Array[String]): Unit = {

    //variables
    val name = "LogFile processing"
    val options = Map("header" -> "true", "inferSchema" -> "true")

    //creating SparkSession
    val appConf = ConfigFactory.load()
    val hiveContext = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .config("spark.sql.warehouse.dir","file:/C:/Users/Brijesh/workspace/big-data-practice/spark-warehouse/")
      .enableHiveSupport()
      .getOrCreate()

    //
    hiveContext.sparkContext.setLogLevel("ERROR")
    hiveContext.conf.set("spark.sql.shuffle.partitions", "5")
    import hiveContext.implicits._
    println(hiveContext.conf.get("spark.sql.warehouse.dir"))
    
    
    }
}