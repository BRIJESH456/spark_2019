package definitiveGuide

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ch4_structure_api_overview {
  
    def main(args: Array[String]): Unit = {
    println("Hello From SparkCourse!!!")
    
    val appConf = ConfigFactory.load()
    
    val sparkSession = SparkSession.builder()
    .appName("ch4_structure_api_overview")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
    .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")
    
    
    
    }
  
}