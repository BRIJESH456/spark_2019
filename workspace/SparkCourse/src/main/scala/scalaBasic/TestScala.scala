package scalaBasic

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object TestScala {
  def main(args: Array[String]): Unit = {
    println("Hello From SparkCourse!!!")
    
    val appConf = ConfigFactory.load()
    
    val sparkSession = SparkSession.builder()
    .appName("Test SparkCourse")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
    .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._
  println("---START OF CODE---")
  
    println(sparkSession.getClass)
    
  println("---END OF CODE---")
  }
}