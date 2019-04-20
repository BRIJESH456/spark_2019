package definitiveGuide

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions.{col, column}

object ch5_basic_structure_operations {

  def main(args: Array[String]): Unit = {
    println("Hello From SparkCourse!!!")

    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("ch4_structure_api_overview")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

    var jsonFile = appConf.getConfig(args(1)).getString("json")

    //println(jsonFile)

    /*    #way1
    val jsonEmpData = sparkSession.read.format("json")
      .load(jsonFile) //.schema


    //jsonEmpData.foreach(println)

     */
    var schema = StructType(
      List(
        StructField("email", StringType, true),
        StructField("first_name", StringType, true),
        StructField("gender", StringType, true),
        StructField("id", LongType, true),
        StructField("ip_address", StringType, true),
        StructField("last_name", StringType, true)))

    val jsonEmpData = sparkSession.read.format("json")
      .schema(schema)
      .load(jsonFile)

    jsonEmpData.printSchema()
    
    //Operation on Columns and Expressions
    

    //Operation on select and selectExpr
    jsonEmpData.select("first_name", "ip_address").show(2)
    

  }

}