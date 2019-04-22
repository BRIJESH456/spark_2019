package definitiveGuide

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions.{ col, column, expr , lit}

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

    jsonEmpData.select(

      jsonEmpData.col("first_name"),
      col("first_name"),
      column("first_name"),
      //'first_name,
      //$"first_name",
      expr("first_name")).show(5)

    //Renaming column name using as and alias

    jsonEmpData.select(expr("first_name as testing")).show(3)
    
    jsonEmpData.select(expr("first_name as testing2"))
      .alias("first_name").show(3)
      
      jsonEmpData.selectExpr("first_name as new_first_name", "first_name")
      .show(3)
      
      jsonEmpData.select(expr("*"), lit(1).as("One")).show(3)
      
      //withColumn , withColumnRenamed , drop, columns, cast, filter, where
      //distinct, union, sort, orderBy
      
  sparkSession.conf.set("spark.sql.caseSenitive", "True")
      

  }

}