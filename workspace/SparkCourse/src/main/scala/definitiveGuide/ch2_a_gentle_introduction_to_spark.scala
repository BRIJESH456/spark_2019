package definitiveGuide

import org.apache.spark.sql.SparkSession

object ch2_a_gentle_introduction_to_spark {
  def main(args : Array[String]) : Unit = {
    val sparkSession = SparkSession.builder()
    .appName("ch2_a_gentle_introduction_to_spark")
    .master("local[2]")
    .getOrCreate()
    
    sparkSession.sparkContext.setLogLevel("ERROR") 
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")
    
    val myRange = sparkSession.range(100)
    .toDF("number")
    
    myRange.show(10)
    
    println("------------------------------")
    var condition1 = "number % 2 = 0"
    val evenNumber = myRange.where(condition1)
    evenNumber.show(10)
    
    
    var condition2 = "number % 2 != 0"
    val oddNumber = myRange.where(condition2)
    oddNumber.show(10)
    
    println("------------------------------")
    
    var csvFile = "file:/G:/spark_big_data_practice/software/Spark-The-Definitive-Guide-master/data/flight-data/csv/2015-summary.csv"
    
    
    val flightData2015 = sparkSession.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(csvFile).printSchema()
    
    //println(sparkSession.sparkContext.getConf.getAll)
        
    
    /*root
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- count: integer (nullable = true)*/
    
    //val groupByDestCountryName = flightData2015
    
  }
}