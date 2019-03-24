package fileformat

import org.apache.spark.sql.SparkSession

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.Row

//case class
case class Txt2(
  id:         Integer,
  first_name: String,
  last_name:  String,
  email:      String,
  gender:     String,
  ip_address: String)
/*
Spark Dataframe:
1. How to create dataframe
2. How to read and write different file format
3. How to read and write table from databases.
3. How to read and write table from hive
*/
object ReadingDifferentFileformat {
  def main(args: Array[String]): Unit = {
    //variables
    val name = "ReadingDifferentFileformat"
    val options = Map("inferSchema" -> "true", "header" -> "true")
    //Configuration External parameter
    val appConf = ConfigFactory.load()

    //SparkSession creation using spark2.x
    val sparkSession = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    //reading csv fileformat
    println("--------------------------CSV--------------------------")
    val csv = appConf.getConfig(args(1)).getString("csv")
    val csvDF = sparkSession.sqlContext.read.options(options).csv(csv)
    csvDF.printSchema()

    //reading json fileformat
    println("--------------------------JOSN--------------------------")
    val json = appConf.getConfig(args(1)).getString("json")
    val jsonDF = sparkSession.sqlContext.read.options(options).json(json)
    jsonDF.printSchema()

    //reading txt fileformat
    println("--------------------------TXT--------------------------")
    def transformRow(r: Row): Txt = {
      def getStr(r: Row, i: Int) = if (!r.isNullAt(i)) Some(r.getString(i)) else None
      def getInt(r: Row, i: Int) = if (!r.isNullAt(i)) Some(r.getInt(i)) else None

      Txt(
        getInt(r, 0),
        getStr(r, 1),
        getStr(r, 2),
        getStr(r, 3),
        getStr(r, 4),
        getStr(r, 5))

    }
    import sparkSession.sqlContext.implicits._

    val txt = appConf.getConfig(args(1)).getString("txt")
    val txtDF = sparkSession.sqlContext.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(txt)
      .rdd
      .map(rec => transformRow(rec))
      .toDF()

    //txtDF.show()

    //way2
    
    val txtRDD = sparkSession.sparkContext.textFile(txt, 2)
    val header = txtRDD.first()
    val data = txtRDD.filter(rec => rec != header)
    val dataDF = data.map(rec => {
      val t = rec.split("\t")
      Txt2(t(0).toInt,
          t(1).toString(),
          t(2).toString(),
          t(3).toString(),
          t(4).toString(),
          t(5).toString())
    }).toDF()
    
    dataDF.printSchema()
    dataDF.show()
    

    //reading parquat fileformat
    println("--------------------------PARQUET--------------------------")
    val parquet = appConf.getConfig(args(1)).getString("parquet")
    val parquetDF = sparkSession.sqlContext.read.parquet(parquet)
    parquetDF.printSchema()

    println("---Done---")

  }
}