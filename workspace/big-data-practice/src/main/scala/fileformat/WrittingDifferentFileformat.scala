package fileformat

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.IntegerType

case class Txt(
  id:         Option[Int],
  first_name: Option[String],
  last_name:  Option[String],
  email:      Option[String],
  gender:     Option[String],
  ip_address: Option[String])

object WrittingDifferentFileformat {
  def main(args: Array[String]): Unit = {
    //variables
    val name = "WrittingDifferentFileformat"

    //Configuration External parameter
    val appConf = ConfigFactory.load()

    //SparkSession creation using spark2.x
    val sparkSession = SparkSession.builder()
      .appName(name)
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    import sparkSession.sqlContext.implicits._

    val txt = appConf.getConfig(args(1)).getString("txt")

    /*   val textRDD = sparkSession.sparkContext.textFile(txt, 2)
    val df = textRDD.map(rec => {
      val t = rec.split("\t")

      Txt(t(0).toInt, t(1).toString(), t(2).toString(), t(3).toString(), t(4).toString(), t(5).toString())

    }).toDF()*/

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

    val txtDF = sparkSession.sqlContext.read
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(txt)
      .rdd
      .map(rec => transformRow(rec))
      .toDF()

    //txtDF.printSchema()
    // coverting dataframe into dataset

    val txtDS = txtDF.as[Txt]
    //txtDS.show()
    

    /*val txtDF2 = txtDF.withColumn("id", txtDF("id").cast(IntegerType))
    txtDF2.printSchema()*/

    /* val options = Map("inferSchema" -> "true", "header" -> "true")
      val path = "file:/E:/Hadoop/learning/hadoop/data-master/data/output/parquet"
      txtDF.write.options(options).parquet(path)*/

  }
}