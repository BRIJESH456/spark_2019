package dataframe

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object CSVDatasetWithoutHeader {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CSVDatasetWithoutHeader")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    val sqlContext = sparkSession.sqlContext


    val path = "E:\\Hadoop\\learning\\hadoop\\data-master\\data\\retail_db\\orders\\orders.txt"

    val schema = StructType(
      StructField("order_id", IntegerType, false) ::
        StructField("order_date", DateType, false) ::
        StructField("order_customer_id", IntegerType, false) ::
        StructField("order_status", StringType, false) :: Nil)

    val orderesDF = sqlContext.read.schema(schema).csv(path)

    orderesDF.createOrReplaceTempView("order")

    val query = "select *  from order where order_status = 'COMPLETE'"
    val orderSQL = sqlContext.sql(query)

    orderSQL.show(10)
    orderSQL.printSchema()

  }
}