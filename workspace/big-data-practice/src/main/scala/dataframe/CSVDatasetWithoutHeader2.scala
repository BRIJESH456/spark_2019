package dataframe

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object CSVDatasetWithoutHeader2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CSVDatasetWithoutHeader2")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")

    val sqlContext = sparkSession.sqlContext

    val path = "E:\\Hadoop\\learning\\hadoop\\data-master\\data\\retail_db\\orders\\orders.txt"
    
    val ordersRDD = sparkSession.sparkContext.textFile(path)
    //sparkSession.createDataFrame(rowRDD, schema)
    val rowRDD = ordersRDD.map(rec => Row(ordersRDD))
    /*val schema = StructType(
      StructField("order_id", IntegerType, false) ::
        StructField("order_date", StringType, false) ::
        StructField("order_customer_id", IntegerType, false) ::
        StructField("order_status", StringType, false) :: Nil)*/
    
    val schema = StructType(List(
        StructField("order_id", IntegerType, false) ,
        StructField("order_date", StringType, false) ,
        StructField("order_customer_id", IntegerType, false) ,
        StructField("order_status", StringType, false)
    ))
        
    val ordersDF = sqlContext.read.schema(schema).option("delimiter",",").csv(path)
    
    ordersDF.createOrReplaceTempView("order")

    val query = "select *  from order where order_status = 'COMPLETE'"
    val orderSQL = sqlContext.sql(query)

    orderSQL.show(10)
    orderSQL.printSchema()
        
  }
}