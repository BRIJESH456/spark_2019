package oracle

import org.apache.spark.sql.SparkSession

object ReportGeneration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConnectingOracleDatabase")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    import spark.implicits._
    
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:oracle:thin:@localhost:1521:xe")
      .option("dbtable", "input_config")
      .option("user", "system")
      .option("password", "oracle")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load()

    jdbcDF.show()
  }
}