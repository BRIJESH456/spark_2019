package dataset

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

case class Ratings(
  userID:    Integer,
  movieID:   Integer,
  rating:    Double,
  timestamp: Integer)

object DatasetBasic extends App {

  val appConf = ConfigFactory.load()

  val sparkSession = SparkSession.builder()
    .appName("DatasetBasic")
    .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  import sparkSession.implicits._

  val ratings_path = appConf.getConfig(args(1)).getString("ratings")

  val ratingDS = sparkSession.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ratings_path)
    .as[Ratings]

  val ratingFilter = ratingDS.filter(rec => rec.rating >= 4.0)
  //ratingFilter.show()

  //ratingDS.printSchema()
  val ratingWhere = ratingDS.where(ratingDS("rating") === 4.0)
  //ratingWhere.show()

  case class MovieRating(
    movieID: Integer,
    rating:  Double)
  val ratingSeletedColumns = ratingDS.select("movieId", "rating")
  ratingSeletedColumns.show()
  

}