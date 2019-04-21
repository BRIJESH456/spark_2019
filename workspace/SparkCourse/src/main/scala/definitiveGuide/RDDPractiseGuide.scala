package definitiveGuide

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

object RDDPractiseGuide {
  def main(args: Array[String]): Unit = {
    val appConf = ConfigFactory.load()

    val sparkSession = SparkSession.builder()
      .appName("RDDPractiseGuide")
      .master(appConf.getConfig(args(0)).getString("deploymentMaster"))
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "5")

    //creating RDD

    val rangeRDD = sparkSession.range(100).rdd
    //rangeRDD.foreach(println)
    //rangeRDD.take(5).foreach(println)

    sparkSession.range(10).rdd

    //from a local collection
    val seq = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
    var numSlices = 2
    val words = sparkSession.sparkContext.parallelize(seq, numSlices)

    words.setName("myWords")

    //sparkSession.sparkContext.

    //Transformation
    //--distinct
    words.distinct().count()

    //--startsWith
    var prefix = "S"
    def startsWithS(individual: String) = {
      individual.startsWith(prefix)
    }

    //--filter
    words.filter(word => startsWithS(word)).collect()

    //--map
    val words2 = words.map(word => (word, word(0), word.startsWith("S")))
    //words2.foreach(println)
    words2.filter(record => record._3)

    //--flatMap

    //--sort
    words.sortBy(word => (word, word.length() * -1)).take(2)

    //Action
    //--reduce
    val total = sparkSession.sparkContext.parallelize(1 to 20).reduce(_ + _)
    //println(total)

    def wordLengthReducer(leftWord: String, rightWord: String): String = {
      if (leftWord.length > rightWord.length)
        return leftWord
      else
        return rightWord
    }

    //words.reduce(wordLengthReducer).foreach(print)

    //--count

    words.count()

    //--countApprox
    val confidence = 0.95
    val timeoutMilliseconds = 400
    words.countApprox(timeoutMilliseconds, confidence)

    //--countApproxDistinct
    words.countApproxDistinct(0.05)

    //--countByValue
    words.countByValue()

    //--countByValueApprox
    words.countByValueApprox(1000, 0.95)

    //--first
    words.first()

    //--max and min
    sparkSession.sparkContext.parallelize(1 to 20).max()
    sparkSession.sparkContext.parallelize(1 to 20).min()

    //--take
    words.take(5)
    words.takeOrdered(5)
    words.top(5)
    val withReplacement = true
    val numberToTake = 6
    val randomSeed = 100L
    words.takeSample(withReplacement, numberToTake, randomSeed)

    //--saveAsTextFile

    //--SequenceFiles
    //words.saveAsObjectFile("/tmp/my/sequenceFilePath")

    //--Caching

    words.getStorageLevel
    //words.cache()
    words.getStorageLevel

    sparkSession.conf.set("org.apache.spark.storage.StorageLevel", "")

    //--Checkpointing
    //sparkSession.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
    //words.checkpoint()
    
    //--pipe
    words.pipe("wc -l").collect().foreach(print)
  }
}