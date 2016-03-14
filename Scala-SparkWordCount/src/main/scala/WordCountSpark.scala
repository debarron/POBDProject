import org.apache.log4j.{FileAppender, Level, Logger, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Path

/**
  * Created by daniel on 2/7/16.
  */
object WordCountSpark {
  def main(args: Array[String]) {// Configure the LOG output
    val fileStrLog : String = "/Users/daniel/LogWordCount-1.txt"
    val fileStrDB : String = "src/main/resources/inputFile/twitterDB.json"
    val fileStrOutput = "src/main/resources/outputFiles"
    val fileStrSparkOutput = fileStrOutput + "/sampleWordCountOutPut.txt"

    val logger = Logger.getRootLogger
    val appender = new FileAppender(new SimpleLayout(), fileStrLog, false)
    logger.addAppender(appender)
    logger.setLevel(Level.ALL)


    // Initialize the Spark context
    val conf = new SparkConf()
      .setAppName("WordCountSpark")
      .setMaster("local[2]")
      .set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)


    println(">> Reading the DB file")
    val textFile = sc.textFile(fileStrDB)

    println(">> Mapping the DB")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x,y)=>x+y)

    println(">> Writing the output")
    val dir = Path(fileStrOutput)
    if (dir.exists) {
      dir.deleteRecursively()
    }
    counts.saveAsTextFile(fileStrSparkOutput)
    counts.foreach(i=>println(i))

    println(">> Computation complete")

    // Stopping spark
    sc.stop()
}


}
