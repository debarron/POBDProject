package pobd.sqp

import org.apache.log4j.{Level, SimpleLayout, FileAppender, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import pobd.sqp.query.states.USStates

/**
  * Created by daniel on 4/9/16.
  */
object StatesTest {

  def main(args : Array[String]): Unit ={
    val baseDir = "/Users/daniel/Documents/" +
      "Dev/GitHubRepos/POBDProject/CSV-ExternalData/"

    val states = s"$baseDir/StateAb.csv"
    val cities = s"$baseDir/CitiesStateAb.csv"

    println(">> Start")
    val conf = new SparkConf()
      .setAppName("WordCountSpark")
      .setMaster("local[2]")
      .set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    val logger = Logger.getRootLogger
    val appender = new FileAppender(new SimpleLayout(), s"$baseDir/logSQ-1.txt", false)
    logger.addAppender(appender)
    logger.setLevel(Level.OFF)

//    val dataStates = sc.textFile(states)
//      .map{line =>
//        val m = line.split(",")
//        (m(1), (m(0)))
//      }
//
//
//    val dataCities = sc.textFile(cities)
//      .map{line =>
//        val m = line.split(",")
//        (m(1), (m(0)))
//      }

    println(">> Start printing cities")
//    dataCities.foreach(println)
    println(">> Print States")
////    dataStates.foreach(println)
//    val hData = dataCities.join(dataStates)
//    val returnList = hData.map(item => item.toString())
//    returnList.foreach(println)

    val d = USStates.getUSStatesData(sc, states, cities)
    d.foreach(println)

    println(">> End")

  }
}
