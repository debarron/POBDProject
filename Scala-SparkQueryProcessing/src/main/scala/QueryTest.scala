/**
  * Created by daniel on 3/31/16.
  */

import java.sql.JDBCType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import pobd.sqp.query.hashtags._
import org.apache.log4j.{FileAppender, Level, Logger, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._
import scala.reflect.io.Path

object QueryTest {

  def getDistributionFromLanguage(resultSet: DataFrame, total : Long): RDD[Any] ={
    resultSet.map(row => List(row(0), row(1),
      (java.lang.Float.parseFloat(row(1).toString) / (total * 1.0)) * 100)
    )
  }

  def getDifferentValues(data : DataFrame, col : String): DataFrame ={
    data.select(col).distinct
  }

  def getAggregateByColumn(data : DataFrame, col : String): DataFrame ={
    data.groupBy(col).agg(
      "id" -> "count"
    )
  }

  def getMostRelevantUsers(data : DataFrame, sqlContext: SQLContext): DataFrame ={
    val userInfo = data.select(
      "user.id",
      "user.name",
      "user.favouritesCount",
      "user.followersCount",
      "user.friendsCount"
    ).registerTempTable("users")

    val res = sqlContext.sql("SELECT name, id, followersCount FROM users " +
      "ORDER BY followersCount DESC")

    res
  }

  def getHashTagsAsKeyValue(data: DataFrame): RDD[(String, Int)] ={
    val res = data.map(row => row.toString().replace("List", ""))
      .flatMap(row => row.split(","))
      .filter(_.contains("]"))
      .map(row => (row.toUpperCase.replace(")", "").replace("]", ""), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2)

    res
  }

  def getDistributionFromHT(data : RDD[(String, Int)], total : Double): RDD[(String, Int, Double)] ={
    val HTDistribution = data.map[(String, Int, Double)] (k =>
      (k._1, k._2, (k._2.toDouble/total)*100)
    )
    HTDistribution
  }

  def main(args : Array[String]): Unit ={
    println(">> The value of the positive Hashtags")
    PositiveHashTags.foreach(println)
    println(" ")
    println(">> The value of the negative Hashtags")
    NegativeHashTags.foreach(println)
    println(" ")
    println(">> The value of the neutral Hashtags")
    NeutralHashTags.foreach(println)
    println(" ")


    val baseDir = "/Users/daniel/Documents/UMKC/Spring2016/PrinciplesOfBigData/TweetData"
    var db = "data-ClimateChange-04012016-1.json"
    db = "data-ClimateDenial-04012016-1.json"

    // Spark and logfiles
    val conf = new SparkConf()
      .setAppName("WordCountSpark")
      .setMaster("local[2]")
      .set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    val logger = Logger.getRootLogger
    val appender = new FileAppender(new SimpleLayout(), s"$baseDir/logSQ-1.txt", false)
    logger.addAppender(appender)
    logger.setLevel(Level.OFF)

    println(">> Reading in SQL Dataframe style")
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.read.json(s"$baseDir/$db").cache()

    // Different languages
    val differentLanguages = getDifferentValues(data, "lang")
    val aggregateLanguages = getAggregateByColumn(data, "lang")
    val relevantUsers = getMostRelevantUsers(data, sqlContext)
    val differentHT = getHashTagsAsKeyValue(data.select("hashtagEntities"))

    val total = differentHT.values.sum()
    val HTDistribution = getDistributionFromHT(differentHT, total)

    println(s"The total is $total")
    HTDistribution.foreach(println)

    val resultsQ1 = differentLanguages.count()
    val resultsQ2 = getDistributionFromLanguage(aggregateLanguages, data.count())
    val resultsQ3 = relevantUsers.take(100)
    val resultsQ4 = differentHT
    val resultsQ5 = getDistributionFromHT(differentHT, total)



//    differentHT.foreach(println)
//    data.toJSON.saveAsTextFile(s"$baseDir/Pooooooooooooja.json")
//    println(">> Some queries")
//    println(">> Q1. Different languages: " + resultsQ1)
//    println(">> Q2. Get distribution of languages: ")
//    resultsQ2.foreach(r => println("  ++ " + r))
//    println(">> Q3. Number of different hashtags: " + resultsQ3)
//    differentHT.foreach(println)
//    println(">> STUFF ")
//    println(s.count())
//    println(">> Dude")
//    val locationsA = data.select("user.location").
    //      rdd.
    //      filter { r =>
    //        val value = r(0).toString
    //
    //        ! value.isEmpty && value.length > 0
    //      }
    ////    val locations = locationsA.flatMap(city =>
    ////      city.toString()
    ////        .toUpperCase
    ////        .replace("[", "")
    ////        .replace("]","")
    ////        .replaceAll(" ", "#")
    ////        .replaceAll("-", "#")
    ////        .replaceAll(",", "#")
    ////        .split("#"))
    ////      .map(k => (k, 1))
    ////      .reduceByKey(_ + _)
    ////      .sortByKey()
    //
    //    val locations = data.select("user.timeZone").rdd
    //      .filter(!_.toString.isEmpty)
    //      .filter(!_.toString().contains("null"))
    ////      .filter(!_.toSTring.contains("Time"))
    //      .map(location => (location, 1))
    //      .reduceByKey(_ + _)
    //
    //
    //    println(locations.count())
    //    locations.foreach(println)

//
//    //    println(">> Reading the Tweets")
//    //    val dbTweets = sc.textFile(s"$baseDir/$db")
//    //    val counts = dbTweets.flatMap(line => line.split("\n"))
//    //      .map(word => (word, 1))
//    //      .reduceByKey((x,y)=>x+y)
//    //
//    //    println(">> Number of lines in the file: " + counts.count())
//
//
//
//    //    data.printSchema()
//    println(">> The count is: " + data.count())
//
//
//    val ids = data.select(data("lang"))
////    val ids = data.select(data("hashtagEntities"))
//
//    val total = ids.count
//
//    println("Number of distinct values: " + ids.distinct.count)
//    println("Number of values: " + total)
//
////    ids.foreach(println)
////    data.registerTempTable("temp")
////    val anotherPeopleRDD = sc.parallelize(
////      {"id","hashtagEntities"}""" :: Nil)
////    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)
////    anotherPeople.foreach(println)
////    {id, hashtagEntities}
//
//    // Lets group data
//
//
////      groups.map(row => row(0) + " " +
////          row(1) + " " +
////          (java.lang.Float.parseFloat(row(1).toString) / (total * 1.0)) * 100
////    )
//    println(">> Processing")
//    x.foreach(println)
//
//
//    // Filter those tweets that have positive hashtags
//    val ht = {
//      data.select(data("hashtagEntities"))
//    }
//
//
//    val v = sqlContext.sql("SELECT id, country from temp")
//    println(">> SQL Query")
//    v.foreach(println)


//    val fpos = data.map(row => row.get)


//      row(1) =>
//      row(0) + " " +
//      row(1) + " " +
//      (java.lang.Float.parseFloat(row(1).toString) / (total * 1.0)) * 100
//    )


//    groups.foreach(println)


//    groups.foreach(row =>
//      println(row(0) + " " +
//        row(1) + " " +
//        (java.lang.Float.parseFloat(row(1).toString) / (total * 1.0)) * 100
//      )
//    )
//    groups.foreach(println)

  }


}

/*
* Analytical queries
The queries that our system will display are related to Climate change. Based our search on this topic,
we can use hashtags like #GlobalWarming, #ClimateChange, #ActionOnChange, #actonclimate,
#environment, #ClimateSkeptic, #ClimateDenial.
Once we have enough information we plan to answer the next list of queries:


What is the distribution
among the tweets who claim
to support and denied climate
change?

Distribution among used language
and Relevance of the tweets related
to the hashtags?

How many are the deniers of climate change and who are the main public figures that people follows?

Countries which are more aware of global warming,
and Distribution in the USA?

Factors and effects (according to people's opinion) that are related to global warming?


* */