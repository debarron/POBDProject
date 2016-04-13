/**
  * Created by daniel on 3/31/16.
  */

import java.sql.JDBCType
import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext, Column}
import pobd.sqp.mongodb.MongoInput
import pobd.sqp.query.DateTimeParser
import pobd.sqp.query.hashtags._
import org.apache.log4j.{FileAppender, Level, Logger, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import pobd.sqp.query.states.USStates
import pobd.sqp.ui.ColorGen
import scala.collection.JavaConverters._

//import scala.reflect.internal.util.TableDef.Column
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

  // Query #1 and #2
  def querySuportAndDeny(data : DataFrame): Seq[String] ={
    val tweetText = data.select("text")
      .map(it => it.toString().replace("#","").split(" "))

    val tp = tweetText.filter(tweet => PositiveHashTags.exists( ht => tweet.contains(ht)))
    val tn = tweetText.filter(tweet => NegativeHashTags.exists( ht => tweet.contains(ht)))
    val tneu = tweetText.filter(tweet => NeutralHashTags.exists( ht => tweet.contains(ht)))

    Seq(
      ("Support", tp.count()),
      ("Deny", tn.count()),
      ("Neutral", tneu.count())
    ).map(tuple =>
      "{\"label\":\"%s\",\"value\":%d,\"color\":\"%s\"}".format(
        tuple._1, tuple._2 , ColorGen.getRandomRGBColor
      )
    )
  }

  def timeSeriesData(data : DataFrame): Seq[String] ={
    val tweetText = data.select("text")
      .map(it => it.toString().replace("#","").split(" "))

    val tp = tweetText.filter(tweet => PositiveHashTags.exists( ht => tweet.contains(ht)))
    val tn = tweetText.filter(tweet => NegativeHashTags.exists( ht => tweet.contains(ht)))
    val tneu = tweetText.filter(tweet => NeutralHashTags.exists( ht => tweet.contains(ht)))

    Seq(
      ("Support", tp.count()),
      ("Deny", tn.count()),
      ("Neutral", tneu.count())
    ).map(tuple =>
      "{\"label\":\"%s\",\"value\":%d,\"color\":\"%s\"}".format(
        tuple._1, tuple._2 , ColorGen.getRandomRGBColor
      )
    )
  }





  // Query #6
  def queryHowManyLanguages(data : DataFrame, column : String, total : Long, sql : SQLContext): RDD[String] ={
    val tempData = data.groupBy(column)
      .agg("id" -> "count")
      .map(row =>
        Row(
            row.get(0).toString,
            row.getLong(1),
            (row.getLong(1) * 1.0) / (total * 1.0) * 100.0,
            ((row.getLong(1) * 1.0) / (total * 1.0) * 100.0).toString,
            row.get(0).toString,
            ColorGen.getRandomRGBColor
        )
      )

    val schemaQ1 = StructType(List(
        StructField("lang", StringType, true),
        StructField("count", LongType, true),
        StructField("percentage", DoubleType, true),
        StructField("value", StringType, true),
        StructField("label", StringType, true),
        StructField("color", StringType, true)

      )
    )

    // TODO Add the JSON Array Format
    sql.createDataFrame(tempData, schemaQ1).toJSON
  }

  /*
  Get the hashtags
  Count them by group them
  Assign a color
  Save them as a JSON Array in q1 in the form
  description: Description of the query, values:[]
  */
  // Query #1 and #2

  def insertQ12(): Unit ={
//    [
//    {"label":"#GlobalWarming","value":"10","color":"#76F397"},
//    {"label":"#ClimateChange","value":"7","color":"#38CAE4"},
//    {"label":"#ActionOnChange","value":"13","color":"#FFA37E"},
//    {"label":"#ClimateSkeptic","value":"4","color":"#2405CE"},
//    {"label":"#ClimateDenial","value":"16","color":"#A255CE"},
//    {"label":"#Environment","value":"20","color":"#A2A26E"},
//    {"label":"#ActonClimate","value":"10","color":"#EEA565"}
//    ]
  }

  def query12(data : DataFrame): Seq[String] ={
    val tweetText = data.select("text")
      .flatMap(it => it.toString().toLowerCase.split(" "))
//      .filter(_.contains("#"))


    val tp = tweetText.filter(tweet => PositiveHashTagsC.exists( ht => tweet.contains(ht)))
    val tn = tweetText.filter(tweet => NegativeHashTagsC.exists( ht => tweet.contains(ht)))
    val tneu = tweetText.filter(tweet => NeutralHashTagsC.exists( ht => tweet.contains(ht)))


    val ach = tweetText.filter(_.contains("#actiononchange"))
    val ac = tweetText.filter(_.contains("#actonclimate"))
    val cs = tweetText.filter(_.contains("#climateskeptic"))
    val cd = tweetText.filter(_.contains("#climatedenial"))
    val gw = tweetText.filter(_.contains("#globalwarming"))
    val cc = tweetText.filter(_.contains("#climatechange"))
    val e = tweetText.filter(_.contains("#environment"))


    println("%s %d\n%s %d\n%s %d\n%s %d\n%s %d\n%s %d\n%s %d\n\n>>SEP".format(
    "#actiononchange",ach.count(),
    "#actonclimate",ac.count(),
    "#climateskeptic",cs.count(),
    "#climatedenial",cd.count(),
    "#globalwarming",gw.count(),
    "#climatechange",cc.count(),
    "#environment",e.count()))


    val mongoValues = Array(
      Array("#actiononchange",ach.count().toString, "#76F397"),
      Array("#actonclimate", ac.count().toString, "#38CAE4"),
      Array("#climateskeptic",cs.count().toString, "#FFA37E"),
      Array("#climatedenial",cd.count().toString, "#2405CE"),
      Array("#globalwarming",gw.count().toString, "#A255CE"),
      Array("#climatechange",cc.count().toString, "#A2A26E"),
      Array("#environment",  e.count().toString, "#EEA565")
    )

    println(">> Preparing the data")
    val mon = new MongoInput()
    mon.insertQ1(mongoValues)

    println(">> data was inserted")

    val p = Seq(
      ("Support", tp.count()),
      ("Deny", tn.count()),
      ("Neutral", tneu.count())
    ).map(tuple =>
      "{\"label\":\"%s\",\"value\":%d,\"color\":\"%s\"}".format(
        tuple._1, tuple._2 , ColorGen.getRandomRGBColor
      )
    )
//
//    tp.foreach(item => println(item.foreach(i=>print(i + ""))))
//    println(">> SEP")
//    tn.foreach(item => println(item.foreach(i=>print(i + ""))))
//    println(">> SEP")
//    tneu.foreach(item => println(item.foreach(i=>print(i + ""))))
//    println(">> SEP")

    p
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
//    db = "data-Positive-1-1.json"



    val baseDirStates = "/Users/daniel/Documents/" +
      "Dev/GitHubRepos/POBDProject/CSV-ExternalData/"

    val states = s"$baseDirStates/StateAb.csv"
    val cities = s"$baseDirStates/CitiesStateAb.csv"


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


    val x = data.select(
      data("id"),
      data("createdAt"),
      data("text"),
      data("user.followersCount"),
      data("user.id").as("userid"),
      data("favoriteCount"),
      data("lang"),
      data("user.location"),
      data("hashtagEntities"),
      data("user.location"),
      data("quotedStatus.retweetCount"),
      data("isRetweeted").as("rt")
    )

    x.registerTempTable("tdata")
    x.printSchema()

    val qw = querySuportAndDeny(x)

//    x.select("createdAt").foreach(println)


    val test = x.select("createdAt")
        .filter(x("createdAt").isNotNull)
        .map(el => el.toString().replace("[","").replace("]",""))
        .filter(it => !it.isEmpty && it.length > 1)
        .map(elm => new DateTimeParser().parseString(elm))
        .map(elm => (elm , 1))
        .reduceByKey(_+_)
        .sortByKey(false, 3)

    test.foreach(println)

    val q1_2 = query12(x)
    val q6 = queryHowManyLanguages(x, "lang", x.count(), sqlContext)

    //    q1_2.foreach(println)

//    q1_2.foreach(println)

    println(">>> End")


//
//    val y = sqlContext.sql("SELECT * FROM tdata")
//    val tx = sqlContext.sql("SELECT id, text, rt from tdata WHERE text not like '%RT%' ").map(row =>
//      (row.get(0).toString, row.get(1).toString().replace("#", "").split(" "))
//    )


//    val tx2 = x.filter(it => it._2.exists(el => el.contains("climatedenial")))

//    val tx1 = tx.filter(it => it._2.exists(el => el.contains("climateskeptic")))
//    val tx2 = tx.filter(it => it._2.exists(el => el.contains("climatedenial")))
//val tx3 = tx1.union(tx2).distinct()
//    println(tx3.count())
//    tx3.foreach(it => println(it._1 + it._2.foreach(it2 => print(it2 + " "))))

//    val mData = x.map(row => row.toString)
//    mData.foreach( item => println(item.toString + "$$ \n"))








//    // Different languages
//    val differentLanguages = getDifferentValues(data, "lang")
//    val aggregateLanguages = getAggregateByColumn(data, "lang")
//    val relevantUsers = getMostRelevantUsers(data, sqlContext)
//    val differentHT = getHashTagsAsKeyValue(data.select("hashtagEntities"))
//
//    val total = differentHT.values.sum()
//    val HTDistribution = getDistributionFromHT(differentHT, total)
//
//
//    val locationsA = data.select("user.location")
////    val locationsA = data.select("retweetedStatus.place")
//
//    println(s"The total is $total")
////    HTDistribution.foreach(println)
//
//    val resultsQ1 = differentLanguages.count()
//    val resultsQ2 = getDistributionFromLanguage(aggregateLanguages, data.count())
//    val resultsQ3 = relevantUsers.take(100)
//    val resultsQ4 = differentHT
//    val resultsQ5 = getDistributionFromHT(differentHT, total)
//
//
//    val usStates = USStates.getUSStatesData(sc, states, cities)
//      .flatMap(line => line.split(","))
//      .distinct().cache()
//
//
//    println(">>> Start")
//    usStates.foreach(println)
//
//
//    val locFilter = locationsA.rdd
//      .filter(item => item.toString.length > 3)
//      .filter(item => !item.toString().contains("#"))
//      .map(item => item.toString().replace("[","").replace("]","").split(" "))
//


//
//    locFilter.foreach{item =>
//      item.foreach{ city =>
//        usStates.foreach{ element =>
//          if(element.contains(city))
//            println(s"$city found in $element")
//        }
//      }
//    }
//    locationsA.foreach{item =>
//      val value = item.toString().length;
//      if(value > 3)
//        println(item)
//    }








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