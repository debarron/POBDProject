package pobd.sqp.mongodb

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._

/**
  * Created by daniel on 4/13/16.
  */
class MongoInput {
  var mongoClient : MongoClient = null
  var tableName : String = null

  def getMongoDB: MongoClient ={
      MongoClient(DBConfig.host, DBConfig.port.toInt)
  }


  //    [
  //    {"label":"#GlobalWarming","value":"10","color":"#76F397"},
  //    {"label":"#ClimateChange","value":"7","color":"#38CAE4"},
  //    {"label":"#ActionOnChange","value":"13","color":"#FFA37E"},
  //    {"label":"#ClimateSkeptic","value":"4","color":"#2405CE"},
  //    {"label":"#ClimateDenial","value":"16","color":"#A255CE"},
  //    {"label":"#Environment","value":"20","color":"#A2A26E"},
  //    {"label":"#ActonClimate","value":"10","color":"#EEA565"}
  //    ]
//  col += MongoDBObject("custid" -> "1001") ++
  //        ("billingAddress" -> (MongoDBObject("state" -> "NV") ++ ("zip" -> "89150"))) ++
  //        ("orders" -> Seq(
  //          MongoDBObject("orderid" -> "1000001") ++ ("itemid" -> "A001") ++ ("quantity" -> 175) ++ ("returned" -> true),
  //          MongoDBObject("orderid" -> "1000002") ++ ("itemid" -> "A002") ++ ("quantity" -> 20)
  //        ))
  //
  def insertQ1(values : Array[Array[String]])={
    val client = getMongoDB
    val db = client.getDB(DBConfig.testDatabase)
    val collection = db("qTest")

    values.foreach { value =>
      collection += MongoDBObject("label" -> value(0),
        "value" -> value(1),
        "color" -> value(2))
    }

    client.close()
  }

}


//  val mongoClient = (DBConfig.userName, DBConfig.password) match {
//      case (Some(u), Some(pw)) => {
//        val server = new ServerAddress(DBConfig.host, DBConfig.port.toInt)
//        val credentials =
//          MongoCredential.createMongoCRCredential(u, DBConfig.testDatabase, pw.toCharArray)
//        MongoClient(server, List(credentials))
//      }
//      case _ => MongoClient(DBConfig.host, DBConfig.port.toInt)
//    }
//
//  val db = mongoClient.getDB(DBConfig.testDatabase)
//
//
//  try {
//  DBConfig.printConfig ()
//  val col = db (DBConfig.testCollection)
//
//  val it = col.iterator
//  it.foreach (println)
//}


