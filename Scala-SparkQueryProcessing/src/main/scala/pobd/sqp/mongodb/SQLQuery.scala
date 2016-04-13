package pobd.sqp.mongodb


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLQuery {
  def main (args: Array[String]) {
    DBConfig.printConfig()
    val coreConf =
      new SparkConf()
        .setAppName("SQLQuery")
        .set("spark.nsmc.connection.host", DBConfig.host)
        .set("spark.nsmc.connection.port", DBConfig.port)
    // if a username AND password are defined, use them
    val conf = (DBConfig.userName, DBConfig.password) match {
      case (Some(u), Some(pw)) => coreConf.set("spark.nsmc.user", u).set("spark.nsmc.password", pw)
      case _ => coreConf
    }
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    try {
      sqlContext.sql(
        s"""
        |CREATE TEMPORARY TABLE dataTable
        |USING nsmc.sql.MongoRelationProvider
        |OPTIONS (db '${DBConfig.testDatabase}', collection '${DBConfig.testCollection}')
      """.stripMargin)

      println("*** Query 1: Everything")
      val data1 =
        sqlContext.sql("SELECT * FROM dataTable")
      data1.schema.printTreeString()
      data1.toDF().show()

      println("*** Query 2: Test IS NOT NULL on a field")
      val data2 =
        sqlContext.sql("SELECT custid FROM dataTable WHERE discountCode IS NOT NULL")
      data2.schema.printTreeString()
      data2.toDF().show()

      println("*** Query 3: Field of a structure that's not always present")
      val data3 =
        sqlContext.sql("SELECT custid, shippingAddress.zip FROM dataTable")
      data3.schema.printTreeString()
      data3.toDF().show()

      println("*** Query 4: Group by field of a structure that's not always present")
      val data4 =
        sqlContext.sql("SELECT COUNT(custid), shippingAddress.zip FROM dataTable GROUP BY shippingAddress.zip")
      data4.schema.printTreeString()
      data4.toDF().show()

    } finally {
      sc.stop()
    }
  }
}