package pobd.sqp.query.states

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by daniel on 4/10/16.
  */
object USStates {
  def getData(sc : SparkContext, fileDir : String): RDD[(String, (String))] ={
    sc.textFile(fileDir)
      .map{line =>
        val m = line.split(",")
        (m(1), (m(0)))
      }
  }

  def getUSStatesData(sc : SparkContext, states : String, cities : String): RDD[String] ={
    val dStates = getData(sc, states)
    val dCities = getData(sc, cities)

    dCities.join(dStates).map(_.toString().replace("(", "").replace(")", ""))
  }
}
