package pobd.sqp.query

import java.text.SimpleDateFormat
import java.util.Formatter.DateTime
import java.util.{Calendar, Locale, Date}

/**
  * Created by daniel on 4/10/16.
  */
class DateTimeParser {
  val twitterFormat = "MMM dd yyyy hh:mm:ss a"
  var parser : SimpleDateFormat = null
  var cal : Calendar = null

  def getParser: SimpleDateFormat = {
    if(parser == null){
      parser = new SimpleDateFormat(this.twitterFormat, Locale.US)
      parser.setLenient(true)
    }

    parser
  }

  def getCalendar : Calendar={
    if(cal == null) cal = Calendar.getInstance()

    cal
  }

  def parseString(dateStr : String): String  ={
    var res : String = null
    try {
      cal = getCalendar
      parser = getParser

      val date = parser.parse(dateStr.replace(",", ""))
      cal.setTime(date)

      res = "%04d-%02d-%02d-%02d".format(
        cal.get(Calendar.YEAR),
        cal.get(Calendar.MONTH),
        cal.get(Calendar.DAY_OF_MONTH),
        cal.get(Calendar.HOUR_OF_DAY)
      )
    }
    catch {
      case p : Exception => p.printStackTrace()
    }

    res
  }

}
