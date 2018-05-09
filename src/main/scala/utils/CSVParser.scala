package utils

import model.PlugData

object CSVParser {

  private val CSV_DELIMITER = ","

  def parse(line: String) : Option[PlugData] = {

    val cols = line.split(CSV_DELIMITER).map(_.trim)
    try {
      val propBool = parseBoolean(cols(3))
      Some(new PlugData(cols(0).toInt, cols(1).toInt, cols(2).toFloat, propBool, cols(4).toInt, cols(5).toInt, cols(6).toInt))
    }catch {
      case ex : NumberFormatException =>  ex.printStackTrace() ; None
    }
  }

  def parseBoolean(value : String): Boolean = {
    val propInt : Int = value.toInt

    propInt match {
      case 1 => true
      case 0 => false
      case _ => throw new Exception
    }

  }
}
