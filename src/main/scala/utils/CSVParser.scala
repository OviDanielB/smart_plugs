package utils

import model.PlugData

object CSVParser {

  private val CSV_DELIMITER = ","
  private var count :Int = 0

  def parse(line: String) : Option[PlugData] = {

    val cols = line.split(CSV_DELIMITER).map(_.trim)
    try {
      val propBool = parseBoolean(cols(3))
      Some(new PlugData(cols(0).toLong, cols(1).toLong, cols(2).toFloat, propBool, cols(4).toInt, cols(5).toInt, cols(6).toInt))
    }catch {
      case ex : Exception =>  ex.printStackTrace() ; count = count + 1; printf("Wrong value count %d\n", count); None
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
