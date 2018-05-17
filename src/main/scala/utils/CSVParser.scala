package utils

import config.{SmartPlugConfig, SmartPlugProperties}
import model.PlugData

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

object CSVParser {

  private val CSV_DELIMITER = ","
  private var count :Int = 0

  /**
    * returns all lines from csv dataset as PlugData class
    * @return mutable list of PlugData
    */
  def readDataFromFile(): ListBuffer[PlugData] = {

    val list : ListBuffer[PlugData] = ListBuffer()
    var data : PlugData = new PlugData()
    for(l <- Source.fromFile(SmartPlugConfig.get(SmartPlugProperties.CSV_DATASET_URL)).getLines()){
      breakable {
        val parsed = parse(l)
        if (parsed.isEmpty) break

        data = parsed.get
        list.append(data)
      }

    }
    list
  }

  /**
    * parses a csv line string into Plug data class
    * @param line
    * @return plug data
    */
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
