package utils

import java.io.{File, PrintWriter}

import model.PlugData

import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}

/**
  * This object implements functions to read data from CSV files
  * as list of Plug Data object or write CSV file as a list of
  * execution times and the related name of query executed.
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object CSVParser {

  private val CSV_DELIMITER = ","
  private var count :Int = 0

  /**
    * Returns all lines from csv dataset as PlugData class.
    *
    * @return mutable list of PlugData
    */
  def readDataFromLocalFile(path: String): ListBuffer[PlugData] = {

    val list : ListBuffer[PlugData] = ListBuffer()
    var data : PlugData = new PlugData()
    for(l <- Source.fromFile(path).getLines()){
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
    * Parses a csv line string into Plug data class.
    *
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

  /**
    * Write a list of couples (name-of-query, execution-time) into a
    * CSV file.
    *
    * @param res couples query/time
    * @param filename file to write
    */
  def writeTimesToCSV(res: Map[String,Double], filename: String): Unit = {

    val file = new PrintWriter(new File(filename))

    var keyList = ListMap(res.toSeq.sortBy(_._1):_*)

    // header
    for (k <- keyList) {
      file.write(k._1)
      file.write(",")
    }
    file.write("\n")

    // results
    for (k <- keyList) {
      file.write(k._2.toString)
      file.write(",")
    }

    file.close()
  }
}
