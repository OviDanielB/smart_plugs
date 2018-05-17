import config.{Properties, SmartPlugConfig}
import model.PlugData
import org.scalatest.FlatSpec
import utils.{CSVParser, CalendarManager}

import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer

class QueryTwoTestSuite extends FlatSpec {

  val TEST_HOUSE_ID = 0
  val TEST_TIME_PERIOD = 0

  /*             (plugID, day, value)         */
  type TestData = (Int, Int, Float)

  "Query Two" should "return values " in {

    val calManager = new CalendarManager
    val dataList : ListBuffer[PlugData] = CSVParser.readDataFromLocalFile(SmartPlugConfig.get(Properties.Test.CSV_DATASET_URL))

    var valList : ListBuffer[TestData] = ListBuffer()
    var triple = (0,0,0f)
    var plugIDs : SortedSet[Int] = SortedSet()
    for(d <- dataList){
      if(d.isWorkMeasurement() && d.house_id == TEST_HOUSE_ID && calManager.getInterval(d.timestamp) == TEST_TIME_PERIOD){
        triple = (d.plug_id, calManager.getDayAndMonth(d.timestamp)(0), d.value)
        plugIDs += d.plug_id
        valList.append( triple )
      }
    }

    val numPlugs = plugIDs.size

    var work : Float = 0f
    for(day <- 1 to 30){
      for(pid <- 0 until numPlugs) {
        work = workConsumed(valList, pid, day)
      }
    }


  }


  def workConsumed(valList: ListBuffer[TestData], plugId : Int, day: Int): Float = {
    var min : Float = Float.MaxValue
    var max : Float = 0f
    for(v <- valList){
      if(v._1 == plugId && v._2 == day){
        println(v)
        if(v._3 < min) min = v._3
        if(v._3 > max) max = v._3
      }
    }
    max - min
  }

}
