import model.PlugData
import org.scalatest.FlatSpec
import utils.{CSVParser, CalendarManager}

import scala.collection.mutable.ListBuffer

class QueryTwoTestSuite extends FlatSpec {

  val TEST_HOUSE_ID = 0
  val TEST_TIME_PERIOD = 0

  "Query Two" should "return values " in {

    val calManager = new CalendarManager
    val dataList : ListBuffer[PlugData] = CSVParser.readDataFromFile()

    for(d <- dataList){
      if(d.house_id == TEST_HOUSE_ID && calManager.getInterval(d.timestamp) == TEST_TIME_PERIOD){

      }
    }
  }

}
