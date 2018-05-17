import config.{SmartPlugConfig, SmartPlugProperties}
import model.PlugData
import org.scalatest.FlatSpec
import utils.CSVParser

import scala.collection.mutable._
import util.control.Breaks._
import scala.io.Source

import scala.collection.SortedSet

class QueryOneTestSuite extends FlatSpec {

  "The first query" should "return houses" in {

    val dataList : ListBuffer[PlugData] = readDataFromFile()
    val nHouses : Int = numberOfHouses(dataList)

    var hMap : Map[Int, ListBuffer[PlugData]] = Map()
    for(i <- 0 until nHouses){
      hMap = hMap.+((i, ListBuffer()))
    }

    for(d <- dataList) {
      hMap(d.house_id).append(d)
    }

    var expectedHousesID : SortedSet[Int] = SortedSet()
    for(h <- hMap){
      for(j <- h._2){
        if(j.value >= 350){
          expectedHousesID += j.house_id
        }
      }
    }

    val actualHousesId : Array[Int] = Query1.execute()

    for(actualHouse <- actualHousesId){
      assert(expectedHousesID.contains(actualHouse))
    }
    assert(actualHousesId.length == expectedHousesID.size)
  }


  def numberOfHouses(list: ListBuffer[PlugData]): Int = {
    var houseIdset : Set[Int] = Set()
    for(l <- list){
      houseIdset = houseIdset.+(l.house_id)
    }
    houseIdset.size
  }

  def readDataFromFile(): ListBuffer[PlugData] = {

    val list : ListBuffer[PlugData] = ListBuffer()
    var data : PlugData = new PlugData()
    for(l <- Source.fromFile(SmartPlugConfig.get(SmartPlugProperties.CSV_DATASET_URL)).getLines()){
      breakable {
        val parsed = CSVParser.parse(l)
        if (parsed.isEmpty) break

        data = parsed.get
        list.append(data)
      }

    }
    list
  }

}
