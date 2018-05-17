import config.{SmartPlugConfig, SmartPlugProperties}
import model.PlugData
import org.scalatest.FlatSpec
import utils.CSVParser

import scala.collection.mutable._

import scala.collection.SortedSet

class QueryOneTestSuite extends FlatSpec {


  def computeExpectedHousesIDWithLoadGreaterThan(dataList: ListBuffer[PlugData], nHouses: Int, i: Int): SortedSet[Int] = {
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
    expectedHousesID
  }

  "The first query" should "return houses" in {

    val dataList : ListBuffer[PlugData] = CSVParser.readDataFromFile()
    val nHouses : Int = numberOfHouses(dataList)
    val expectedHousesID : SortedSet[Int] = computeExpectedHousesIDWithLoadGreaterThan(dataList,nHouses,350)
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


}
