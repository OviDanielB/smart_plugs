import model.PlugData
import org.scalatest.FlatSpec
import utils.CSVParser

import scala.collection.mutable._

import scala.collection.SortedSet

import scala.util.control.Breaks._

class QueryOneTestSuite extends FlatSpec {

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


  def computeExpectedHousesIDWithLoadGreaterThan(dataList: ListBuffer[PlugData], nHouses: Int, i: Int): SortedSet[Int] = {
    var hMap : Map[Int, ListBuffer[PlugData]] = Map()
    for(i <- 0 until nHouses){
      hMap = hMap.+((i, ListBuffer()))
    }

    for(d <- dataList) {
      hMap(d.house_id).append(d)
    }

    var expectedHousesID : SortedSet[Int] = SortedSet()

    var tsWorkSum : Map[Long, Float] = Map()
    for( h <-hMap) {
      tsWorkSum = Map()
      breakable {
        for (j <- h._2) {
          if (tsWorkSum.contains(j.timestamp)) {
            val prevValue = tsWorkSum(j.timestamp)
            val newValue = prevValue + j.value
            if (newValue >= i) {
              expectedHousesID += h._1;
              break()
            }
            tsWorkSum += (j.timestamp -> newValue)
          } else {
            tsWorkSum += (j.timestamp -> j.value)
          }
        }
      }

    }
      /*breakable {
        for (t <- tsWorkSum) {
          if (t._2 >= i) {
            expectedHousesID += h._1
            break()
          }
        }
      }
    } */

    /*for(h <- hMap){
      for(j <- h._2){
        if(j.value >= 350){
          expectedHousesID += j.house_id
        }
      }
    } */
    expectedHousesID
  }


  def numberOfHouses(list: ListBuffer[PlugData]): Int = {
    var houseIdset : Set[Int] = Set()
    for(l <- list){
      houseIdset = houseIdset.+(l.house_id)
    }
    houseIdset.size
  }


}
