import config.{Properties, SmartPlugConfig}
import model.PlugData
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import utils.CSVParser

import scala.collection.mutable.{ListBuffer, Map}
import scala.collection.SortedSet
import scala.util.control.Breaks._

class QueryOneTestSuite extends FlatSpec {

  private[this] val sparkContext : SparkContext = SparkController.localTestSparkSession()

  private[this] val LOAD_THRESHOLD = 350

  private[this] val expectedHousesID : SortedSet[Int] = {
    val dataList : ListBuffer[PlugData] = CSVParser.readDataFromLocalFile(SmartPlugConfig.get(Properties.Test.CSV_DATASET_URL))
    val nHouses : Int = numberOfHouses(dataList)
    computeExpectedHousesIDWithLoadGreaterThan(dataList,nHouses,LOAD_THRESHOLD)
  }

  "The first query with CSV dataset" should "return houses" in {
    val actualHousesId : Array[Int] = Query1.executeCSV(sparkContext, SmartPlugConfig.get(Properties.Test.CSV_DATASET_URL))
    for (actualHouse <- actualHousesId) {
      assert(expectedHousesID.contains(actualHouse))
    }
    assert(actualHousesId.length == expectedHousesID.size)
  }

  "The first query with CSV dataset (faster version)" should "return houses" in {
    val actualHousesIdFaster : Array[Int] = Query1.executeCSV(sparkContext, SmartPlugConfig.get(Properties.Test.CSV_DATASET_URL))
    for (actualHouse <- actualHousesIdFaster) {
      assert(expectedHousesID.contains(actualHouse))
    }
    assert(actualHousesIdFaster.length == expectedHousesID.size)
  }


  "The first query with Parquet dataset" should "return houses" in {
    val actualHousesIdParquet : Array[Int] = Query1.executeParquet(sparkContext, SmartPlugConfig.get(Properties.Test.PARQUET_DATASET_URL))
    for (actualHouse <- actualHousesIdParquet) {
      assert(expectedHousesID.contains(actualHouse))
    }
    assert(actualHousesIdParquet.length == expectedHousesID.size)
  }


  def computeExpectedHousesIDWithLoadGreaterThan(dataList: ListBuffer[PlugData], nHouses: Int, i: Int): SortedSet[Int] = {
    var hMap : Map[Int, ListBuffer[PlugData]] = Map()
    for(i <- 0 until nHouses){
      hMap = hMap.+((i, ListBuffer()))
    }

    for(d <- dataList) {
      if(d.isLoadMeasurement()) {
        hMap(d.house_id).append(d)
      }
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
              expectedHousesID += h._1
              break
            }
            tsWorkSum += (j.timestamp -> newValue)
          } else {
            tsWorkSum += (j.timestamp -> j.value)
          }
        }
      }

    }
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
