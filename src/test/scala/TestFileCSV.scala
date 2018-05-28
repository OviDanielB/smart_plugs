import java.io.{File, PrintWriter}

import config.{Properties, SmartPlugConfig}
import controller.SparkController
import model.PlugData
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.FlatSpec
import utils.{CSVParser, CalendarManager}

import util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.util.Random

class TestFileCSV extends FlatSpec {

  val TEST_HOUSEs = Array(0,1)
  val TEST_HOUSEHOLD_IDs = Array(0)
  val TEST_PLUG_IDs = Array(0,1)

  val TEST_TIMESTAMP_BEGIN: Long =
    new DateTime(2013,9,1,17,59,59,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
  val TEST_TIMESTAMP_END: Long =
    new DateTime(2013,9,3,17,59,59,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000

  val TEST_PROPERTIES = Array(0,1)

  val TEST_RESET_PROBABILITY = 0.1
  val TEST_INCREMENT_PROBABILITY = 0.8

  val TEST_FILENAME = "dataset/testFile.csv"

  val MONTH = Range(1,31)


//  createTestFile()


  private[this] val sparkContext = SparkController.localTestSparkSession()

  private[this] val calendarManager : CalendarManager = new CalendarManager

  private[this] val LOAD_THRESHOLD = 350

  private[this] val dataList : ListBuffer[PlugData] = CSVParser.readDataFromLocalFile(TEST_FILENAME)

// OK
//  "The first query with CSV test dataset" should "return houses" in {
//    val actualHousesId : Array[Int] = Query1.executeCSV(sparkContext, TEST_FILENAME)
//    val expectedHousesID : Array[Int] = query1Results(dataList, TEST_HOUSEs.length)
//    println("Computed house ids:")
//    for (actualHouse <- actualHousesId)
//      println(actualHouse)
//    println("Expected house ids:")
//    for (expectedHouse <- expectedHousesID)
//      println(expectedHouse)
//    for (actualHouse <- actualHousesId) {
//      assert(expectedHousesID.contains(actualHouse))
//    }
//    assert(actualHousesId.length == expectedHousesID.length)
//  }

  // TODO
//  "The second query with CSV test dataset" should "return statistics" in {
//    val actualStats : Array[((Int,Int),Double,Double)] =
//      Query2.executeCSV(sparkContext, calendarManager, TEST_FILENAME)
//    val expectedStats : Array[((Int,Int),Double,Double)] = query2Results(dataList, TEST_HOUSEs.length)
//    println("Computed houses statistics:")
//    for (actualStat <- actualStats)
//      println(actualStat)
//    println("Expected houses statistics:")
//    for (expectedStat <- expectedStats)
//      println(expectedStat)
//    for (actualStat <- actualStats) {
//      assert(expectedStats.contains(actualStat))
//    }
//    assert(actualStats.length == TEST_HOUSEs.length)
//  }

  "The third query with CSV test dataset" should "return ranking" in {
    val data = sparkContext.textFile("dataset/testFile.csv")
    val actualRanking : Array[((Int,Int,Int,Int),Double)] =
      Query3.executeCSV(sparkContext, data, calendarManager)
    val expectedRanking : Array[((Int,Int,Int,Int),Double)] = query3ResultsMaxMin(dataList)
    println("Computed plugs ranking:")
    for (actualRank <- actualRanking)
      println(actualRank)
    println("Expected plugs ranking:")
    for (expectedRank <- expectedRanking)
      println(expectedRank)
    assert(actualRanking.length == expectedRanking.length)
    for (actualRank <- actualRanking.indices) {
      assert(expectedRanking(actualRank)._1.equals(actualRanking(actualRank)._1))
    }
  }

  def query1Results(values: ListBuffer[PlugData], nHouses: Int): Array[Int] = {
    val q = new QueryOneTestSuite
    q.computeExpectedHousesIDWithLoadGreaterThan(values, nHouses, 350).toArray
  }

  def query2Results(values: ListBuffer[PlugData], nHouses: Int): Array[((Int,Int),Double,Double)] = {
    new Array[((Int,Int),Double,Double)](0)
  }


  def query3ResultsMaxMin(values: ListBuffer[PlugData]): Array[((Int,Int,Int,Int),Double)] = {
    // plugid, houshold, house, day,month - min, max
    var pMap_high_day: Map[(Int,Int,Int,Int,Int),(Float,Float)] = Map()
    var pMap_low_day: Map[(Int,Int,Int,Int,Int),(Float,Float)] = Map()
    for (r <- values) {
      val rate = calendarManager.getPeriodRate(r.timestamp)
      val day = calendarManager.getDayAndMonth(r.timestamp)(0)
      val month = calendarManager.getDayAndMonth(r.timestamp)(1)
      if (r.isWorkMeasurement() && rate > 0) { // high
        if (!pMap_high_day.contains((r.plug_id, r.household_id, r.house_id, day, month)))
          pMap_high_day += ((r.plug_id, r.household_id, r.house_id, day, month) -> (r.value,r.value))
        else {
          pMap_high_day.-((r.plug_id, r.household_id,r.house_id, day, month))
          var currentMin = pMap_high_day((r.plug_id, r.household_id, r.house_id, day, month))._1
          var currentMax = pMap_high_day((r.plug_id, r.household_id, r.house_id, day, month))._2
          if (r.value < currentMin)
            currentMin = r.value
          if (r.value > currentMax)
            currentMax = r.value
          pMap_high_day += ((r.plug_id, r.household_id, r.house_id,day, month) -> (currentMin,currentMax))
        }
      }
      if (r.isWorkMeasurement() && rate < 0) { // low
        if (!pMap_low_day.contains((r.plug_id, r.household_id,r.house_id, day, month)))
          pMap_low_day += ((r.plug_id, r.household_id,r.house_id, day, month) -> (r.value,r.value))
        else {
          pMap_low_day.-((r.plug_id, r.household_id,r.house_id, day, month))
          var currentMin = pMap_low_day((r.plug_id, r.household_id, r.house_id, day, month))._1
          var currentMax = pMap_low_day((r.plug_id, r.household_id, r.house_id, day, month))._2
          if (r.value < currentMin)
            currentMin = r.value
          if (r.value > currentMax)
            currentMax = r.value
          pMap_low_day += ((r.plug_id, r.household_id, r.house_id, day, month) -> (currentMin,currentMax))
        }
      }
    }

    var avg_high_day: Map[(Int,Int,Int,Int,Int),(Float)] = Map()
    var avg_low_day: Map[(Int,Int,Int,Int,Int),(Float)] = Map()

    for (k <- pMap_high_day.keys) {
      avg_high_day += k -> (pMap_high_day(k)._2 - pMap_high_day(k)._1)
    }
    for (k <- pMap_low_day.keys) {
      avg_low_day += k -> (pMap_low_day(k)._2 - pMap_low_day(k)._1)
    }

    var avg_high = Map[(Int,Int,Int,Int),Double]()
    var avg_low = Map[(Int,Int,Int,Int),Double]()

    for (d <- MONTH) {
      for (k <- avg_high_day.keys) {
        if (k._4 == d) {
          if (avg_high.contains((k._1,k._2,k._3,k._5))) {
            val tmp = avg_high((k._1,k._2,k._3,k._5))
            avg_high.-((k._1,k._2,k._3,k._5))
            avg_high += ((k._1,k._2,k._3,k._5) -> (tmp+avg_high_day(k)))

          } else
            avg_high += ((k._1,k._2,k._3,k._5) -> avg_high_day(k))
        }
      }
      for (k <- avg_low_day.keys) {
        if (k._4 == d) {
          if (avg_low.contains((k._1,k._2,k._3,k._5))) {
            val tmp = avg_low((k._1,k._2,k._3,k._5))
            avg_low.-((k._1,k._2,k._3,k._5))
            avg_low += ((k._1,k._2,k._3,k._5) -> (tmp+avg_low_day(k)))

          } else
            avg_low += ((k._1,k._2,k._3,k._5) -> avg_low_day(k))
        }
      }
    }

    for (k <- avg_high.keys) {
      val tmp = avg_high(k)
      avg_high.-(k)
      avg_high += (k -> avg_high(k)/MONTH.length)
    }
    for (k <- avg_low.keys) {
      val tmp = avg_low(k)
      avg_low.-(k)
      avg_low += (k -> avg_low(k)/MONTH.length)
    }

    val results = new Array[((Int,Int,Int,Int),Double)](avg_high.size)

    var i = 0
    for (k <- avg_high.keys) {
      results(i) = (k, avg_high(k)-avg_low(k))
      i += 1
    }

    results.sortBy(_._2).reverse
  }

  def query3Results(values: ListBuffer[PlugData]): Array[((Int,Int,Int,Int),Double)] = {

    var pMap_high: Map[(Int,Int,Int,Int),ListBuffer[(Float,Long)]] = Map()
    var pMap_low: Map[(Int,Int,Int,Int),ListBuffer[(Float,Long)]] = Map()

    for (r <- values) {
      val rate = calendarManager.getPeriodRate(r.timestamp)
      if (r.isWorkMeasurement() && rate > 0) { // high
        if (!pMap_high.contains((r.plug_id, r.household_id, r.house_id, math.abs(rate))))
          pMap_high += ((r.plug_id, r.household_id, r.house_id, math.abs(rate)) -> new ListBuffer[(Float, Long)])
        pMap_high((r.plug_id, r.household_id, r.house_id, math.abs(rate))).append((r.value, r.timestamp))
      }
      if (r.isWorkMeasurement() && rate < 0) { // low
        if (!pMap_low.contains((r.plug_id, r.household_id,r.house_id, math.abs(rate))))
          pMap_low += ((r.plug_id, r.household_id,r.house_id, math.abs(rate)) -> new ListBuffer[(Float,Long)])
        pMap_low((r.plug_id, r.household_id,r.house_id, math.abs(rate))).append((r.value,r.timestamp))
      }
    }


    val n_high = pMap_high.size
    val n_low = pMap_low.size

    val avg_high = new Array[((Int,Int,Int,Int),Double)](n_high)
    val avg_low = new Array[((Int,Int,Int,Int),Double)](n_low)

    var i: Int = 0
    for (k <- pMap_high.keys) {
      breakable {
        val list = pMap_high(k)
        val n: Int = list.size
        if (n == 1) {
          avg_high(i) = ((k._1, k._2, k._3, k._4), 0d)
          i += 1
          break
        }
        list.sortBy(_._2)
        var sum: Float = 0f
        var prev = 0f
        for (v <- list) {
          if (i == 0)
            prev = v._1
          else {
            sum += v._1 - prev
            prev = v._1
          }
        }
        avg_high(i) = ((k._1, k._2, k._3, k._4), sum / n)
        i += 1
      }
    }
    i = 0
    for (k <- pMap_low.keys) {
      breakable {
        val list = pMap_low(k)
        val n: Int = list.size
        if (n == 1) {
          avg_low(i) = ((k._1,k._2,k._3,k._4), 0d)
          i += 1
          break
        }
        var sum : Float = 0f
        list.sortBy(_._2)
        var prev = 0f
        for (v <- list) {
          if (i == 0)
            prev = v._1
          else {
            sum += v._1-prev
            prev = v._1
          }
        }
        avg_low(i) = ((k._1,k._2,k._3,k._4), sum/n)
        i += 1
      }
    }

    val results = new Array[((Int,Int,Int,Int),Double)](avg_high.length)

    for (i <- avg_high.indices) {
      results(i) = (avg_high(i)._1, avg_high(i)._2-avg_low(i)._2)
    }

    results.sortBy(_._2).reverse
  }


  def generateTimestamps(l: Long, l1: Long): Array[Long] = {
    val size = ((l1-l)/20).toInt
    val timestamps = new Array[Long](size+1)
    var i = 0
    for (t <- List.range(l,l1,20)) {
      timestamps(i) = t
      i += 1
    }
    timestamps
  }

  def increment(v: Float) : Float = {
    var tmp = v
    tmp += 0.001f
    tmp
  }

  /**
    *
    * @param filename
    * @param rows
    */
  def writeNumberTestRow(filename: String, rows: ListBuffer[(Int,Long,Float,Int,Int,Int,Int)]): Unit = {

    val file = new PrintWriter(new File(filename))

    for (r <- rows) {
      file.write(r._1.toString)
      file.write(",")
      file.write(r._2.toString)
      file.write(",")
      file.write(r._3.toString)
      file.write(",")
      file.write(r._4.toString)
      file.write(",")
      file.write(r._5.toString)
      file.write(",")
      file.write(r._6.toString)
      file.write(",")
      file.write(r._7.toString)
      file.write("\n")
    }
    file.close()
  }

  def createTestFile(): Unit = {
    val timestamps: Array[Long] = generateTimestamps(TEST_TIMESTAMP_BEGIN,TEST_TIMESTAMP_END)

    //    id,timestamp,value,property,plug id,household id,house id
    val plugValues = new ListBuffer[(Int,Long,Float,Int,Int,Int,Int)]

    val r: Random = new Random(1234)

    var pval = 0f

    for (h <- TEST_HOUSEs) {
      for (hh <- TEST_HOUSEHOLD_IDs) {
        for (p <- TEST_PLUG_IDs) {
          for (prop <- TEST_PROPERTIES) {
            if (prop == 0)
              pval = r.nextFloat() * 1000
            else
              pval = r.nextFloat() * 1000

            for (t <- timestamps) {
              if (t != 0) {
                if (r.nextFloat() < TEST_INCREMENT_PROBABILITY && prop == 0) {
                  pval = increment(pval)
                }
                val id: Int = r.nextInt(10000)
                plugValues.append((id, t, pval, prop, p, hh, h))
              }
            }
          }
        }
      }
    }

    plugValues.sortBy(_._2)

    writeNumberTestRow(TEST_FILENAME, plugValues)
  }
}
