import config.{Properties, SmartPlugConfig}
import model.PlugData
import org.scalatest.FlatSpec
import utils.{CSVParser, CalendarManager}

import scala.collection.SortedSet
import scala.collection.mutable.ListBuffer

class QueryTwoTestSuite extends FlatSpec {

  val TEST_HOUSE_ID = 1
  val TEST_TIME_PERIOD = 1

  val START_DAY_OF_MONTH = 1
  val END_DAY_OF_MONTH = 30

  val calManager = CalendarManager
  val dataList : ListBuffer[PlugData] = CSVParser.readDataFromLocalFile(SmartPlugConfig.get(Properties.Test.CSV_DATASET_URL))

  /*             (plugID, day, value)         */
  type TestData = (Int, Int, Float)


  /*"Query Two dataset" should "compute mean for a single plug" in {

    val sc = SparkController.defaultSparkContext()

    val c = sc.textFile("dataset/d14_filtered.csv")
      .filter(line => line.endsWith("1,0,0,0"))
      .map(el => println(el) ).count()

    println(c)

  } */

  "Query Two" should "return values " in {


    println("==========   Query Two Test  ==========")

    var valList : ListBuffer[TestData] = ListBuffer()
    var triple = (0,0,0f)
    /* keep track of plug IDs */
    var plugIDs : SortedSet[Int] = SortedSet()
    for(d <- dataList){
      if(d.isWorkMeasurement() && d.house_id == TEST_HOUSE_ID && calManager.getTimeSlot(d.timestamp) == TEST_TIME_PERIOD){
        triple = (d.plug_id, calManager.getDayAndMonth(d.timestamp)(0), d.value)
        plugIDs += d.plug_id
        valList.append( triple )
      }
    }

    val numPlugs = plugIDs.size

    /*for(p <- dataList){
      if(p.isWorkMeasurement() && p.house_id == TEST_HOUSE_ID && calManager.getInterval(p.timestamp) == TEST_TIME_PERIOD && p.plug_id == 1){
        println(calManager.getDayAndMonth(p.timestamp)(0) +" " + p.value )
      }
    } */

    println(s"House $TEST_HOUSE_ID  with $numPlugs plugs ")
    println(s"Days from $START_DAY_OF_MONTH to $END_DAY_OF_MONTH")

    var timePeriodStr = ""
    TEST_TIME_PERIOD  match {
      case 0 => timePeriodStr = "00:00 to 5:59"
      case 1 => timePeriodStr = "6:00 to 11:59"
      case 2 => timePeriodStr = "12:00 to 17:59"
      case 3 => timePeriodStr = "18:00 to 23:59"
    }

    println(s"Time period is from $timePeriodStr")


    var work : Float = 0f
    var workPerDay : Float = 0f
    var daysActiveMeasurement: Set[Int] = Set()
    for(day <- START_DAY_OF_MONTH to END_DAY_OF_MONTH){
      workPerDay = 0f
      for(pid <- 0 until numPlugs) {
        val plugWorkPerDayConsumed = workConsumed(valList, pid, day)
        if(plugWorkPerDayConsumed != 0) daysActiveMeasurement += day
        work += plugWorkPerDayConsumed
        workPerDay += plugWorkPerDayConsumed
        println(s"Plug $pid consumed $plugWorkPerDayConsumed on day $day ")
      }

      println(s"Total day work consumed is $workPerDay")

    }

    val mean = work / (END_DAY_OF_MONTH - START_DAY_OF_MONTH + 1)

    println(s"Total mean per time period is $mean")


    var workVarianceUndivided : Double = 0d
    var workSumPerDay :Float = 0f
    for(day <- START_DAY_OF_MONTH to END_DAY_OF_MONTH) {
      workSumPerDay = 0f
      for (pid <- 0 until numPlugs) {
        val plugWorkPerDayConsumed = workConsumed(valList, pid, day)
        workSumPerDay += plugWorkPerDayConsumed
      }
      workVarianceUndivided += Math.pow(workSumPerDay - mean, 2)
    }

    val variance = workVarianceUndivided / (END_DAY_OF_MONTH - START_DAY_OF_MONTH)
    var std = Math.sqrt(variance)

    if(std == Double.PositiveInfinity) std = 0d
    println(s"Variance is $variance and standard deviation is $std")


  }


  def workConsumed(valList: ListBuffer[TestData], plugId : Int, day: Int): Float = {

    val filtered = valList.filter(el => el._1.equals(plugId) && el._2.equals(day) ).map(f => f._3)
    if(filtered.isEmpty) 0f
    else filtered.max - filtered.min
    /*var min : Float = Float.MaxValue
    var max : Float = 0f
    for(v <- valList){
      if(v._1 == plugId && v._2 == day){
        println(v)
        if(v._3 < min) min = v._3
        if(v._3 > max) max = v._3
      }
    }
    max - min */
  }

}
