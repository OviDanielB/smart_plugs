import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.{CSVParser, CalendarManager, Statistics}

object Query3 {

  var conf : SparkConf = new SparkConf()
  conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
  conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
  val sc: SparkContext = new SparkContext(conf)
  val cm: CalendarManager = new CalendarManager

  def execute(): Unit = {

    val data = sc.textFile("dataset/d14_filtered.csv")

    val q3 = data
      .map(
        line => CSVParser.parse(line)
      )
      .filter(
        f => f.get.isWorkMeasurement()
      )
      .map(
        d => (
          (d.get.house_id, d.get.household_id, d.get.plug_id, cm.getPeriodRate(d.get.timestamp)), (d.get.value, 1))
      )
      .filter(
        f => f._1._4 != 0 // if in one of the rate slot
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y)
      )
      .map {
        case (k,v) =>
          if (k._4 < 0) { // if lowest rate invert sign
            ((k._1,k._2,k._3, math.abs(k._4)), -v._1)
          } else {
            ((k._1,k._2,k._3, math.abs(k._4)), v._1)
          }
      }
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    for (q <- q3) {
      println(q)
    }
  }

  def main(args: Array[String]): Unit = {
    execute()
  }
}
