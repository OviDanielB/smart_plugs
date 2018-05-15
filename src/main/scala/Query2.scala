import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.{CSVParser, CalendarManager}

object Query2 {

  var conf : SparkConf = new SparkConf()
  conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
  conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
  val sc: SparkContext = new SparkContext(conf)
  val cm: CalendarManager = new CalendarManager

  def execute(): Unit = {

    val data = sc.textFile("dataset/d14_filtered.csv")

    val q2 = data
      .map(
        line => CSVParser.parse(line)
      )
      .filter(
        f => !f.get.property
      )
      .map(
        d => ((d.get.house_id, cm.getInterval(d.get.timestamp)), (d.get.value, d.get.value, 1))
      )
      .reduceByKey(
        (v1,v2) => (v1._1+v2._1, math.pow(v1._1,2).toFloat+math.pow(v2._1,2).toFloat, v1._3+v2._3)
      )
//        .aggregateByKey((0.0,0))((v1,v2) => (v1._1+v2._1,v1._2+v2._2), (v1,v2) => (v1._1+v2._1,v1._2+v2._2))
      .map{
        case(k, v) =>

          val mean = v._1 / v._3
          val dev = math.sqrt((v._2 - math.pow(mean,2))/v._3)

          (k, mean, dev)
      }
      .sortBy(_._1)
      .collect()

    for (q <- q2) {
      println(q)
    }
  }

  def main(args: Array[String]): Unit = {
    execute()
  }
}
