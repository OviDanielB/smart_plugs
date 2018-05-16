import config.SmartPlugConfig
import model.MeanStdHolder
import org.apache.spark.{SparkConf, SparkContext}
import utils.{CSVParser, CalendarManager, Statistics, ProfilingTime}

object Query2 extends Serializable {

  def execute(): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
    conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
    val sc: SparkContext = new SparkContext(conf)
    val cm: CalendarManager = new CalendarManager

    val data = sc.textFile("dataset/d14_filtered.csv")

    val q2 = data
      .map(
        line => CSVParser.parse(line)
      )
      .filter(
        f => f.get.isWorkMeasurement()
      )
      .map {
        d =>((d.get.house_id, cm.getInterval(d.get.timestamp)), new MeanStdHolder(d.get.value, 1, 0d))
      }
      .reduceByKey( (x,y) =>
         Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map {
        case (k,v) => (k, v.mean(), v.std())
      }
      .sortBy(_._1)
      .collect()

    for (q <- q2) {
      println(q)
    }
  }

  def main(args: Array[String]): Unit = {
    ProfilingTime.time {
      execute()
    }
  }
}
