import config.SmartPlugConfig
import model.MeanStdHolder
import org.apache.spark.{SparkConf, SparkContext}
import utils.{CSVParser, CalendarManager, Statistics}

object Query2 extends Serializable {

  def execute(): Unit = {

    val sc: SparkContext = SparkController.defaultSparkContext()

    val cm: CalendarManager = new CalendarManager

    val data = sc.textFile("dataset/d14_filtered.csv")

    val q2 = data
      .map( line => CSVParser.parse(line) )
      .filter( f => f.isDefined && f.get.isWorkMeasurement() )
      .map {
        data =>
          val current = data.get
          ((current.house_id, cm.getInterval(current.timestamp)), new MeanStdHolder(current.value))
      }
      .reduceByKey( (x,y) => Statistics.computeOnlineMeanAndStd(x,y))
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )
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
