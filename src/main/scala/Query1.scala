import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.CSVParser

object Query1 extends Serializable {

  def execute(): Unit = {

    val sc = SparkController.defaultSparkContext()

    //val data = sc.textFile("hdfs://master:54310/dataset/d14_filtered.csv")
    val data = sc.textFile("dataset/d14_filtered.csv")

    val q1 = data
        .map(
          line => CSVParser.parse(line))
        .filter(
          f => f.get.isLoadMeasurement && f.get.value >= 350
        )
        .map(
          data => (data.get.house_id, 1)
        )
        .groupByKey()
        .map(v => v._1)
        .collect()

    for (q <- q1) {
      println(q)
    }
  }

  def main(args: Array[String]): Unit = {
    execute()
  }
}
