import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.CSVParser

object Query1 {

  val conf = new SparkConf()
  conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
  conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
  val sc = new SparkContext(conf)


  def execute(): Unit = {

    //val data = sc.textFile("hdfs://master:54310/dataset/d14_filtered.csv")
    val data = sc.textFile("dataset/d14_filtered.csv")

    var q1 = data
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
