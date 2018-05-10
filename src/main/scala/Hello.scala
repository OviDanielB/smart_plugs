import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}
import utils.CSVParser

object Hello {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
    conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
    val sc = new SparkContext(conf)

    //val data = sc.textFile("hdfs://localhost:54310/dataset/d14_filtered.csv")

    //val data = sc.textFile("/dataset/d14_filtered.csv")

    val data = sc.textFile(args(0))
    var list =
      data.map(line => line.length)
      .saveAsTextFile(args(1))

      /*data
      .map(line => CSVParser.parse(line))
      .filter(f => !None.contains(f))
      .filter(f => f.get.property && f.get.value >= 350)
      .map(data => (data.get.house_id, data.get.value))
      .groupByKey()
      .collect() */

  }
}
