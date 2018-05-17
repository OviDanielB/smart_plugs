import config.{SmartPlugConfig, SmartPlugProperties}
import utils.{CSVParser, ProfilingTime}

object Query1 extends Serializable {

  def execute(): Array[Int] = {

    val sc = SparkController.defaultSparkContext()
    val data = sc.textFile(SmartPlugConfig.get(SmartPlugProperties.CSV_DATASET_URL))

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

    q1
  }

  def executeFaster(): Unit = {

    val sc = SparkController.defaultSparkContext()
    val data = sc.textFile(SmartPlugConfig.get(SmartPlugProperties.CSV_DATASET_URL))

    val start = System.currentTimeMillis()
    val q = data
      .map(line => line.split(","))
      .flatMap(f => if (f(3).toInt == 1 && f(2).toFloat >= 350) Some(f(6)) else None)
      .distinct()
      .collect()
    val elapsed = System.currentTimeMillis() - start

    println("elapsed: ", elapsed)
    for (q <- q) {
      println(q)
    }
  }

  def main(args: Array[String]): Unit = {
    ProfilingTime.time {
      execute()
    }
  }
}
