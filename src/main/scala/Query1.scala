import config.{SmartPlugConfig, Properties}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.{CSVParser, ProfilingTime}

object Query1 extends Serializable {

  val LOAD_THRESHOLD = 350

  def executeCSV(sc: SparkContext, fileURL : String): Array[Int] = {
    val data = sc.textFile(fileURL)
    executeCSV(sc, data)
  }

  def executeCSV(sc: SparkContext, data: RDD[String]): Array[Int] = {

    val q = data
      .map(
        line => CSVParser.parse(line))
      .flatMap(
        f => if (f.get.isLoadMeasurement())
              Some((f.get.house_id, f.get.timestamp), f.get.value)
        else None
      )
      .reduceByKey(_ + _)
      .flatMap(
        f =>
          if (f._2 >= LOAD_THRESHOLD) {
            Some(f._1._1)
          } else
            None
      )
      .distinct()
      .collect()
    q
  }

  def executeFasterCSV(sc: SparkContext, filePath: String): Array[Int] = {
    val data = sc.textFile(filePath)
    executeFasterCSV(sc,data)
  }

  def executeFasterCSV(sc: SparkContext, data: RDD[String]): Array[Int] = {

    val q = data
      .flatMap { line =>
        val f = line.split(",")
        val property = f(3).toInt
        val house = f(6).toInt
        val value = f(2).toFloat

        if (property == 1) Some((house, value)) else None
      }
      .reduceByKey(_+_)
      .flatMap(
        f =>
          if (f._2 >= LOAD_THRESHOLD) {
            Some(f._1)
          } else
            None
      )
      .distinct()
      .collect()
    q
  }

  def executeFasterParquet(sc: SparkContext, filePath: String): Array[Int] = {
    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.parquet(filePath)
    executeFasterParquet(sc, data_p.rdd)
  }

  def executeFasterParquet(sc: SparkContext, data: RDD[Row]): Array[Int] = {

    val q = data
      .flatMap {
        line =>
          val property = line(3)
          val house = line(6).toString.toInt
          val value = line(2).toString.toFloat

          if (property == 1) Some((house, value)) else None
      }
      .reduceByKey(_+_)
      .flatMap(
        f =>
          if (f._2 >= LOAD_THRESHOLD) {
            Some(f._1)
          } else
            None
      )
      .distinct()
      .collect()
    q
  }

  def main(args: Array[String]): Unit = {

    val sc = SparkController.defaultSparkContext()
    val data = sc.textFile(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.parquet(SmartPlugConfig.get(Properties.PARQUET_DATASET_URL))

    ProfilingTime.time {
      executeCSV(sc, data)                  // 6,6
    }
    ProfilingTime.time {
      executeFasterCSV(sc, data)            // 2,4 BEST
    }
    ProfilingTime.time {
      executeFasterParquet(sc, data_p.rdd)  // 2,7
    }
  }
}
