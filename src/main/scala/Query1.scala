import config.{SmartPlugConfig, SmartPlugProperties}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.{CSVParser, ProfilingTime}

object Query1 extends Serializable {

  def executeCSV(sc: SparkContext, data: RDD[String]): Array[Int] = {

    val q1 = data
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
          if (f._2 >= 350) {
            Some(f._1._1)
          } else
            None
      )
      .distinct()
      .collect()
    q1
  }

  def executeFasterCSV(sc: SparkContext, data: RDD[String]): Array[Int] = {

    val q = data
      .flatMap { line =>
        val f = line.split(",")
        if (f(3).toFloat == 1) Some((f(6).toInt, f(2).toFloat)) else None
      }
      .reduceByKey(_+_)
      .flatMap(
      f =>
        if (f._2 >= 350) {
          Some(f._1)
        } else
          None
      )
      .distinct()
      .collect()
    q
  }

  def executeFasterParquet(sc: SparkContext, data: RDD[Row]): Array[Int] = {

    val q = data
      .flatMap { line =>
        if (line(3) == 1) Some((line(6).toString.toInt, line(2).toString.toFloat)) else None
      }
      .reduceByKey(_+_)
      .flatMap(
        f =>
          if (f._2 >= 350) {
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
    val data = sc.textFile(SmartPlugConfig.get(SmartPlugProperties.CSV_DATASET_URL))

    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.parquet(SmartPlugConfig.get(SmartPlugProperties.PARQUET_DATASET_URL))

    ProfilingTime.time {
      executeFasterCSV(sc, data)
    }
    ProfilingTime.time {
      executeCSV(sc, data)
    }
    ProfilingTime.time {
      executeFasterParquet(sc, data_p.rdd)
    }
  }
}
