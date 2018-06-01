package Queries

import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.ProfilingTime
import com.databricks.spark.avro._


/**
  * QUERY 1 USING SPARK CORE
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object Query1 extends Serializable {

  val LOAD_THRESHOLD = 350


  /**
    * Find houses with a instantaneous energy consumption greater or equal to 350 Watt
    * analyzing data read from a CSV file.
    * Fast parsing CSV version.
    *
    * @param sc   spark context
    * @param data rdd
    * @return array of houses id
    */
  def executeCSV(sc: SparkContext, data: RDD[String]): Array[Int] = {

    val q = data
      .flatMap { line =>
        val f = line.split(",")
        val property = f(3).toInt
        val house = f(6).toInt
        val value = f(2).toFloat
        val timestamp = f(1).toLong

        if (property == 1) Some(((house, timestamp), value)) else None
      }
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

  /**
    * Find houses with a instantaneous energy consumption greater or equal to 350 Watt
    * analyzing data converting a DataFrame read from a Parquet file.
    *
    * @param sc   spark context
    * @param data rdd
    * @return array of houses id
    */
  def executeOnRow(sc: SparkContext, data: RDD[Row]): Array[Int] = {

    val q = data
      .flatMap {
        line =>
          val property = line(3)
          val house = line(6).toString.toInt
          val value = line(2).toString.toFloat
          val timestamp = line(1).toString.toLong

          if (property == 1) Some(((house, timestamp), value)) else None
      }
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

  def executeCSV(sc: SparkContext, fileURL: String): Array[Int] = {
    val data = sc.textFile(fileURL)
    executeCSV(sc, data)
  }

  def executeParquet(sc: SparkContext, filePath: String): Array[Int] = {
    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.parquet(filePath)
    executeOnRow(sc, data_p.rdd)
  }

  def executeAvro(sc: SparkContext, filePath: String): Array[Int] = {
    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.avro(filePath)
    executeOnRow(sc, data_p.rdd)
  }

  def main(args: Array[String]): Unit = {

    //    val sc = SparkController.sparkContextNoMaster
    val sc = SparkController.defaultSparkContext()
    val spark = SparkController.defaultSparkSession()

    var datasetCSV: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)
    var datasetParquet: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)
    var datasetAvro: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)

    if (args.length == 3) {
      datasetCSV = args(0)
      datasetParquet = args(1)
      datasetAvro = args(2)
    } else if (args.length != 0) {
      println("Required params: csv path, parquet path, avro path!")
    }

    val data = sc.textFile(datasetCSV)
    val data_p = spark.read.parquet(datasetParquet)
    val data_a = spark.read.avro(datasetAvro)


    ProfilingTime.time {
      executeCSV(sc, data)
    }

    ProfilingTime.time {
      executeOnRow(sc, data_p.rdd)
    }

    ProfilingTime.time {
      executeOnRow(sc, data_a.rdd)
    }

  }
}
