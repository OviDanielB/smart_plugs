package Queries

import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import model.MaxMinHolder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import utils.{CalendarManager, ProfilingTime, Statistics}


/**
  * QUERY 2 USING SPARK CORE
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object Query2 extends Serializable {

  /**
    * Compute mean and standard deviation statistics of every house energy consumption
    * during each time slot in [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59].
    * Single value of energy consumption is computed as the difference between the value
    * of the last record of the period and the first one, because it is a cumulative quantity.
    * It does NOT keep into account errors obtained for plugs that have been reset into a period.
    * RDD is created by reading a CSV file (fast version).
    *
    * @param sc Spark context
    * @param data rdd
    * @param cm calendar manager
    * @return array of statistics for each house
    */
  def executeCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager) : Array[((Int,Int),Double,Double)] = {

    val q = data
      .flatMap (
        line => {
          val f = line.split(",")
          val house = f(6).toInt
          val household = f(5).toInt
          val plug = f(4).toInt
          val property = f(3).toInt
          val timestamp = f(1).toLong
          val value = f(2).toFloat

          if (property == 0) {
            val d_m = cm.getDayAndMonth(timestamp)
            val day = d_m(0)
            val month = d_m(1)
            Some((house,household,plug,cm.getTimeSlot(timestamp),day,month),
              new MaxMinHolder(value,value))
          } else None
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMaxMin(x,y) // per plug per slot per day
      )
      .map (
        d => {
          val house = d._1._1
          val slot = d._1._4
          val day = d._1._5
          val month = d._1._6

          ((house,slot,day,month), d._2.delta())
        }
      )
      .reduceByKey(_+_) // per day per house as sum of per day per plug
      .map(
        d => {
          val house = d._1._1
          val slot = d._1._2
          val value = d._2

          ((house,slot), (value, 1, math.pow(value,2)))
        }
      )
      .reduceByKey {
        case ((sumL, countL, powL), (sumR, countR, powR)) =>

          (sumL + sumR, countL + countR, powL + powR)
      }
      .map (
        d => {
          val key = d._1
          val sum = d._2._1
          val count = d._2._2
          val sum_pow = d._2._3
          val avg = sum/count
          val stddev = math.sqrt(sum_pow/count - math.pow(avg,2))

          (key, avg, stddev)
        }
      )
      .collect()

    q
  }

  def executeCSV(sc: SparkContext, filename: String, cm: CalendarManager) : Array[((Int,Int),Double,Double)] = {
    val data = sc.textFile(filename)
    executeCSV(sc, data, cm)
  }

  /**
    * Compute mean and standard deviation statistics of every house energy consumption
    * during each time slot in [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59].
    * Single value of energy consumption is computed as the difference between the value
    * of the last record of the period and the first one, because it is a cumulative quantity.
    * It does NOT keep into account errors obtained for plugs that have been reset into a period.
    * RDD is created by converting a DataFrame read from a Parquet file.
    *
    * @param sc Spark context
    * @param data rdd
    * @param cm calendar manager
    * @return array of statistics for each house
    */
  def executeOnRow(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
  : Array[((Int,Int),Double,Double)] = {

    val q = data
      .flatMap (
        f => {
          val house = f(6).toString.toInt
          val household = f(5).toString.toInt
          val plug = f(4).toString.toInt
          val property = f(3).toString.toInt
          val timestamp = f(1).toString.toLong
          val value = f(2).toString.toFloat

          if (property == 0) {
            val d_m = cm.getDayAndMonth(timestamp)
            val day = d_m(0)
            val month = d_m(1)
            Some((house,household,plug,cm.getTimeSlot(timestamp),day,month),
              new MaxMinHolder(value,value))
          } else None
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMaxMin(x,y) // per day
      )
      .map (
        d => {
          val house = d._1._1
          val slot = d._1._4
          val day = d._1._5
          val month = d._1._6

          ((house,slot,day,month), d._2.delta())
        }
      )
      .reduceByKey(_+_) // per day per house as sum of per day per plug
      .map(
        d => {
          val house = d._1._1
          val slot = d._1._2
          val value = d._2

          ((house,slot), (value, 1, math.pow(value,2)))
        }
      )
      .reduceByKey {
        case ((sumL, countL, powL), (sumR, countR, powR)) =>

          (sumL + sumR, countL + countR, powL + powR)
      }
      .map (
        d => {
          val key = d._1
          val sum = d._2._1
          val count = d._2._2
          val sum_pow = d._2._3
          val avg = sum/count
          val stddev = math.sqrt(sum_pow/count - math.pow(avg,2))

          (key, avg, stddev)
        }
      )
      .collect()

    q
  }

  def main(args: Array[String]): Unit = {

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

    val cm = new CalendarManager

    ProfilingTime.time {
      executeCSV(sc, data, cm)
    }
    ProfilingTime.time {
      executeOnRow(sc, data_p.rdd, cm)
    }

    ProfilingTime.time {
      executeOnRow(sc, data_a.rdd, cm)
    }

  }
}
