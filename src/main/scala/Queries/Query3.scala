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
  * QUERY 3 USING SPARK CORE
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object Query3 extends Serializable {

  /**
    * Compute plugs ranking based on the difference between monthly average values
    * of energy consumption during the high rate periods and low rate periods.
    * Those plugs that have higher value of the difference are at the top of the
    * ranking.
    * Single value of energy consumption is retrieved computing difference between
    * maximum and minimum cumulative value in a hour.
    * RDD is created by reading a CSV file.
    *
    * @param sc   Spark context
    * @param data rdd
    * @param cm   calendar manager
    * @return ranking of plugs
    */
  def executeCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
  : Array[((Int, Int, Int, Int), Double)] = {

    val q = data
      .flatMap(
        line => {
          val f = line.split(",")
          val house = f(6).toInt
          val household = f(5).toInt
          val plug = f(4).toInt
          val property = f(3).toInt
          val timestamp = f(1).toLong
          val value = f(2).toFloat

          if (property == 0) {
            val rate = cm.getPeriodRate(timestamp)
            val day = cm.getDayOfYear(timestamp)
            val hour = cm.getHourOfDay(timestamp)
            Some((house, household, plug, rate, hour, day),
              new MaxMinHolder(value, value))
          } else None
        }
      )
      .reduceByKey(
        (x, y) => Statistics.computeOnlineMaxMin(x, y)
      )
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val day = d._1._6
          val value = d._2

          ((house, household, plug, rate, day), (value.delta(), 1))
        }
      )
      .reduceByKey {
        case ((sum1, count1), (sum2, count2)) =>

          (sum1 + sum2, count1 + count2)
      }
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum / count

          ((house, household, plug, rate), (avg, 1))
        }
      )
      .reduceByKey {
        case ((sum1, count1), (sum2, count2)) =>

          (sum1 + sum2, count1 + count2)
      }
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum / count

          if (rate < 0)
            ((house, household, plug, math.abs(rate)), -avg)
          else
            ((house, household, plug, math.abs(rate)), avg)
        }
      )
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()

    q
  }

  def executeCSV(sc: SparkContext, filename: String, cm: CalendarManager): Array[((Int, Int, Int, Int), Double)] = {
    val data = sc.textFile(filename)
    executeCSV(sc, data, cm)
  }

  /**
    * Compute plugs ranking based on the difference between monthly average values
    * of energy consumption during the high rate periods and low rate periods.
    * Those plugs that have higher value of the difference are at the top of the
    * ranking.
    * Single value of energy consumption is retrieved computing difference between
    * maximum and minimum cumulative value in a hour.
    * RDD is created by converting a DataFrame read from a Parquet file.
    *
    * @param sc   Spark context
    * @param data rdd
    * @param cm   calendar manager
    * @return ranking of plugs
    */
  def executeOnRow(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
  : Array[((Int, Int, Int, Int), Double)] = {

    val q = data
      .flatMap(
        f => {
          val house = f(6).toString.toInt
          val household = f(5).toString.toInt
          val plug = f(4).toString.toInt
          val property = f(3).toString.toInt
          val timestamp = f(1).toString.toLong
          val value = f(2).toString.toFloat

          if (property == 0 && value != 0) {
            val rate = cm.getPeriodRate(timestamp)
            val day = cm.getDayOfYear(timestamp)
            val hour = cm.getHourOfDay(timestamp)
            Some((house, household, plug, rate, hour, day),
              new MaxMinHolder(value, value))
          } else None
        }
      )
      .reduceByKey(
        (x, y) => Statistics.computeOnlineMaxMin(x, y)
      )
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val day = d._1._6
          val value = d._2

          ((house, household, plug, rate, day), (value.delta(), 1))
        }
      )
      .reduceByKey {
        case ((sum1, count1), (sum2, count2)) =>

          (sum1 + sum2, count1 + count2)
      }
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum / count

          ((house, household, plug, rate), (avg, 1))
        }
      )
      .reduceByKey {
        case ((sum1, count1), (sum2, count2)) =>

          (sum1 + sum2, count1 + count2)
      }
      .map(
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum / count

          if (rate < 0)
            ((house, household, plug, math.abs(rate)), -avg)
          else
            ((house, household, plug, math.abs(rate)), avg)
        }
      )
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
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
