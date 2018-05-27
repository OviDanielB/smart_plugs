import config.{Properties, SmartPlugConfig}
import controller.SparkController
import model.{MaxMinHolder, MeanHolder, SubMeanHolder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import utils.{CSVParser, CalendarManager, ProfilingTime, Statistics}

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
    * @param sc Spark context
    * @param data rdd
    * @param cm calendar manager
    * @return ranking of plugs
    */
  def executeCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
    : Array[((Int,Int,Int,Int),Double)] = {

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
        (x,y) => Statistics.computeOnlineMaxMin(x,y)
      )
      .map(
        d =>  {
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
        d =>  {
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
      .map (
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum/count

          if (rate < 0)
            ((house, household, plug, math.abs(rate)), -avg)
          else
            ((house, household, plug, math.abs(rate)), avg)
        }
      )
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    for (x<-q) {
      println(x)
    }

    q
  }

  def executeCSV(sc: SparkContext, filename: String, cm: CalendarManager) : Array[((Int,Int,Int,Int),Double)] = {
    val data = sc.textFile(filename)
    executeCSV(sc, data, cm)
  }

  def executeSlowCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
    : Array[((Int,Int,Int,Int),Double)] = {

    val q = data
      .map(
        line => CSVParser.parse(line)
      )
      .flatMap(
        d =>
          if (d.get.isWorkMeasurement() && d.isDefined) {
            val rate = cm.getPeriodRate(d.get.timestamp)
            val day = cm.getDayOfMonth(d.get.timestamp)
            val hour = cm.getHourOfDay(d.get.timestamp)
            Some((d.get.house_id, d.get.household_id, d.get.plug_id, rate, hour, day),
              new MaxMinHolder(d.get.value,d.get.value))
          } else None
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMaxMin(x,y)
      )
      .map(
        d =>  {
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
        d =>  {
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
      .map (
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum/count

          if (rate < 0)
            ((house, household, plug, math.abs(rate)), -avg)
          else
            ((house, household, plug, math.abs(rate)), avg)
        }
      )
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    q
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
    * @param sc Spark context
    * @param data rdd
    * @param cm calendar manager
    * @return ranking of plugs
    */
  def executeParquet(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
  : Array[((Int,Int,Int,Int),Double)] = {

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
        (x,y) => Statistics.computeOnlineMaxMin(x,y)
      )
      .map(
        d =>  {
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
        d =>  {
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
      .map (
        d => {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4
          val sum = d._2._1
          val count = d._2._2
          val avg = sum/count

          if (rate < 0)
            ((house, household, plug, math.abs(rate)), -avg)
          else
            ((house, household, plug, math.abs(rate)), avg)
        }
      )
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    q
  }


  def main(args: Array[String]): Unit = {
    val sc = SparkController.defaultSparkContext()
    val data = sc.textFile(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    val spark = SparkController.defaultSparkSession()
    val data_p = spark.read.parquet(SmartPlugConfig.get(Properties.PARQUET_DATASET_URL))

    val cm = new CalendarManager

    ProfilingTime.time {
      executeSlowCSV(sc, data,cm)
    }
    ProfilingTime.time {
      executeCSV(sc, data, cm)              // BEST
    }
    ProfilingTime.time {
      executeParquet(sc, data_p.rdd, cm)
    }
  }
}
