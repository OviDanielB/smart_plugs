import config.{Properties, SmartPlugConfig}
import model.{MaxMinHolder, MeanHolder, SubMeanHolder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.SparkContext
import utils.{CSVParser, CalendarManager, ProfilingTime, Statistics}

object Query3 extends Serializable {

  def executeCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
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
            Some((d.get.house_id, d.get.household_id, d.get.plug_id, rate, day),
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

          ((house, household, plug, rate), new MeanHolder(d._2.delta(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      ).map {
        case (k, v) =>
          if (k._4 < 0)  // if lowest rate invert sign
            ((k._1, k._2, k._3, math.abs(k._4)), -v.mean())
          else
            ((k._1, k._2, k._3, math.abs(k._4)), v.mean())
        }
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    for (x<-q) {
      println(x._1,x._2)
    }

    q
  }

  def executeCSV(sc: SparkContext, cm: CalendarManager, filePath: String):
  Array[((Int,Int,Int,Int),Double)] = {
    val data = sc.textFile(filePath)
    executeCSV(sc,data,cm)
  }

  def executeFasterCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
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
            Some((house, household, plug, rate, day),
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

          ((house, household, plug, rate), new MeanHolder(d._2.delta(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      )
      .map {
        case (k, v) =>
          if (k._4 < 0)  // if lowest rate invert sign
            ((k._1, k._2, k._3, math.abs(k._4)), -v.mean())
          else
            ((k._1, k._2, k._3, math.abs(k._4)), v.mean())
      }
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()
    q
  }

  def executeSlowerParquet(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
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
            Some((house, household, plug, rate, day),
              new MaxMinHolder(value, value))
          } else None
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMaxMin(x,y) // average on day per rate
      )
      .map(
        d =>  {
          val house = d._1._1
          val household = d._1._2
          val plug = d._1._3
          val rate = d._1._4

          ((house, household, plug, rate), new MeanHolder(d._2.delta(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      )
      .map {
        case (k, v) =>
          if (k._4 < 0)  // if lowest rate invert sign
            ((k._1, k._2, k._3, math.abs(k._4)), -v.mean())
          else
            ((k._1, k._2, k._3, math.abs(k._4)), v.mean())
      }
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
      executeCSV(sc, data,cm)
    }
//    ProfilingTime.time {
//      executeFasterCSV(sc, data, cm)              // BEST
//    }
//    ProfilingTime.time {
//      executeSlowerParquet(sc, data_p.rdd, cm)
//    }
  }
}
