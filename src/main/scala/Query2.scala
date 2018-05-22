import config.{Properties, SmartPlugConfig}
import model.{MaxMinHolder, MeanStdHolder, SubMeanStdHolder}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import utils.{CSVParser, CalendarManager, ProfilingTime, Statistics}

object Query2 extends Serializable {


  def executeCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager): Array[((Int,Int),Double,Double)] = {

    val q = data
      .map(
        line => CSVParser.parse(line)
      )
      .flatMap (
        d =>
          if (d.get.isWorkMeasurement() && d.isDefined) {
            val d_m = cm.getDayAndMonth(d.get.timestamp)
            val day = d_m(0)
            val month = d_m(1)
            Some((d.get.house_id, d.get.household_id, d.get.plug_id, cm.getTimeSlot(d.get.timestamp), day,month),
              new MaxMinHolder(d.get.value,d.get.value))
          } else {
            None
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
        d => ((d._1._1,d._1._2), new MeanStdHolder(d._2, 1, 0d))
      )
      .reduceByKey( (x,y) =>
        Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )

      .collect()

    q
  }

  def executeFasterCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager): Array[((Int,Int),Double,Double)] = {

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

          if (property == 0 && value != 0) {
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
      d => ((d._1._1,d._1._2), new MeanStdHolder(d._2, 1, 0d))
    )
      .reduceByKey( (x,y) =>
        Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )

      .collect()

    q
  }

  def executeParquet(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
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

          if (property == 0 && value != 0) {
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
      d => ((d._1._1,d._1._2), new MeanStdHolder(d._2, 1, 0d))
    )
      .reduceByKey( (x,y) =>
        Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )

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
      executeCSV(sc, data, cm)
    }
    ProfilingTime.time {
      executeFasterCSV(sc, data, cm)     // BEST
    }
    ProfilingTime.time {
      executeParquet(sc, data_p.rdd, cm)
    }
  }
}
