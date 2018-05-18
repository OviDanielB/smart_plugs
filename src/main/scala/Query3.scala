import config.{SmartPlugConfig, Properties}
import model.{MeanHolder, SubMeanHolder}
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
          if (d.get.isWorkMeasurement()) {
            val rate = cm.getPeriodRate(d.get.timestamp)
            val day_month = cm.getDayAndMonth(d.get.timestamp)
            val day = day_month(0)
            val month = day_month(1)
            if (rate != 0) { // if in a rate
              Some((d.get.house_id, d.get.household_id, d.get.plug_id, rate, day, month),
                new SubMeanHolder(d.get.value, -1d, 1))
            } else None
          } else None
        )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMean(x,y) // average on single day per rate
      )
     .map(
        d =>  {
          ((d._1._1, d._1._2, d._1._3, d._1._4, d._1._6),
            new MeanHolder(d._2.mean(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      )
      .map {
        case (k,v) =>
          if (k._4 < 0) { // if lowest rate invert sign
            ((k._1,k._2,k._3, math.abs(k._4)), -v.mean())
          } else {
            ((k._1,k._2,k._3, math.abs(k._4)), v.mean())
          }
      }
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()
    q
  }

  def executeFasterCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager)
    : Array[((Int,Int,Int,Int),Double)] = {

    val q = data
      .flatMap {
        line =>
          val f = line.split(",")
          if (f(3).toInt == 0) {                                            // f(3) <- property
            val rate = cm.getPeriodRate(f(1).toLong)                        // f(1) <- timestamp
            val day_month = cm.getDayAndMonth(f(1).toLong)
            val day = day_month(0)
            val month = day_month(1)                                        // f(4) <- plug_id
            if (rate != 0) { // if in a rate                                // f(5) <- household_id
              Some((f(6).toInt, f(5).toInt, f(4).toInt, rate, day, month),  // f(6) <- house_id
                new SubMeanHolder(f(2).toFloat, -1d, 1))                    // f(2) <- value
            } else None
          } else None
      }
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMean(x,y) // average on single day per rate
      )
      .map(
        d =>  {
          ((d._1._1, d._1._2, d._1._3, d._1._4, d._1._6),
            new MeanHolder(d._2.mean(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      )
      .map {
        case (k,v) =>
          if (k._4 < 0) { // if lowest rate invert sign
            ((k._1,k._2,k._3, math.abs(k._4)), -v.mean())
          } else {
            ((k._1,k._2,k._3, math.abs(k._4)), v.mean())
          }
      }
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .collect()

    q
  }

  def executeSlowerParquet(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
  : Array[((Int,Int,Int,Int),Double)] = {

    val q = data
      .flatMap {
        f =>
          if (f(3).toString.toInt == 0) {                                            // f(3) <- property
            val rate = cm.getPeriodRate(f(1).toString.toLong)                        // f(1) <- timestamp
          val day_month = cm.getDayAndMonth(f(1).toString.toLong)
            val day = day_month(0)
            val month = day_month(1)                                                 // f(4) <- plug_id
            if (rate != 0) { // if in a rate                                         // f(5) <- household_id
              Some((f(6).toString.toInt, f(5).toString.toInt, f(4).toString.toInt, rate, day, month),  // f(6) <- house_id
                new SubMeanHolder(f(2).toString.toFloat, -1d, 1))                    // f(2) <- value
            } else None
          } else None
      }
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMean(x,y) // average on single day per rate
      )
      .map(
        d =>  {
          ((d._1._1, d._1._2, d._1._3, d._1._4, d._1._6),
            new MeanHolder(d._2.mean(), 1))
        }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineMean(x,y) // average on month per rate
      )
      .map {
        case (k,v) =>
          if (k._4 < 0) { // if lowest rate invert sign
            ((k._1,k._2,k._3, math.abs(k._4)), -v.mean())
          } else {
            ((k._1,k._2,k._3, math.abs(k._4)), v.mean())
          }
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
      executeCSV(sc, data,cm)                     // 22,1 s
    }
    ProfilingTime.time {
      executeFasterCSV(sc, data, cm)              // 17,5 s BEST
    }
    ProfilingTime.time {
      executeSlowerParquet(sc, data_p.rdd, cm)    // 29 s
    }
  }
}
