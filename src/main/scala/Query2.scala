import config.{Properties, SmartPlugConfig}
import model.{MeanStdHolder, SubMeanStdHolder}
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
           if (d.get.isWorkMeasurement() && !(d.get.value == 0)) {
             Some((d.get.house_id, d.get.household_id, d.get.plug_id, cm.getInterval(d.get.timestamp)),
               new SubMeanStdHolder(d.get.value, -1d, 1, 0d, d.get.timestamp)) // giorno mese
           } else {
             None
           }
      )
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMeanAndStd(x,y) // average on single day
      )
      .map(
        d => ((d._1._1,d._1._4), new MeanStdHolder(d._2.mean(), 1, 0d))
      )
      .reduceByKey( (x,y) =>
         Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) ) // TODO compare with mapValues
      .sortBy(_._1)
      .collect()

    q
  }

  def executeCSV(sc: SparkContext, cm: CalendarManager, filePath: String):
    Array[((Int,Int),Double,Double)] = {
    val data = sc.textFile(filePath)
    executeCSV(sc,data,cm)
  }


  def executeFasterCSV(sc: SparkContext, data: RDD[String], cm: CalendarManager): Array[((Int,Int),Double,Double)] = {

    val q = data
      .flatMap { line =>
        val f = line.split(",")
        if (f(3).toInt == 0)
          Some((f(6).toInt, f(5).toInt, f(4).toInt, cm.getInterval(f(1).toLong)),
            new SubMeanStdHolder(f(2).toFloat, -1d, 1, 0d, f(1).toLong))
        else None
      }
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMeanAndStd(x,y)
      )
      .map(
        d => ((d._1._1,d._1._4), new MeanStdHolder(d._2.mean(), 1, 0d))
      )
      .reduceByKey( (x,y) =>
        Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )
      .sortBy(_._1)
      .collect()

    q
  }

  def executeParquet(sc: SparkContext, data: RDD[Row], cm: CalendarManager)
  : Array[((Int,Int),Double,Double)] = {

    val q = data
      .flatMap { f =>
        if (f(3).toString.toInt == 0)
          Some((f(6).toString.toInt, f(5).toString.toInt, f(4).toString.toInt, cm.getInterval(f(1).toString.toLong)),
            new SubMeanStdHolder(f(2).toString.toFloat, -1d, 1, 0d, f(1).toString.toLong))
        else None
      }
      .reduceByKey(
        (x,y) => Statistics.computeOnlineSubMeanAndStd(x,y)
      )
      .map(
        d => ((d._1._1,d._1._4), new MeanStdHolder(d._2.mean(), 1, 0d))
      )
      .reduceByKey( (x,y) =>
        Statistics.computeOnlineMeanAndStd(x,y)
      )
      .map(stat => (stat._1, stat._2.mean(), stat._2.std()) )
      .sortBy(_._1)
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
      executeCSV(sc, data, cm)           // 2,9 s
    }
    ProfilingTime.time {
      executeFasterCSV(sc, data, cm)     // 2,3 s BEST
    }
    ProfilingTime.time {
      executeParquet(sc, data_p.rdd, cm) // 4,7 s
    }
  }
}
