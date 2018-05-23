import java.io.{File, PrintWriter}

import QueryOneSQL.{customSchema, spark}
import org.scalatest.FlatSpec
import config.{Properties, SmartPlugConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import utils.{CalendarManager, ProfilingTime}
import com.databricks.spark.avro._


class ProfilingQueryTime extends FlatSpec {

  val TIMES_FILENAME : String = "dataset/times.csv"

  val CSV_FILE : String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)

  val PARQUET_FILE : String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)

  val AVRO_FILE : String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)

  val sparkContext : SparkContext = SparkController.defaultSparkContext()

  val sparkSession : SparkSession = SparkController.defaultSparkSession()

  val calendarManager : CalendarManager = new CalendarManager

  val schema : StructType = SparkController.defaultCustomSchema()

  computeTimes()

  def computeTimes(): Unit = {

    val dataCSV = sparkContext.textFile(CSV_FILE)
    val dataFramePARQUET = sparkSession.read.parquet(PARQUET_FILE)
    val dataFrameAVRO = sparkSession.read.avro(AVRO_FILE)
    val dataFrameCSV = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load(CSV_FILE)

    var res : Map[String,Double] = Map()

    var t = ProfilingTime.getTime {
      Query1.executeCSV(sparkContext, dataCSV)
    }
    res += ("query1csv_slow" -> t)

    t = ProfilingTime.getTime {
      Query1.executeFasterCSV(sparkContext, dataCSV)
    }
    res += ("query1csv_fast" -> t)

    t = ProfilingTime.getTime {
      Query1.executeParquet(sparkContext, dataFramePARQUET.rdd)
    }
    res += ("query1parquet" -> t)

    t = ProfilingTime.getTime {
      QueryOneSQL.execute(dataFrameCSV)
    }
    res += ("query1SQLcsv" -> t)

    t = ProfilingTime.getTime {
      QueryOneSQL.execute(dataFramePARQUET)
    }
    res += ("query1SQLparquet" -> t)

    t = ProfilingTime.getTime {
      QueryOneSQL.execute(dataFrameAVRO)
    }
    res += ("query1SQLavro" -> t)

    t = ProfilingTime.getTime {
      Query2.executeCSV(sparkContext, dataCSV, calendarManager)
    }
    res += ("query2csv_slow" -> t)

    t = ProfilingTime.getTime {
      Query2.executeFasterCSV(sparkContext, dataCSV, calendarManager)
    }
    res += ("query2csv_fast" -> t)

    t = ProfilingTime.getTime {
      Query2.executeParquet(sparkContext, dataFramePARQUET.rdd, calendarManager)
    }
    res += ("query2parquet" -> t)

    t = ProfilingTime.getTime {
      QueryTwoSQL.executeOnSlot(dataFrameCSV)
    }
    res += ("query2SQLcsv" -> t)

    t = ProfilingTime.getTime {
      QueryTwoSQL.executeOnSlot(dataFrameCSV)
    }
    res += ("query2SQLparquet" -> t)

    t = ProfilingTime.getTime {
      QueryTwoSQL.executeOnSlot(dataFrameAVRO)
    }
    res += ("query2SQLavro" -> t)

    t = ProfilingTime.getTime {
      Query3.executeCSV(sparkContext, dataCSV, calendarManager)
    }
    res += ("query3csv_slow" -> t)

    t = ProfilingTime.getTime {
      Query3.executeFasterCSV(sparkContext, dataCSV, calendarManager)
    }
    res += ("query3csv_fast" -> t)

    t = ProfilingTime.getTime {
      Query3.executeParquet(sparkContext, dataFramePARQUET.rdd, calendarManager)
    }
    res += ("query3parquet" -> t)

    t = ProfilingTime.getTime {
      QueryThreeSQL.execute(dataFrameCSV)
    }
    res += ("query3SQLcsv" -> t)

    t = ProfilingTime.getTime {
      QueryThreeSQL.execute(dataFramePARQUET)
    }
    res += ("query3SQLparquet" -> t)

    t = ProfilingTime.getTime {
      QueryThreeSQL.execute(dataFrameAVRO)
    }
    res += ("query3SQLavro" -> t)

    writeTimesToCSV(res)
  }

  def writeTimesToCSV(res: Map[String,Double]): Unit = {

    val file = new PrintWriter(new File(TIMES_FILENAME))

    // header
    for (k <- res.keys) {
      file.write(k)
      file.write(",")
    }
    file.write("\n")

    // results
    for (k <- res.keys) {
      file.write(res(k).toString)
      file.write(",")
    }

    file.close()
  }
}
