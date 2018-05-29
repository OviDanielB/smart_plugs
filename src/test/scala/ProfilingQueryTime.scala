import QueryOneSQL.spark
import org.scalatest.FlatSpec
import config.{Properties, SmartPlugConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import utils.{CSVParser, CalendarManager, ProfilingTime}
import com.databricks.spark.avro._
import controller.SparkController


class ProfilingQueryTime extends FlatSpec {

  val TIMES_FILENAME: String = "dataset/times.csv"

  val CSV_FILE: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL_FILTERED)

  val PARQUET_FILE: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL_FILTERED)

  val AVRO_FILE: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL_FILTERED)

  val sparkContext: SparkContext = SparkController.defaultSparkContext()

  val sparkSession: SparkSession = SparkController.defaultSparkSession()

  val calendarManager: CalendarManager = new CalendarManager

  val schema: StructType = SparkController.defaultCustomSchema()

  computeTimes()

  def computeTimes(): Unit = {

    val RUN = 5 // Number of runs to compute mean execution time

    val dataCSV = sparkContext.textFile(CSV_FILE).cache()

    val dataFramePARQUET = sparkSession.read.parquet(PARQUET_FILE).cache()

    val dataFrameAVRO = sparkSession.read.avro(AVRO_FILE)
    val dataFrameCSV = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load(CSV_FILE)
      .cache()

    var res: Map[String, Double] = Map() // keep results

    /*
      Query 1
     */

    var t = ProfilingTime.getMeanTime(RUN, Query1.executeSlowCSV(sparkContext, dataCSV))
    res += ("query1csv_slow" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query1.executeCSV(sparkContext, dataCSV))
    res += ("query1csv_fast" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query1.executeParquet(sparkContext, dataFramePARQUET.rdd))
    res += ("query1parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query1.executeParquet(sparkContext, dataFrameAVRO.rdd))
    res += ("query1avro" -> t)

    /*
      Query 1 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameCSV))
    res += ("query1SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFramePARQUET))
    res += ("query1SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameAVRO))
    res += ("query1SQLavro" -> t)

    /*
      Query 2
     */

    t = ProfilingTime.getMeanTime(RUN, Query2.executeSlowCSV(sparkContext, dataCSV, calendarManager))
    res += ("query2csv_slow" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query2.executeCSV(sparkContext, dataCSV, calendarManager))
    res += ("query2csv_fast" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query2.executeParquet(sparkContext, dataFramePARQUET.rdd, calendarManager))
    res += ("query2parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query2.executeParquet(sparkContext, dataFrameAVRO.rdd, calendarManager))
    res += ("query2avro" -> t)

    /*
      Query 2 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.executeOnSlot(dataFrameCSV))
    res += ("query2SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.executeOnSlot(dataFramePARQUET))
    res += ("query2SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.executeOnSlot(dataFrameAVRO))
    res += ("query2SQLavro" -> t)

    /*
      Query 3
     */

    t = ProfilingTime.getMeanTime(RUN, Query3.executeSlowCSV(sparkContext, dataCSV, calendarManager))
    res += ("query3csv_slow" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query3.executeCSV(sparkContext, dataCSV, calendarManager))
    res += ("query3csv_fast" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query3.executeParquet(sparkContext, dataFramePARQUET.rdd, calendarManager))
    res += ("query3parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query3.executeParquet(sparkContext, dataFrameAVRO.rdd, calendarManager))
    res += ("query3avro" -> t)

    /*
      Query 3 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameCSV))
    res += ("query3SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFramePARQUET))
    res += ("query3SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameAVRO))
    res += ("query3SQLavro" -> t)

    // Write results on file as CSV
    CSVParser.writeTimesToCSV(res, TIMES_FILENAME)
  }
}
