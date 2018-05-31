import Queries._
import com.google.gson.Gson
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.sql.types.StructType
import utils.{CalendarManager, ProfilingTime}
import com.databricks.spark.avro._


object BenchmarkMain {

  def main(args: Array[String]): Unit = {

    val calendarManager: CalendarManager = new CalendarManager
    val schema: StructType = SparkController.defaultCustomSchema()

    //    val sparkContext = SparkController.sparkContextNoMaster
    var sparkContext = SparkController.defaultSparkContext()
    var sparkSession = SparkController.defaultSparkSession()

    /*
       Default path to dataset and output file
     */
    var outputPath = SmartPlugConfig.get(Properties.JSON_TIMES_URL)
    var datasetPathCSV: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)
    var datasetPathParquet: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)
    var datasetPathAvro: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)
    var deployMode = "local"
    var cacheOrNot = "no_cache"

    if (args.length == 6) {
      outputPath = args(0)
      datasetPathCSV = args(1)
      datasetPathParquet = args(2)
      datasetPathAvro = args(3)
      deployMode = args(4)
      if (deployMode.equals("cluster")) {
        sparkContext = SparkController.sparkContextNoMaster
        sparkSession = SparkController.sparkSessionNoMaster
      }
      cacheOrNot = args(5)
    } else if (args.length != 0) {
      println("Required params: csv path, parquet path, avro path!")
    }

    val gson: Gson = new Gson

    /*
       Get RDD[String] from csv file
     */
    var rddCSV = sparkContext.textFile(datasetPathCSV)

    /*
      Get dataframes from Parquet and Avro files
     */
    var dataFrameParquet = sparkSession.read.parquet(datasetPathParquet)
    var dataFrameAvro = sparkSession.read.avro(datasetPathAvro)
    var dataFrameCSV = sparkSession.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    if (cacheOrNot.equals("cache")) {
      rddCSV = rddCSV.cache()
      dataFrameParquet = dataFrameParquet.cache()
      dataFrameAvro = dataFrameAvro.cache()
      dataFrameCSV = dataFrameCSV.cache()
    }

    /*
      Spark core queries
      Note: For queries on Parquet and Avro, dataframes are converted to RDD[Row]
     */
    var res: Map[String, Double] = Map() // keep results
    val RUN = 5

    /*
      Query 1
     */

    var t = ProfilingTime.getMeanTime(RUN, Query1.executeCSV(sparkContext, rddCSV))
    res += ("query1csv" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query1.executeOnRow(sparkContext, dataFrameParquet.rdd))
    res += ("query1parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query1.executeOnRow(sparkContext, dataFrameAvro.rdd))
    res += ("query1avro" -> t)

    /*
      Query 1 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameCSV))
    res += ("query1SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameParquet))
    res += ("query1SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameAvro))
    res += ("query1SQLavro" -> t)

    /*
      Query 2
     */

    t = ProfilingTime.getMeanTime(RUN, Query2.executeCSV(sparkContext, rddCSV, calendarManager))
    res += ("query2csv" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query2.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager))
    res += ("query2parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query2.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager))
    res += ("query2avro" -> t)

    /*
      Query 2 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameCSV))
    res += ("query2SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameParquet))
    res += ("query2SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameAvro))
    res += ("query2SQLavro" -> t)

    /*
      Query 3
     */

    t = ProfilingTime.getMeanTime(RUN, Query3.executeCSV(sparkContext, rddCSV, calendarManager))
    res += ("query3csv" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query3.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager))
    res += ("query3parquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, Query3.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager))
    res += ("query3avro" -> t)

    /*
      Query 3 with Spark SQL
     */

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameCSV))
    res += ("query3SQLcsv" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameParquet))
    res += ("query3SQLparquet" -> t)

    t = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameAvro))
    res += ("query3SQLavro" -> t)

    // Write times as JSON file
    val times = sparkSession.read.textFile(gson.toJson(Array(System.currentTimeMillis(), res)))
    times.write.json(outputPath)
  }
}
