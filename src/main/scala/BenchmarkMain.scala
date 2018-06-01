import Queries._
import com.google.gson.Gson
import controller.SparkController
import org.apache.spark.sql.types.StructType
import utils.{CalendarManager, JSONConverter, ProfilingTime}
import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import utils.JSONConverter.Times

/**
  * The Main for queries execution times measurements
  * that requires the following input parameters:
  *   <csv file> :path
  *   <parquet file> : path
  *   <avro file> : path
  *   <deploymode> [local/cluster] : to choose running mode
  *   <cacheOrNot> [cache/no_cache] : to include/exclude RDD caching Action
  *   <runString> [1..] : to choose the number of runs among which average times
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object BenchmarkMain {

  def main(args: Array[String]): Unit = {

    val calendarManager: CalendarManager = new CalendarManager
    val schema: StructType = SparkController.defaultCustomSchema()

    //    val sparkContext = SparkController.sparkContextNoMaster
    var sparkContext = SparkController.defaultSparkContext()
    var sparkSession = SparkController.defaultSparkSession()

//    var sparkContext = SparkController.sparkContextNoMaster
//    var sparkSession = SparkController.sparkSessionNoMaster

    /*
       Default path to dataset and output file
     */
    var outputPath = SmartPlugConfig.get(Properties.JSON_TIMES_URL)
    var datasetPathCSV: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)
    var datasetPathParquet: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)
    var datasetPathAvro: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)
    var deployMode = "local"
    var cacheOrNot = "no_cache"
    var runString = "1"

    if (args.length == 7) {
      outputPath = args(0)
      datasetPathCSV = args(1)
      datasetPathParquet = args(2)
      datasetPathAvro = args(3)
      deployMode = args(4)
      if (deployMode.equals("cluster")) {
        sparkContext = SparkController.sparkContextNoMaster
        sparkSession = SparkController.sparkSessionNoMaster

        sparkContext.setLogLevel("INFO")
        sparkSession.sparkContext.setLogLevel("INFO")

      }
      cacheOrNot = args(5)
      runString = args(6)
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
      .load(datasetPathCSV)

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
    val RUN = runString.toInt

    /*
      Query 1
     */

    println("Running t1csv")
    val t1csv = ProfilingTime.getMeanTime(RUN, Query1.executeCSV(sparkContext, rddCSV))

    println("Running t1parquet")
    val t1parquet = ProfilingTime.getMeanTime(RUN, Query1.executeOnRow(sparkContext, dataFrameParquet.rdd))

    println("Running t1avro")
    val t1avro = ProfilingTime.getMeanTime(RUN, Query1.executeOnRow(sparkContext, dataFrameAvro.rdd))

    /*
      Query 1 with Spark SQL
     */

    println("Running t1SQLcsv")
    val t1SQLcsv = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameCSV))

    println("Running t1SQLparquet")
    val t1SQLparquet = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameParquet))

    println("Running t1SQLavro")
    val t1SQLavro = ProfilingTime.getMeanTime(RUN, QueryOneSQL.execute(dataFrameAvro))

    /*
      Query 2
     */

    println("Running t2csv")
    val t2csv = ProfilingTime.getMeanTime(RUN, Query2.executeCSV(sparkContext, rddCSV, calendarManager))

    println("Running t2parquet")
    val t2parquet = ProfilingTime.getMeanTime(RUN, Query2.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager))

    println("Running t2avro")
    val t2avro = ProfilingTime.getMeanTime(RUN, Query2.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager))

    /*
      Query 2 with Spark SQL
     */

    println("Running t2SQLcsv")
    val t2SQLcsv = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameCSV))

    println("Running t2SQLparquet")
    val t2SQLparquet = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameParquet))

    println("Running t2SQLavro")
    val t2SQLavro = ProfilingTime.getMeanTime(RUN, QueryTwoSQL.execute(dataFrameAvro))

    /*
      Query 3
     */

    println("Running t3csv")
    val t3csv = ProfilingTime.getMeanTime(RUN, Query3.executeCSV(sparkContext, rddCSV, calendarManager))

    println("Running t3parquet")
    val t3parquet = ProfilingTime.getMeanTime(RUN, Query3.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager))

    println("Running t3avro")
    val t3avro = ProfilingTime.getMeanTime(RUN, Query3.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager))

    /*
      Query 3 with Spark SQL
     */

    println("Running t3SQLcsv")
    val t3SQLcsv = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameCSV))

    println("Running t3SQLparquet")
    val t3SQLparquet = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameParquet))

    println("Running t3SQLavro")
    val t3SQLavro = ProfilingTime.getMeanTime(RUN, QueryThreeSQL.execute(dataFrameAvro))


    val res = new Times(
      System.currentTimeMillis(),
      t1csv, t1parquet, t1avro,
      t2csv, t2parquet, t2avro,
      t3csv, t3parquet, t3avro,
      t1SQLcsv, t1SQLparquet, t1SQLavro,
      t2SQLcsv, t2SQLparquet, t2SQLavro,
      t3SQLcsv, t3SQLparquet, t3SQLavro)

    // Write times as JSON file
    sparkContext.parallelize(Seq(JSONConverter.timesToJson(res)))
      .saveAsTextFile(outputPath)
  }
}
