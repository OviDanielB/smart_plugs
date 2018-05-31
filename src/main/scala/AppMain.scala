import DAO.hdfsDAO
import Queries._
import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.sql.types.StructType
import utils.CalendarManager

/**
  * @author emanuele 
  */
object AppMain {

  def main(args: Array[String]): Unit = {

    val calendarManager: CalendarManager = new CalendarManager
    val schema: StructType = SparkController.defaultCustomSchema()

    //    val sparkContext = SparkController.sparkContextNoMaster
    var sparkContext = SparkController.defaultSparkContext()
    var sparkSession = SparkController.defaultSparkSession()

    /*
       Default path to dataset and output file
     */
    var datasetPathCSV: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)
    var datasetPathParquet: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)
    var datasetPathAvro: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)
    var deployMode = "local"
    var cacheOrNot = "no_cache"

    if (args.length == 5) {
      datasetPathCSV = args(0)
      datasetPathParquet = args(1)
      datasetPathAvro = args(2)
      deployMode = args(3)
      if(deployMode.equals("cluster")){
        sparkContext = SparkController.sparkContextNoMaster
        sparkSession = SparkController.sparkSessionNoMaster
      }
      cacheOrNot = args(4)
    } else if (args.length != 0) {
      println("Required params: csv path, parquet path, avro path!")
    }

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

    if(cacheOrNot.equals("cache")){
      rddCSV = rddCSV.cache()
      dataFrameParquet = dataFrameParquet.cache()
      dataFrameAvro = dataFrameAvro.cache()
      dataFrameCSV = dataFrameCSV.cache()
    }

    /*
      Spark core queries
      Note: For queries on Parquet and Avro, dataframes are converted to RDD[Row]
     */

    val results1 = Query1.executeCSV(sparkContext, datasetPathCSV)
    Query1.executeOnRow(sparkContext, dataFrameParquet.rdd)
    Query1.executeOnRow(sparkContext, dataFrameAvro.rdd)
    hdfsDAO.writeQuery1Results(sparkSession, results1)

    val results2 = Query2.executeCSV(sparkContext, rddCSV, calendarManager)
    Query2.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager)
    Query2.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager)
    hdfsDAO.writeQuery2Results(sparkSession, results2)

    val results3 = Query3.executeCSV(sparkContext, rddCSV, calendarManager)
    Query3.executeOnRow(sparkContext, dataFrameParquet.rdd, calendarManager)
    Query3.executeOnRow(sparkContext, dataFrameAvro.rdd, calendarManager)
    hdfsDAO.writeQuery3Results(sparkSession, results3)

    /*
      Spark SQL queries
     */

    QueryOneSQL.execute(dataFrameCSV)
    QueryOneSQL.execute(dataFrameParquet)
    QueryOneSQL.execute(dataFrameAvro)

    QueryTwoSQL.execute(dataFrameCSV)
    QueryTwoSQL.execute(dataFrameParquet)
    QueryTwoSQL.execute(dataFrameAvro)

    QueryThreeSQL.execute(dataFrameCSV)
    QueryThreeSQL.execute(dataFrameParquet)
    QueryThreeSQL.execute(dataFrameAvro)

  }
}
