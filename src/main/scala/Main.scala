import QueryOneSQL.{customSchema, spark}
import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.sql.types.StructType
import utils.CalendarManager

/**
  * @author emanuele 
  */
object Main {

  def main(args: Array[String]): Unit = {

    val calendarManager: CalendarManager = new CalendarManager
    val schema: StructType = SparkController.defaultCustomSchema()

    //    val sc = SparkController.sparkContextNoMaster
    val sc = SparkController.defaultSparkContext()
    val spark = SparkController.defaultSparkSession()

    /*
       Default path to dataset and output file
     */
    var outputPath = "dataset/times.csv"
    var datasetPathCSV: String = SmartPlugConfig.get(Properties.CSV_DATASET_URL)
    var datasetPathParquet: String = SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)
    var datasetPathAvro: String = SmartPlugConfig.get(Properties.AVRO_DATASET_URL)

    if (args.length == 4) {
      outputPath = args(0)
      datasetPathCSV = args(1)
      datasetPathParquet = args(2)
      datasetPathAvro = args(3)
    } else if (args.length != 0) {
      println("Required params: csv path, parquet path, avro path!")
    }

    /*
       Get RDD[String] from csv file
     */
    val data = sc.textFile(datasetPathCSV)

    /*
      Get dataframes from Parquet and Avro files
     */
    val data_p = spark.read.parquet(datasetPathParquet)
    val data_a = spark.read.avro(datasetPathAvro)
    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    /*
      Spark core queries
      Note: For queries on Parquet and Avro, dataframes are converted to RDD[Row]
     */
    Query1.executeCSV(sc, datasetPathCSV)
    Query1.executeOnRow(sc, data_p.rdd)
    Query1.executeOnRow(sc, data_a.rdd)

    Query2.executeCSV(sc, data, calendarManager)
    Query2.executeOnRow(sc, data_p.rdd, calendarManager)
    Query2.executeOnRow(sc, data_a.rdd, calendarManager)

    Query3.executeCSV(sc, data, calendarManager)
    Query3.executeOnRow(sc, data_p.rdd, calendarManager)
    Query3.executeOnRow(sc, data_a.rdd, calendarManager)

    /*
      Spark SQL queris
     */

    QueryOneSQL.execute(df)
    QueryOneSQL.execute(data_p)
    QueryOneSQL.execute(data_a)

    QueryTwoSQL.execute(df)
    QueryTwoSQL.execute(data_p)
    QueryTwoSQL.execute(data_a)

    QueryThreeSQL.execute(df)
    QueryThreeSQL.execute(data_p)
    QueryThreeSQL.execute(data_a)

  }

}
