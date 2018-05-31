import DAO.hdfsDAO
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
    var sc = SparkController.defaultSparkContext()
    var spark = SparkController.defaultSparkSession()

    /*
       Default path to dataset and output file
     */
    var outputPath = "dataset/times.csv"
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
      if(deployMode.equals("cluster")){
        sc = SparkController.sparkContextNoMaster
        spark = SparkController.sparkSessionNoMaster
      }
      cacheOrNot = args(5)
    } else if (args.length != 0) {
      println("Required params: csv path, parquet path, avro path!")
    }

    /*
       Get RDD[String] from csv file
     */
    var data = sc.textFile(datasetPathCSV)

    /*
      Get dataframes from Parquet and Avro files
     */
    var data_p = spark.read.parquet(datasetPathParquet)
    var data_a = spark.read.avro(datasetPathAvro)
    var df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    if(cacheOrNot.equals("cache")){
      data = data.cache()
      data_p = data_p.cache()
      data_a = data_a.cache()
      df = df.cache()
    }

    /*
      Spark core queries
      Note: For queries on Parquet and Avro, dataframes are converted to RDD[Row]
     */
    hdfsDAO.writeQuery1Results(Query1.executeCSV(sc, datasetPathCSV))
    Query1.executeOnRow(sc, data_p.rdd)
    Query1.executeOnRow(sc, data_a.rdd)

    hdfsDAO.writeQuery2Results(Query2.executeCSV(sc, data, calendarManager))
    Query2.executeOnRow(sc, data_p.rdd, calendarManager)
    Query2.executeOnRow(sc, data_a.rdd, calendarManager)

    hdfsDAO.writeQuery3Results(Query3.executeCSV(sc, data, calendarManager))
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
