import config.{SmartPlugConfig, Properties}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._


/**
  * Find house with a instantaneous energy consumption greater or equal to 350 Watt.
  * Implementation through the Spark SQL library
  */
object QueryOneSQL {

  val spark: SparkSession = SparkController.defaultSparkSession()

  import spark.implicits._

  // Create schema
  val customSchema : StructType = SparkController.defaultCustomSchema()

  /**
    * Execute query creating a DataFrame from a csv file
    */
  def executeOnCsv(): Unit = {

    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    execute(df)

  }

  /**
    * Execute query creating a DataFrame from a parquet file
    */
  def executeOnParquet(): Unit = {

    val df = spark.read.load(SmartPlugConfig.get(Properties.PARQUET_DATASET_URL))
    execute(df)

  }

  /**
    * Execute query creating a DataFrame from a avro file
    */
  def executeOnAvro(): Unit = {

    val df = spark.read.avro(SmartPlugConfig.get(Properties.AVRO_DATASET_URL))
    execute(df)

  }

  def execute(df: DataFrame): Unit = {

    val res = df
      .where("property = 1")
      .groupBy("house_id", "timestamp")
      .agg(sum("value").as("sum"))
      .select("house_id")
      .where("sum >= 350")
      .distinct()
      .sort($"house_id")


    spark.time(res.show(100))

  }

  def main(args: Array[String]): Unit = {
    executeOnCsv()
        executeOnParquet()
  }

}
