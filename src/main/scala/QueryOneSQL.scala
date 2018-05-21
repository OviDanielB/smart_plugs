import config.{SmartPlugConfig, Properties}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
  * Find house with a instantaneous energy consumption greater or equal to 350 Watt.
  * Implementation through the Spark SQL library
  */
object QueryOneSQL {

  val spark: SparkSession = SparkSession.builder()
    .appName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
    .master(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
    .getOrCreate()

  import spark.implicits._


  // Create schema
  val customSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("value", FloatType, nullable = false),
    StructField("property", IntegerType, nullable = false),
    StructField("plug_id", LongType, nullable = false),
    StructField("household_id", LongType, nullable = false),
    StructField("house_id", LongType, nullable = false)))

  /**
    * Execute query creating a DataFrame from a csv file
    */
  def executeOnCsv(): Unit = {

    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load("dataset/d14_filtered.csv")


    execute(df)

  }

  /**
    * Execute query creating a DataFrame from a parquet file
    */
  def executeOnParquet(): Unit = {

    val df = spark.read.load("dataset/d14_filtered.parquet")
    execute(df)

  }

  private def execute(df: DataFrame): Unit = {

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
