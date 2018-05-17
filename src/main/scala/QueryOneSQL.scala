import config.{SmartPlugConfig, Properties}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

/**
  * Find house with a instantaneous energy consumption greater or equal to 350 Watt.
  * Implementation through the Spark SQL library
  */
object QueryOneSQL {

  val spark: SparkSession = SparkSession.builder()
    .appName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
    .master(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
    .getOrCreate()

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

    df.show()

    execute(df)

  }

  /**
    * Execute query creating a DataFrame from a parquet file
    */
  def executeOnParquet(): Unit = {

    val df = spark.read.load("dataset/d14_filtered.parquet")
    //    df.show()
    execute(df)

  }

  private def execute(df: DataFrame): Unit = {

    val res = df.select("house_id")
      .where("property = 1 and value >= 350")
      .distinct()

    spark.time(res.show())

  }

  def main(args: Array[String]): Unit = {
    executeOnCsv()
    executeOnParquet()
  }

}
