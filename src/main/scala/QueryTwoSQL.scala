import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.ProfilingTime


/**
  * @author emanuele 
  */
object QueryTwoSQL {

  val spark: SparkSession = SparkSession.builder()
    .appName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
    .master(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  import spark.implicits._


  def executeOnCsv(): Unit = {
    // Create schema
    val customSchema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("value", FloatType, nullable = false),
      StructField("property", IntegerType, nullable = false),
      StructField("plug_id", LongType, nullable = false),
      StructField("household_id", LongType, nullable = false),
      StructField("house_id", LongType, nullable = false)))

    // Load DataFrame from parquet file
    //  val df: DataFrame = spark.read.load("dataset/d14_filtered.parquet")
    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load("dataset/d14_filtered.csv").persist()

    executeOnSlot(df)
  }

  def executeOnParquet(): Unit = {
    val df = spark.read.parquet("dataset/d14_filtered.parquet").persist()
    executeOnSlot(df)
  }

  def executeOnAvro(): Unit = {
    val df = spark.read.avro("dataset/d14_filtered.avro").persist()
    executeOnSlot(df)
  }

  /**
    * Compute energy consumption as the difference between an event and the previous.
    *
    * @param df DataFrame
    */
  private def executeOnEvent(df: DataFrame): Unit = {

    /*
      Use window functions to find given an event the previous.
      Useful to find the difference between the value of an event and the previous one to compute consumption
      https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
     */
    val windowSpec = Window.partitionBy("house_id", "plug_id")
      .orderBy("timestamp")

    val data = df
      .where("property == 0 AND value <> 0")
      .withColumn("timestamp", to_utc_timestamp(from_unixtime($"timestamp"), "Etc/GMT+2"))
      // Convert value to DecimalType for avoiding float precision issues
      .withColumn("value", $"value".cast(DataTypes.createDecimalType(10, 3)))

      // For each plug compute the difference between the current value and the previous.
      .withColumn("plug_consumption", when(lag($"value", 1, -1).over(windowSpec) === -1, 0)
      //      .when($"value".leq(lag("value", 1, 0).over(windowSpec)), 0)
      .otherwise($"value" - lag("value", 1, 0).over(windowSpec))
    )

      // For each plug compute the consumption on each time slot for each day
      // out schema:|house_id|household_id|plug_id|window|tot_cons|
      .groupBy($"house_id", window($"timestamp", "6 hours"))
      .agg(sum("plug_consumption").alias("slot_consumption"))

      // window: [window.start: TimestampType, window.end: TimestampType]
      // Reformat window to remove year,month and day. Then we can compute statistics with a groupBy on the same time slot
      .withColumn("window", struct(date_format($"window.start", "HH:mm"), date_format($"window.end", "HH:mm")))

      // Aggregate for each house and time slot
      .groupBy("house_id", "window")
      .agg(avg("slot_consumption").as("avg"), stddev("slot_consumption").as("stddev"))

      // Format DataFrame
      .orderBy("house_id", "window")
      .select("*")
      .collect()

  }

  /**
    * Compute energy consumption in a period as the difference between the value of the last record of the period
    * and the first. It does NOT keep into account errors obtained for plugs that have been reset into a period
    *
    * @param df DataFrame
    */
  private def executeOnSlot(df: DataFrame): Unit = {

    val data = df
      // Keep only value for energy consumption
      .where("property == 0")
      // Transform unix timestamp to TimestampType
      .withColumn("timestamp", to_utc_timestamp(from_unixtime($"timestamp"), "Etc/GMT+2"))
      // Convert value to DecimalType for avoiding float precision issues
      .withColumn("value", $"value".cast(DataTypes.createDecimalType(20, 5)))

      // For each plug and for each time slot, compute difference between the first value in the time slot and the last.
      // The result is the consumption for a plug in a given time slot
      .groupBy($"house_id", $"household_id", $"plug_id", window($"timestamp", "6 hours"))
      .agg(
        when(last("value") >= first("value"), last("value") - first("value"))
          .otherwise(last("value"))
          .alias("plug_consumption")
      )

      // The sum of the energy consumption of each plug of a given house is the consumption for the house
      .groupBy("house_id", "window")
      .agg(sum($"plug_consumption").as("home_consumption"))

      // Format window to remove year,month and day.
      .withColumn("window", struct(date_format($"window.start", "HH:mm"), date_format($"window.end", "HH:mm")))
      //Then we can compute statistics into each time slot over all the days for each house
      .groupBy($"house_id", $"window")
      .agg(avg("home_consumption").as("avg"), stddev("home_consumption").as("stddev"))

      .orderBy("house_id", "window")
      .select("*")

    data.collect()
  }


  def main(args: Array[String]): Unit = {
    ProfilingTime.time {
      executeOnCsv()
    }

    ProfilingTime.time {
      executeOnParquet()
    }

    ProfilingTime.time {
      executeOnAvro()
    }
  }

}
