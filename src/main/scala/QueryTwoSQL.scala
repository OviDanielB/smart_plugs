import com.databricks.spark.avro._
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.{ProfilingTime, udfDataFunction}


/**
  * QUERY 2 USING SPARK SQL
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object QueryTwoSQL {

  val spark : SparkSession = SparkController.defaultSparkSession()

  val customSchema : StructType = SparkController.defaultCustomSchema()

  import spark.implicits._


  /**
    * Load CSV file as a dataframe.
    *
    * @return DataFrame
    */
  private def loadDataframeFromCSV() : DataFrame = {
    spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(customSchema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))
      .persist()
  }


  /**
    * Compute mean and standard deviation statistics of every house energy consumption
    * during each time slot in [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59].
    * Single value of energy consumption is computed as the difference between the value
    * of the last record of the period and the first one, because it is a cumulative quantity.
    * It does NOT keep into account errors obtained for plugs that have been reset into a period
    *
    * @param df DataFrame
    */
  def executeOnSlot(df: DataFrame): Unit = {

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
      .collect()
  }

  def executeOnCsv(): Unit = {
    val df = loadDataframeFromCSV()
    executeOnSlot(df)
  }

  def executeOnParquet(): Unit = {
    val df = spark.read.parquet(SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)).persist()
    executeOnSlot(df)
  }

  def executeOnAvro(): Unit = {
    val df = spark.read.avro(SmartPlugConfig.get(Properties.AVRO_DATASET_URL)).persist()
    executeOnSlot(df)
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
