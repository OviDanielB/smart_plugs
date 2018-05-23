import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.udfDataFunction
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._


object QueryThreeSQL {

  val spark: SparkSession = SparkController.defaultSparkSession()

  val schema : StructType = SparkController.defaultCustomSchema()

  import spark.implicits._



  def executeOnCSV(): Unit = {

    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load(SmartPlugConfig.get(Properties.CSV_DATASET_URL))

    execute(df)
  }

  def executeOnParquet(): Unit = {
    val df = spark.read.parquet(SmartPlugConfig.get(Properties.PARQUET_DATASET_URL)).persist()
    execute(df)
  }

  def executeOnAvro(): Unit = {
    val df = spark.read.avro(SmartPlugConfig.get(Properties.AVRO_DATASET_URL)).persist()
    execute(df)
  }


  def execute(df: DataFrame): Unit = {


    val res = df
      // Keep only value for energy consumption
      .where("property = 0")
      // Convert value to DecimalType for avoiding float precision issues
      .withColumn("value", $"value".cast(DataTypes.createDecimalType(20, 5)))
      // For each record compute the referring rate (high/low) per month
      .withColumn("slot", udfDataFunction.getPeriodRateUDF('timestamp))
      // For each record compute the referring day of month
      .withColumn("day", udfDataFunction.getDayOfMonthUDF('timestamp))
      // For each plug, for each rate, for each day computes consumption increase per hour of the day
      .groupBy($"house_id", $"household_id", $"plug_id", udfDataFunction.getHourOfDayUDF('timestamp), $"day", $"slot")
      .agg((max("value") - min("value")).as("plug_consumption_hour"))
      // For each plug, for each rate, for each day computes average consumption increase per day
      .groupBy($"house_id", $"household_id", $"plug_id", $"day", $"slot")
      .agg(avg("plug_consumption_hour").as("plug_consumption_day"))
      // For each plug, for each rate computes average consumption per month
      // (month described by absolute value of slot value)
      .groupBy($"house_id", $"household_id", $"plug_id", $"slot")
      .agg(avg("plug_consumption_day").as("avg"))
      // Inverting sign of average values referring to low slot rate
      .withColumn("avg", udfDataFunction.invertSignUDF('avg, 'slot))
      // For each month, for each slot computes difference between
      // high rate average values and the low rate ones
      .groupBy($"house_id",$"household_id",$"plug_id", abs($"slot").as("month"))
      .agg(sum("avg").as("score"))
      // Ranking sorted by decreasing value of score
      .orderBy(desc("score"))

//    spark.time(res.show(100))
  }

  def main(args: Array[String]): Unit ={
    executeOnCSV()
  }

}
