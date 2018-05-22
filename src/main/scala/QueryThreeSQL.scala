import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CalendarManager
import org.apache.spark.sql.functions._


object QueryThreeSQL {

  val spark: SparkSession = SparkSession.builder()
    .appName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
    .master(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
  val schema : StructType = SparkController.defaultCustomSchema()

  import spark.implicits._



  def executeCSV(): Unit = {

    val df = spark.read.format("csv")
      .option("header", "false")
      .option("delimiter", ",")
      .schema(schema)
      .load("dataset/d14_filtered.csv")

    execute(df)
  }

  def executeOnParquet(): Unit = {

    val df = spark.read.load("dataset/d14_filtered.parquet")
    execute(df)
  }


  def execute(df: DataFrame): Unit = {

    spark.udf.register("customAverage", CustomAverage)


    val cm = new CalendarManager

    val getSlot: Long => Int = {
      cm.getPeriodRate
    }

    val getDay: Long => Int = {
      cm.getDayAndMonth(_)(0)
    }



    val getSlotUDF = udf(getSlot)
    val getDayUDF = udf(getDay)


    val res = df
      .where("property = 0")
      .withColumn("slot", getSlotUDF('timestamp))
      .groupBy($"house_id",$"plug_id", getDayUDF('timestamp), $"slot")
      .agg((max("value") - min("value")).as("diff"))
      .groupBy($"house_id",$"plug_id", $"slot")
      .agg(avg("diff").as("avg"))

    spark.time(res.show(100))
  }

  def main(args: Array[String]): Unit ={
    executeCSV()
  }

}
