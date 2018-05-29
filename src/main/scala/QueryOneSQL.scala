import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import controller.SparkController
import utils.ProfilingTime

/**
  * QUERY 1 USING SPARK SQL
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object QueryOneSQL {

  val spark: SparkSession = SparkController.defaultSparkSession()

  import spark.implicits._

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

  /**
    * Find houses with a instantaneous energy consumption greater or equal to 350 Watt.
    *
    * @param df DataFrame
    */
  def execute(df: DataFrame): Unit = {

    val res = df
      .where("property = 1")
      .groupBy("house_id", "timestamp")
      .agg(sum("value").as("sum"))
      .select("house_id")
      .where("sum >= 350")
      .distinct()
      .sort($"house_id")
      .collect()

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
