package controller

import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object SparkController {

  private[this] lazy val sparkContext : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
    conf.setMaster(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
    new SparkContext(conf)
  }

  private[this] lazy val sparkSession : SparkSession = {
    SparkSession
      .builder()
      .appName(SmartPlugConfig.get(Properties.SPARK_APP_NAME))
      .master(SmartPlugConfig.get(Properties.SPARK_MASTER_URL))
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  private[this] lazy val sparkTestContext : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.get(Properties.Test.SPARK_APP_NAME))
    conf.setMaster(SmartPlugConfig.get(Properties.Test.SPARK_MASTER_URL))
    new SparkContext(conf)
  }

  private[this] lazy val customSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("timestamp", LongType, nullable = false),
    StructField("value", FloatType, nullable = false),
    StructField("property", IntegerType, nullable = false),
    StructField("plug_id", LongType, nullable = false),
    StructField("household_id", LongType, nullable = false),
    StructField("house_id", LongType, nullable = false)))

  def defaultSparkContext() : SparkContext = {
    this.sparkContext
  }

  def defaultSparkSession() : SparkSession = {
    this.sparkSession
  }

  def localTestSparkSession() : SparkContext = {
    this.sparkTestContext
  }

  def defaultCustomSchema() : StructType = {
    this.customSchema
  }
}
