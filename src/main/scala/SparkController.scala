import config.{SmartPlugConfig, Properties}
import org.apache.spark.sql.SparkSession
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
      .getOrCreate()
  }


  private[this] lazy val sparkTestContext : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.get(Properties.Test.SPARK_APP_NAME))
    conf.setMaster(SmartPlugConfig.get(Properties.Test.SPARK_MASTER_URL))
    new SparkContext(conf)
  }

  def defaultSparkContext() : SparkContext = {
    this.sparkContext
  }

  def defaultSparkSession() : SparkSession = {
    this.sparkSession
  }

  def localTestSparkSession() : SparkContext = {
    this.sparkTestContext
  }
}
