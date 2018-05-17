import config.{SmartPlugConfig, SmartPlugProperties}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkController {

  private[this] val sparkContext : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.get(SmartPlugProperties.SPARK_APP_NAME))
    conf.setMaster(SmartPlugConfig.get(SmartPlugProperties.SPARK_MASTER_URL))
    new SparkContext(conf)
  }

  private[this] val sparkSession : SparkSession = {
    SparkSession
      .builder()
      .appName(SmartPlugConfig.get(SmartPlugProperties.SPARK_APP_NAME))
      .getOrCreate()
  }



  def defaultSparkContext() : SparkContext = {
    this.sparkContext
  }

  def defaultSparkSession() : SparkSession = {
    this.sparkSession
  }
}
