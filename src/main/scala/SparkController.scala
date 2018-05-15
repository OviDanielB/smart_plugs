import config.SmartPlugConfig
import org.apache.spark.{SparkConf, SparkContext}

object SparkController {

  private[this] val sparkContext : SparkContext = {
    val conf = new SparkConf()
    conf.setAppName(SmartPlugConfig.SPARK_APP_NAME)
    conf.setMaster(SmartPlugConfig.SPARK_MASTER_URL)
    new SparkContext(conf)

  }


  def defaultSparkContext() : SparkContext = {
    this.sparkContext
  }
}
