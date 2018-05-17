package config

import com.typesafe.config.{Config, ConfigFactory}

object SmartPlugConfig {

  private[this] val config : Config = {
    ConfigFactory.load()
  }

  def get(property : String) : String = {
    config.getString(property)
  }
}



object Properties {

  val CSV_DATASET_URL = "dataset.csv.url"
  val PARQUET_DATASET_URL = "dataset.parquet.url"
  val AVRO_DATASET_URL = "dataset.avro.url"

  val SPARK_APP_NAME = "spark.app.name"
  val SPARK_MASTER_URL = "spark.master.url"

  val SPARK_QUERY_ONE_NAME = "spark.query.one.name"
  val SPARK_QUERY_TWO_NAME = "spark.query.two.name"
  val SPARK_QUERY_THREE_NAME = "spark.query.three.name"

  object Test {
    private val TEST_PREFIX = "test."

    val SPARK_APP_NAME = TEST_PREFIX + Properties.SPARK_APP_NAME
    val SPARK_MASTER_URL = TEST_PREFIX + Properties.SPARK_MASTER_URL
    val CSV_DATASET_URL = TEST_PREFIX + Properties.CSV_DATASET_URL
    val PARQUET_DATASET_URL = TEST_PREFIX + Properties.PARQUET_DATASET_URL
    val AVRO_DATASET_URL = TEST_PREFIX + Properties.AVRO_DATASET_URL
  }
}
