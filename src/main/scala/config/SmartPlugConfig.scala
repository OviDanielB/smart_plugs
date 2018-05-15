package config

import com.typesafe.config.{Config, ConfigFactory}

object SmartPlugConfig {

  object Properties {
    val URL = "url"
  }

  val DATASET_URL = "dataset/d14_filtered.csv"
  val SPARK_APP_NAME = "smartPlug"
  val SPARK_DATASET_APP_NAME = "smartPlug"
  val SPARK_MASTER_URL = "local[2]"

  private[this] val config : Config = {
    ConfigFactory.load()

  }

  println(config.getString("dataset.parquet.filename"))
}
