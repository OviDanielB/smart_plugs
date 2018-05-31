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

  val CSV_DATASET_URL_FILTERED = "filteredDataset.csv.url"
  val PARQUET_DATASET_URL_FILTERED = "filteredDataset.parquet.url"
  val AVRO_DATASET_URL_FILTERED = "filteredDataset.avro.url"

  val SPARK_APP_NAME = "sp.spark.app.name"
  val SPARK_MASTER_URL = "sp.spark.mastername.url"

  val SPARK_QUERY_ONE_NAME = "sp.spark.query.one.name"
  val SPARK_QUERY_TWO_NAME = "sp.spark.query.two.name"
  val SPARK_QUERY_THREE_NAME = "sp.spark.query.three.name"

  val JSON_TIMES_URL = "results.times.url"
  val JSON_RESULTS_1_URL = "results.times.res.query1.url"
  val JSON_RESULTS_SQL_1_URL = "results.times.res.query1SQL.url"
  val JSON_RESULTS_2_URL = "results.times.res.query2.url"
  val JSON_RESULTS_SQL_2_URL = "results.times.res.query2SQL.url"
  val JSON_RESULTS_3_URL = "results.times.res.query3.url"
  val JSON_RESULTS_SQL_3_URL = "results.times.res.query3SQL.url"

  object Test {
    private val TEST_PREFIX = "test."

    val SPARK_APP_NAME = TEST_PREFIX + Properties.SPARK_APP_NAME
    val SPARK_MASTER_URL = TEST_PREFIX + Properties.SPARK_MASTER_URL
    val CSV_DATASET_URL = TEST_PREFIX + Properties.CSV_DATASET_URL
    val PARQUET_DATASET_URL = TEST_PREFIX + Properties.PARQUET_DATASET_URL
    val AVRO_DATASET_URL = TEST_PREFIX + Properties.AVRO_DATASET_URL
  }
}
