package DAO

import com.google.gson.Gson
import config.{Properties, SmartPlugConfig}
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object hdfsDAO {

  private val gson : Gson = new Gson()

  def writeQuery1Results(sparkSession : SparkSession, res : Array[Int]) : Unit = {

    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_1_URL))
  }

  def writeQuery1SQLResults(sparkSession : SparkSession, res: Dataset[Row]) : Unit = {

    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_SQL_1_URL))
  }

  def writeQuery2Results(sparkSession : SparkSession, res: Array[((Int,Int),Double,Double)]) : Unit = {

    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_2_URL))
  }

  def writeQuery2SQLResults(sparkSession : SparkSession, res: Dataset[Row]) : Unit = {
    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_SQL_2_URL))
  }

  def writeQuery3Results(sparkSession: SparkSession, res: Array[((Int,Int,Int,Int),Double)]) : Unit = {
    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_3_URL))
  }

  def writeQuery3SQLResults(sparkSession: SparkSession, res: Dataset[Row]) : Unit = {
    val tmp = Array(System.currentTimeMillis(), res)

    val results = gson.toJson(tmp)

    val df = sparkSession.read.textFile(results)

    df.coalesce(1).write.json(SmartPlugConfig.get(Properties.JSON_RESULTS_SQL_3_URL))
  }
}
