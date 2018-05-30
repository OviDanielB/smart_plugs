package DAO

import controller.SparkController
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import utils.CalendarManager

object hdfsDAO {

  private val RESULTS_1_FILENAME = "results/results1.parquet"
  private val RESULTS_2_FILENAME = "results/results2.parquet"
  private val RESULTS_3_FILENAME = "results/results3.parquet"

  private val schema_1 : StructType = StructType(Array(
    StructField("house_id", IntegerType, nullable = false)))

  private val schema_2 : StructType = StructType(Array(
    StructField("house_id", IntegerType, nullable = false),
    StructField("slot", IntegerType, nullable = false),
    StructField("avg", DoubleType, nullable = false),
    StructField("stddev", DoubleType, nullable = false)
  ))

  private val schema_3 : StructType = StructType(Array(
    StructField("house_id", IntegerType, nullable = false),
    StructField("household_id", IntegerType, nullable = false),
    StructField("plug_id", IntegerType, nullable = false),
    StructField("month", IntegerType, nullable = false),
    StructField("score", DoubleType, nullable = false)
  ))

  val sparkContext : SparkContext = SparkController.defaultSparkContext()
  val sparkSession : SparkSession = SparkController.defaultSparkSession()

  def writeQuery1Results(res: Array[Int]) : Unit = {

    val results = res.map(r => Row(r))

    val rdd = sparkContext.parallelize(results)
    val df = sparkSession.createDataFrame(rdd, schema_1)

    df.coalesce(1).write.parquet(RESULTS_1_FILENAME)
  }

  def writeQuery1Results(res: Dataset[Row]) : Unit = {

  }

  def writeQuery2Results(res: Array[((Int,Int),Double,Double)]) : Unit = {

    val results = res.map(r => Row(r._1._1, r._1._2, r._2, r._3))

    val rdd = sparkContext.parallelize(results)
    val df = sparkSession.createDataFrame(rdd, schema_2)

    df.coalesce(1).write.parquet(RESULTS_2_FILENAME)

    sparkSession.read.parquet(RESULTS_2_FILENAME).show()
  }

  def writeQuery2Results(res: Dataset[Row]) : Unit = {

  }

  def writeQuery3Results(res: Array[((Int,Int,Int,Int),Double)]) : Unit = {
    val results = res.map(r => Row(r._1._1, r._1._2, r._1._3, r._1._4, r._2))

    val rdd = sparkContext.parallelize(results)
    val df = sparkSession.createDataFrame(rdd, schema_3)

    df.coalesce(1).write.parquet(RESULTS_3_FILENAME)

//    sparkSession.read.parquet(RESULTS_3_FILENAME).show()
  }

  def writeQuery3Results(res: Dataset[Row]) : Unit = {

  }

  def main(args: Array[String]) : Unit = {
    val cm = new CalendarManager()
//    val res1 = Query1.executeCSV(sparkContext, "dataset/d14_filtered.csv")
//    writeQuery1Results(res1)
//    val res2 = Query2.executeCSV(sparkContext, "dataset/d14_filtered.csv", cm)
//    writeQuery2Results(res2)
//    val res3 = Query3.executeCSV(sparkContext, "dataset/d14_filtered.csv", cm)
//    writeQuery3Results(res3)

//    sparkSession.read.parquet(RESULTS_3_FILENAME).show(100)

    val rdd2 = sparkSession.read.textFile("hdfs://master:54310/dataset/d14_filtered.csv")
    rdd2.toDF().show()

  }

}
