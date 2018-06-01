package DAO

import Queries.QueryOneSQL.spark
import Queries.{Query1, Query2, Query3}
import alluxio.AlluxioURI
import alluxio.client.file.{FileOutStream, FileSystem}
import config.{Properties, SmartPlugConfig}
import controller.SparkController
import org.apache.spark.sql.SparkSession
import utils.{CalendarManager, JSONConverter}


object hdfsDAO {

  import spark.implicits._


  def writeQuery1Results(sparkSession : SparkSession, res : Array[Int]) : Unit = {

    val results = JSONConverter.results1ToJSON(res)

    sparkSession.sparkContext.parallelize(Seq(results))
      .saveAsTextFile(SmartPlugConfig.get(Properties.JSON_RESULTS_1_URL))
    //    writeOnAlluxio(results, "alluxio://localhost:19998/results/results1.json")
  }

  def writeQuery2Results(sparkSession : SparkSession, res: Array[((Int,Int),Double,Double)]) : Unit = {

    val results = JSONConverter.results2ToJSON(res)

    sparkSession.sparkContext.parallelize(Seq(results))
      .saveAsTextFile(SmartPlugConfig.get(Properties.JSON_RESULTS_2_URL))
  }

  def writeQuery3Results(sparkSession: SparkSession, res: Array[((Int,Int,Int,Int),Double)]) : Unit = {

    val results = JSONConverter.results3ToJSON(res)

    sparkSession.sparkContext.parallelize(Seq(results))
      .saveAsTextFile(SmartPlugConfig.get(Properties.JSON_RESULTS_3_URL))
  }


  def writeOnAlluxio(r: String, dest: String): Unit = {

    val fs : FileSystem = FileSystem.Factory.get

    val path : AlluxioURI = new AlluxioURI(dest)

    val out : FileOutStream = fs.createFile(path)

    out.write(r.toByte)

    out.close()
  }

  def main(args: Array[String]): Unit = {

    val sc = SparkController.defaultSparkContext()
    val res1 = Query1.executeCSV(sc,"dataset/filtered/d14_filtered.csv")
    writeQuery1Results(SparkController.defaultSparkSession(),res1)
    val res2 = Query2.executeCSV(sc,"dataset/filtered/d14_filtered.csv", new CalendarManager)
    writeQuery2Results(SparkController.defaultSparkSession(),res2)
    val res3 = Query3.executeCSV(sc,"dataset/filtered/d14_filtered.csv", new CalendarManager)
    writeQuery3Results(SparkController.defaultSparkSession(),res3)
  }
}
