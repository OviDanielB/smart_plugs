import config.SmartPlugConfig
import model.PlugData
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import utils.CSVParser

object Dataset {


  def datasetEntries(dataSetData: RDD[PlugData]) = {
    val dataSetRowsNumber =
      dataSetData
        .map(_ => 1)
        .reduce(_ + _)

    printf("Dataset has %d entries\n", dataSetRowsNumber)
  }

  def datasetHouses(dataSetData: RDD[PlugData]) = {
    val numHouses = dataSetData
      .map(d => (d.house_id, null))
      .groupByKey()
      .count()

    printf("Number of houses : %d \n", numHouses)
  }

  def datasetHouseHolds(dataSetData: RDD[PlugData]) = {
    val numHouseHolds = dataSetData
      .map(d => (d.house_id, d.household_id))
      .distinct()
      .count()


    printf("Number of households : %d \n", numHouseHolds)
  }

  def datasetPlugs(dataSetData: RDD[PlugData]) = {
    val numPlugs = dataSetData
      .map(d => (d.house_id, d.household_id, d.plug_id))
      .distinct()
      .count()


    printf("Number of plugs : %d \n", numPlugs)
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf()
      .setAppName(SmartPlugConfig.SPARK_DATASET_APP_NAME)
      .setMaster(SmartPlugConfig.SPARK_MASTER_URL)
    val sc = new SparkContext(sparkConf)

    val dataSetData = sc.textFile(SmartPlugConfig.DATASET_URL)
      .map(line => CSVParser.parse(line).get).cache()


    datasetEntries(dataSetData)
    datasetHouses(dataSetData)
    datasetHouseHolds(dataSetData)
    datasetPlugs(dataSetData)
    sc.stop()

  }

}
