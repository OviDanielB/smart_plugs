import config.SmartPlugConfig
import model.PlugData
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeZone}
import utils.CSVParser

object Dataset {

  type ValueCount = (Float, Int)


  def datasetEntries(dataSetData: RDD[PlugData]) : Unit = {
    val dataSetRowsNumber =
      dataSetData
        .map(_ => 1)
        .reduce(_ + _)

    printf("Dataset has %d entries\n", dataSetRowsNumber)
  }

  def datasetHouses(dataSetData: RDD[PlugData]) : Unit = {
    val numHouses = dataSetData
      .map(d => (d.house_id, null))
      .groupByKey()
      .count()

    printf("Number of houses : %d \n", numHouses)
  }

  def datasetHouseHolds(dataSetData: RDD[PlugData]) : Unit = {
    val numHouseHolds = dataSetData
      .map(d => (d.house_id, d.household_id))
      .distinct()
      .count()


    printf("Number of households : %d \n", numHouseHolds)
  }

  def datasetPlugs(dataSetData: RDD[PlugData]): Unit = {
    val numPlugs = dataSetData
      .map(d => (d.house_id, d.household_id, d.plug_id))
      .distinct()
      .count()


    printf("Number of plugs : %d \n", numPlugs)
  }

  def datasetPlugsAveragePowerConsumption(dataSetData: RDD[PlugData]) : Unit = {

    val plug_average_power =
      dataSetData
      .filter(d => d.isWorkMeasurement())
      .map(d => ((d.house_id, d.household_id,d.plug_id), (BigDecimal.decimal(d.value), 1) ))
      .reduceByKey{
        case ((value1, count1), (value2, count2)) =>
          (value1 + value2, count1 + count2)
      }
      .map(d => (d._1, d._2._1 / d._2._2))
      .sortByKey()
      .collect()

    for( avg <- plug_average_power){
      println(avg)
    }
  }


  def computeMean(prevTuple: ValueCount, currTuple: ValueCount) : ValueCount = {

    (prevTuple._1 + (currTuple._1 - prevTuple._1)/prevTuple._2 + currTuple._2, prevTuple._2 + currTuple._2)

  }

  def datasetHouseAveragePowerConsumption(dataSetData: RDD[PlugData]): Unit = {
    val house_average_power =
      dataSetData
      .filter(d => d.isWorkMeasurement())
      .map(d => (d.house_id, (d.value, 1)) )
        .reduceByKey{
        case ((value1, count1), (value2, count2)) =>
          (value1 + value2, count1 + count2)
      }
        .map(d => (d._1, d._2._1 / d._2._2))
      .sortByKey()
      .collect()

    for( avg <- house_average_power){
      println(avg)
    }

  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf()
      .setAppName(SmartPlugConfig.SPARK_DATASET_APP_NAME)
      .setMaster(SmartPlugConfig.SPARK_MASTER_URL)

    val sc = new SparkContext(sparkConf)


    println(sc.startTime)
    println(new DateTime(1377986420 * 1000L).toDateTime(DateTimeZone.forID("Europe/Berlin")))

    val dataSetData = sc.textFile(SmartPlugConfig.DATASET_URL)
      .map(line => CSVParser.parse(line).get).cache()

    datasetEntries(dataSetData)
    datasetHouses(dataSetData)
    datasetHouseHolds(dataSetData)
    datasetPlugs(dataSetData)
    datasetPlugsAveragePowerConsumption(dataSetData)
    datasetHouseAveragePowerConsumption(dataSetData)


    sc.stop()

  }

}
