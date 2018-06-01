package utils

import com.google.gson.Gson

object JSONConverter {

  private val gson : Gson = new Gson()

  class Result1(t: Long, res: Array[Int]) {
    val timestamp: Long = t
    val house_list: Array[Int] = res
  }

  class Statistic(house: Int, slot: Int, avg: Double, devstd: Double) {
    val house_id : Int = house
    val time_slot: Int = slot
    val mean : Double = avg
    val std : Double = devstd
  }

  class Result2(t: Long, res: Array[((Int, Int), Double, Double)]) {
    val timestamp: Long = t
    val statistics_list: Array[Statistic] = res.map(r => new Statistic(r._1._1, r._1._2,r._2,r._3))
  }

  class Ranking(house: Int, household: Int, plug: Int, m: Int, diff: Double) {
    val house_id : Int = house
    val household_id : Int = household
    val plug_id : Int = plug
    val month : Int = m
    val score : Double = diff
  }

  class Result3(t: Long, res: Array[((Int, Int, Int, Int), Double)]) {
    val timestamp: Long = t
    val ranking: Array[Ranking] = res.map(r => new Ranking(r._1._1,r._1._2,r._1._3,r._1._4,r._2))
  }

  class Times(timestamp: Long,
              t1csv: Double, t1parquet: Double, t1avro: Double,
              t2csv: Double, t2parquet: Double, t2avro: Double,
              t3csv: Double, t3parquet: Double, t3avro: Double,
              t1SQLcsv: Double, t1SQLparquet: Double, t1SQLavro: Double,
              t2SQLcsv: Double, t2SQLparquet: Double, t2SQLavro: Double,
              t3SQLcsv: Double, t3SQLparquet: Double, t3SQLavro: Double) {
    val t : Long = timestamp
    val t1 : Double = t1csv
    val t2 : Double = t1parquet
    val t3 : Double = t1avro
    val t4 : Double = t2csv
    val t5 : Double = t2parquet
    val t6 : Double = t2avro
    val t7 : Double = t3csv
    val t8 : Double = t3parquet
    val t9 : Double = t3avro
    val t10 : Double = t1SQLcsv
    val t11 : Double = t1SQLparquet
    val t12 : Double = t1SQLavro
    val t13 : Double = t2SQLcsv
    val t14 : Double = t2SQLparquet
    val t15 : Double = t2SQLavro
    val t16 : Double = t3SQLcsv
    val t17 : Double = t3SQLparquet
    val t18 : Double = t3SQLavro
  }

  def results1ToJSON(res : Array[Int]): String
    = gson.toJson(new Result1(System.currentTimeMillis(), res))

  def results2ToJSON(res : Array[((Int, Int),Double, Double)]): String
    = gson.toJson(new Result2(System.currentTimeMillis(), res))

  def results3ToJSON(res : Array[((Int, Int, Int, Int), Double)]): String
    = gson.toJson(new Result3(System.currentTimeMillis(), res))

  def timesToJson(res: Times): String
    = gson.toJson(res)
}
