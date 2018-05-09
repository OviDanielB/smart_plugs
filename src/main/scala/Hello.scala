import org.apache.spark.{SparkConf, SparkContext}
import utils.CSVParser

object Hello {

  def main(args: Array[String]): Unit = {

    val line = "a33011,1377986420,0,1,1,0,3"

    val parsed = CSVParser.parse(line)
    println(parsed.get)

    /*val conf = new SparkConf()
    conf.setAppName("demo-app")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    var list = sc.textFile("dataset/d14_filtered.csv")
      .collect()

    for( line <- list){
      println(line)
    }


    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    val square = distData
      .map(x => x * x)

    for( i <- square){
      println(i)
    } */

  }
}
