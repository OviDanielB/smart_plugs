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

    val data = sc.textFile("hdfs://localhost:54310/dataset/d14_filtered.csv").collect()

    for ( row <- data ) {
      println(row)
    } */
  }
}
