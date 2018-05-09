import org.apache.spark.{SparkConf, SparkContext}

object Hello {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("demo-app")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://localhost:54310/dataset/d14_filtered.csv").collect()

    for ( row <- data ) {
      println(row)
    }
  }
}
