import org.apache.spark.{SparkConf, SparkContext}

object Hello {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("demo-app")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)

    val square = distData
      .map(x => x * x)

    for( i <- square){
      println(i)
    }

  }
}
