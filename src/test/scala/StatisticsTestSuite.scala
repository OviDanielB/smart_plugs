import org.scalatest.{FlatSpec}
import utils.Statistics

class StatisticsTestSuite extends FlatSpec {

  "The online mean" should "calculate mean with equal values" in {
    val values = Seq((2f,1), (2f,1), (2f,1))

    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean._1 == expectedMean(values) && actualMean._2 == 3)
  }

  it should "be equal calculate mean with different values" in {
    val values = Seq((3f,1), (4f,1), (5f,1))
    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean._1 == expectedMean(values) && actualMean._2 == 3)
  }


  it should "be equal calculate mean with different values and long list" in {
    val values = Seq((3.24f,1), (4.53f,1), (5.321f,1), (10.3215f,1), (100.65f,1))
    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean._1 == expectedMean(values) && actualMean._2 == 5)

  }

  def expectedMean(values: Seq[(Float, Int)]): Float = {
    var sum = 0f
    var count = 0
    for(v <- values){
      sum = sum + v._1
      count = count + v._2
    }
    sum / count
  }



}
