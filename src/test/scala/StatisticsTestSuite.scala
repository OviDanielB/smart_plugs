import config.SmartPlugConfig
import model.{MeanHolder, MeanStdHolder}
import org.scalatest.FlatSpec
import utils.Statistics

class StatisticsTestSuite extends FlatSpec {

  SmartPlugConfig.SPARK_APP_NAME

  val STATISTICS_DECIMAL_PLACE_PRECISION = 5

  "The online algorithm" should "calculate mean with equal values" in {
    val values = Seq((2f,1), (2f,1), (2f,1))

    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean._1 == expectedMeanTuple(values))
    assert(actualMean._2 == values.length)
  }

  it should "be equal calculate mean with different values" in {
    val values = Seq((3f,1), (4f,1), (5f,1))
    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean._1 == expectedMeanTuple(values))
    assert(actualMean._2 == values.length)
  }


  it should "be equal calculate mean with different values and long list with decimal places" in {
    val values = Seq(new MeanHolder(3.24f,1), new MeanHolder(4.53f,1),
      new MeanHolder(5.321f,1), new MeanHolder(10.3215f,1),
      new MeanHolder(100.65f,1))
    val actualMean = values.reduce((s,v) => Statistics.computeOnlineMean(s,v))
    assert(actualMean.mean() == expectedMean(values).mean())
    assert(actualMean.count == values.length)

  }


  "The online one-pass algorithm" should "should calculate mean and std with equal values" in {
    val values = Seq((2f,1, 0d), (2f,1, 0d), (2f,1, 0d))
    val actualMeanStd = values.reduce( (s,v) => Statistics.computeOnlineMeanAndStd(s,v))
    val expectedMeanStd = expectedMeanStdFromTriple(values)
    assert(actualMeanStd._1 == expectedMeanStd._1)
    assert(actualMeanStd._3 / (actualMeanStd._2 - 1) == expectedMeanStd._3)
    assert(values.length == expectedMeanStd._2)
  }

  it should "should calculate mean and std with different values" in {
    val values = Seq((3f,1, 0d), (4f,1, 0d), (5f,1, 0d))
    val actualMeanStd = values.reduce( (s,v) => Statistics.computeOnlineMeanAndStd(s,v))
    val expectedMeanStd = expectedMeanStdFromTriple(values)
    assert(actualMeanStd._1 == expectedMeanStd._1)
    assert(actualMeanStd._3 / (actualMeanStd._2 - 1) == expectedMeanStd._3)
    assert(values.length == expectedMeanStd._2)
  }

  it should "should calculate mean and std with different values and long list with decimal places" in {
    val values = Seq(new MeanStdHolder(3.3f,1, 0d), new MeanStdHolder(4.21f,1, 0d),
      new MeanStdHolder(5.432f,1, 0d), new MeanStdHolder(10.0f, 1, 0d),
      new MeanStdHolder(2.5f, 1, 0d))

    val actualMeanStd = values.reduce( (s,v) => Statistics.computeOnlineMeanAndStd(s,v))
    val expectedMeanStd = expectedMeanStdClass(values)
    assert(round(STATISTICS_DECIMAL_PLACE_PRECISION)(actualMeanStd.mean()) == round(STATISTICS_DECIMAL_PLACE_PRECISION)(expectedMeanStd.mean()) )
    assert(round(STATISTICS_DECIMAL_PLACE_PRECISION)(actualMeanStd.std()) == round(STATISTICS_DECIMAL_PLACE_PRECISION)(expectedMeanStd.std()))
    assert(values.length == expectedMeanStd.count)
  }
  // 5.0884 - 34.9116512 -
  def round(p: Int)(n: Double) : Double = {BigDecimal(n).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble}

  def expectedMeanStdClass(values: Seq[MeanStdHolder]) : MeanStdHolder = {
    var sum = 0d
    var count = 0L
    for(v <- values){
      sum = sum + v.avg
      count = count + v.count
    }
    val mean = sum / count
    var sumD : Double = 0
    for( s <- values){
      sumD = sumD + Math.pow( s.avg - mean , 2)
    }
    //val variance = sumD / ( count - 1)
    new MeanStdHolder(mean, count, sumD)
  }

  def expectedMeanStdFromTriple(values: Seq[(Float, Int, Double)]) : (Float, Int, Double) = {
    var sum = 0f
    var count = 0
    for(v <- values){
      sum = sum + v._1
      count = count + v._2
    }
    val mean = sum / count
    var sumD : Double = 0
    for( s <- values){
      sumD = sumD + Math.pow( s._1 - mean , 2)
    }
    val variance = sumD / ( count - 1)
    (mean, count, variance)
  }

  def expectedMeanTuple(values: Seq[(Float, Int)]): Float = {
    var sum = 0f
    var count = 0
    for(v <- values){
      sum = sum + v._1
      count = count + v._2
    }
    sum / count
  }

  def expectedMean(values: Seq[MeanHolder]): MeanHolder = {
    var sum = 0d
    var count = 0L
    for(v <- values){
      sum = sum + v.avg
      count = count + v.count
    }
    new MeanHolder(sum / count, count)
  }



}
