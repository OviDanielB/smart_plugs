import config.SmartPlugConfig
import model.{MaxMinHolder, MeanHolder, MeanStdHolder}
import org.scalatest.FlatSpec
import utils.Statistics

class StatisticsTestSuite extends FlatSpec {


  val STATISTICS_DECIMAL_PLACE_PRECISION = 5


  "The max min online function" should "output the correct max and min with increasing timestamps " in {
    val values = Seq(new MaxMinHolder(2.3, 0), new MaxMinHolder(3,1) , new MaxMinHolder(10,2))

    val actualOutput = values.reduce( (t1,t2) => Statistics.computeOnlineMaxMin(t1,t2))
    val expectedOutput : MaxMinHolder = expectedMaxMin(values)
    assert(expectedOutput.min == actualOutput.min)
    assert(expectedOutput.max == actualOutput.max)
  }

  it should "output the correct max and min with decreasing timestamps" in {
    val values = Seq(new MaxMinHolder(2.3, 65), new MaxMinHolder(50, 30 ) , new MaxMinHolder(1, 2))

    val actualOutput = values.reduce( (t1,t2) => Statistics.computeOnlineMaxMin(t1,t2))
    val expectedOutput : MaxMinHolder = expectedMaxMin(values)
    assert(expectedOutput.min == actualOutput.min)
    assert(expectedOutput.max == actualOutput.max)
  }


  it should "output the correct max and min with equal timestamps" in {
    val values = Seq(new MaxMinHolder(2.3, 1), new MaxMinHolder(50, 1 ) , new MaxMinHolder(1, 1))

    val actualOutput = values.reduce( (t1,t2) => Statistics.computeOnlineMaxMin(t1,t2))
    val expectedOutput : MaxMinHolder = expectedMaxMin(values)
    assert(expectedOutput.min == actualOutput.min)
    assert(expectedOutput.max == actualOutput.max)
  }

  def expectedMaxMin(values: Seq[MaxMinHolder]): MaxMinHolder = {
    var max = 0d
    var min = Double.MaxValue
    for(v <- values){
      if(v.value < min ){
        min = v.value
      }
      if(v.value > max ){
        max = v.value
      }
    }
    new MaxMinHolder(0,min,max,0)
  }

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
