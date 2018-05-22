package utils

import model._

object Statistics extends Serializable {

  /**
    * computes online mean to AVOID OVERFLOW using Welford's one pass algorithm
    * E[x_n] = E[x_{n-1}] + (x_n - E[x_{n-1}]) / ( (n - 1) + 1)
    * @param prevTuple (E[x_{n-1}], n - 1)
    * @param currTuple (x_n, 1)
    * @return (E[x_n], n )
    */
  def computeOnlineMean(prevTuple: (Float, Int), currTuple: (Float, Int)) : (Float, Int) = {

    (prevTuple._1 + (currTuple._1 - prevTuple._1)/(prevTuple._2 + currTuple._2) , prevTuple._2 + currTuple._2)
  }

  def computeOnlineMean(prevTuple: MeanHolder, currTuple: MeanHolder) : MeanHolder = {

    new MeanHolder(prevTuple.avg + (currTuple.avg - prevTuple.avg)/(prevTuple.count + currTuple.count) , prevTuple.count + currTuple.count)
  }

  def computeOnlineMeanBD(prevTuple: MeanHolderBD, currTuple: MeanHolderBD) : MeanHolderBD = {

    new MeanHolderBD(prevTuple.avg + (currTuple.avg - prevTuple.avg)/(prevTuple.count + currTuple.count) , prevTuple.count + currTuple.count)
  }


  def computeOnlineMeanAndStd(prevTuple: (Float, Int, Double), currTuple: (Float, Int, Double)): (Float, Int, Double) = {
    val n = prevTuple._2 + currTuple._2

    val currentValue = currTuple._1 // x_n

    val oldMean = prevTuple._1
    val newMean = oldMean + (currentValue - oldMean) / n

    val oldStd = prevTuple._3
    val newStd =  oldStd + ( currentValue - oldMean) * (currentValue - newMean) // / ( n - 1 )
    (newMean , n, newStd)
  }


  def computeOnlineMeanAndStdBD(prevTuple: MeanStdHolderBD, currTuple: MeanStdHolderBD): MeanStdHolderBD = {
    val n = prevTuple.count + currTuple.count

    val currentValue = currTuple.avg // x_n

    val oldMean = prevTuple.avg
    val newMean = oldMean + (currentValue - oldMean) / BigDecimal.decimal(n)

    val oldStd = prevTuple.stdSumUndivided
    val newStd = oldStd + ( currentValue - oldMean) * (currentValue - newMean)
    new MeanStdHolderBD(newMean , newStd, n)
  }


  def computeOnlineMeanAndStd(prevTuple: MeanStdHolder, currTuple: MeanStdHolder): MeanStdHolder = {
    val n = prevTuple.count + currTuple.count

    val currentValue = currTuple.avg // x_n

    val oldMean = prevTuple.avg
    val newMean = oldMean + (currentValue - oldMean) / n

    val oldStd = prevTuple.stdSumUndivided
    val newStd = oldStd + ( currentValue - oldMean) * (currentValue - newMean)
    new MeanStdHolder(newMean , n, newStd)
  }

  /**
    * Compute online mean and standard deviation among values given by the
    * SUBTRACTION between one and the previous one.
    *
    * @param prevTuple
    * @param currTuple
    * @return
    */
  def computeOnlineSubMeanAndStd(prevTuple: SubMeanStdHolder, currTuple: SubMeanStdHolder): SubMeanStdHolder = {

    if (prevTuple.timestamp != currTuple.timestamp) {

      val n = prevTuple.count + currTuple.count

      val currentValue = currTuple.value - prevTuple.value // x_n

      var oldMean = 0d
      if (prevTuple.avg == -1d)
        oldMean = currentValue
      else
        oldMean = prevTuple.avg
      val newMean = oldMean + (currentValue - oldMean) / n

      val oldStd = prevTuple.stdSumUndivided
      val newStd = oldStd + (currentValue - oldMean) * (currentValue - newMean)
      new SubMeanStdHolder(currTuple.value, newMean, n, newStd, currTuple.timestamp)
    } else
      new SubMeanStdHolder(prevTuple.value, prevTuple.avg , prevTuple.count, prevTuple.stdSumUndivided, prevTuple.timestamp)

  }

  def computeOnlineMaxMinBD(prevTuple: MaxMinHolderBD, currTuple: MaxMinHolderBD): MaxMinHolderBD = {
    var newMin: BigDecimal = 0d
    var newMax: BigDecimal = 0d

    if (currTuple.value < prevTuple.min)
      newMin = currTuple.value
    else
      newMin = prevTuple.min

    if (currTuple.value > prevTuple.max)
      newMax = currTuple.value
    else
      newMax = prevTuple.max

    new MaxMinHolderBD(currTuple.value, newMin, newMax)
  }

  def computeOnlineMaxMin(prevTuple: MaxMinHolder, currTuple: MaxMinHolder): MaxMinHolder = {

      var newMin = prevTuple.min
      var newMax = prevTuple.max

      if (currTuple.min < prevTuple.min) {
        newMin = currTuple.min
      }
      if (currTuple.max > prevTuple.max) {
        newMax = currTuple.max
      }

      new MaxMinHolder(newMin, newMax)
  }
    /**
    * Compute online mean among values given by the SUBTRACTION between one and the previous one.
    *
    * @param prevTuple
    * @param currTuple
    * @return
    */
  def computeOnlineSubMean(prevTuple: SubMeanHolder, currTuple: SubMeanHolder): SubMeanHolder = {

    if (prevTuple.timestamp != currTuple.timestamp) {
      if (prevTuple.timestamp > currTuple.timestamp) {
        println("[ERR] not sorted by timestamps")
      }
      val n = prevTuple.count + currTuple.count

      var currentValue = currTuple.value - prevTuple.value // x_n
//      if (currentValue < 0) {
//        currentValue = 0d
//      }

      var oldMean = 0d
      if (prevTuple.avg == -1d)
        oldMean = currentValue
      else
        oldMean = prevTuple.avg
      val newMean = oldMean + (currentValue - oldMean) / n

      new SubMeanHolder(currTuple.value, newMean , n, currTuple.timestamp)
    } else {
      new SubMeanHolder(prevTuple.value, prevTuple.avg, prevTuple.count, prevTuple.timestamp)
    }
  }
}
