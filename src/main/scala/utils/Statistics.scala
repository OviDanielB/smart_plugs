package utils

import model._

/**
  * This object implement functions to compute statistics of mean
  * and standard deviation following online algorithms both considering
  * single value of difference between next and previous value and to compute
  * minimum and maximum along a list of tuples.
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object Statistics extends Serializable {

  /**
    * Computes online mean to AVOID OVERFLOW using Welford's one pass algorithm
    * E[x_n] = E[x_{n-1}] + (x_n - E[x_{n-1}]) / ( (n - 1) + 1)
    *
    * @param prevTuple (E[x_{n-1}], n - 1)
    * @param currTuple (x_n, 1)
    * @return (E[x_n], n )
    */
  def computeOnlineMean(prevTuple: MeanHolder, currTuple: MeanHolder) : MeanHolder = {

    new MeanHolder(prevTuple.avg + (currTuple.avg - prevTuple.avg)/(prevTuple.count + currTuple.count) , prevTuple.count + currTuple.count)
  }

  /**
    * Computes online mean and variance to AVOID OVERFLOW using Welford's one pass algorithm
    * E[x_n] = E[x_{n-1}] + (x_n - E[x_{n-1}]) / ( (n - 1) + 1)
    *
    * @param prevTuple (E[x_{n-1}], n - 1)
    * @param currTuple (x_n, 1)
    * @return (E[x_n], n )
    */
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
    * @param prevTuple (E[x_{n-1}], n - 1)
    * @param currTuple (x_n, 1)
    * @return (E[x_n], n )
    */
  def computeOnlineSubMeanAndStd(prevTuple: SubMeanStdHolder, currTuple: SubMeanStdHolder): SubMeanStdHolder = {

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
  }

  /**
    * Compute maximum and minimum values among a list of tuples.
    *
    * @param prevTuple (previous min, previous max)
    * @param currTuple (min, max)
    * @return (new min, new max)
    */
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
    val n = prevTuple.count + currTuple.count

    val currentValue = currTuple.value - prevTuple.value // x_n

    var oldMean = 0d
    if (prevTuple.avg == -1d)
      oldMean = currentValue
    else
      oldMean = prevTuple.avg
    val newMean = oldMean + (currentValue - oldMean) / n

    new SubMeanHolder(currTuple.value, newMean , n, currTuple.timestamp)
  }
}
