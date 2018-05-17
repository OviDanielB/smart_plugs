package utils

import model.{MeanHolder, MeanStdHolder}

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


  def computeOnlineMeanAndStd(prevTuple: (Float, Int, Double), currTuple: (Float, Int, Double)): (Float, Int, Double) = {
    val n = prevTuple._2 + currTuple._2

    val currentValue = currTuple._1 // x_n

    val oldMean = prevTuple._1
    val newMean = oldMean + (currentValue - oldMean) / n

    val oldStd = prevTuple._3
    val newStd =  oldStd + ( currentValue - oldMean) * (currentValue - newMean) // / ( n - 1 )
    (newMean , n, newStd)
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

}
