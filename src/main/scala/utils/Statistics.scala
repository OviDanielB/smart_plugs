package utils

object Statistics {

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


}
