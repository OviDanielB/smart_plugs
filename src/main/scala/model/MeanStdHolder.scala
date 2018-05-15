package model

/**
  * class for holding statistic data
  * (mean, std, count, etc)
  * NOTE: stdSumUndivided is std * (n - 1)  and NOT std
  * to get std, call std() method
  * @param x avg value
  * @param n count value
  * @param s std * (n - 1)
  */
class MeanStdHolder(x: Double, n: Long, s: Double) extends MeanHolder(x, n) {
  /*var avg : Double = x
  var count : Long = n */
  var stdSumUndivided : Double = s


  def variance() : Double = {
    this.stdSumUndivided / (this.count - 1)
  }

  def std() : Double = {
    Math.sqrt(variance())
  }
}
