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

  def this(value: Double) = this(value, 1, 0d)

  def variance() : Double = {
    this.stdSumUndivided / (this.count - 1)
  }

  def std() : Double = {
    Math.sqrt(variance())
  }


  override def toString = s"MeanStdHolder( avg $avg, count $count, stdUndivided $stdSumUndivided)"
}


class SubMeanStdHolder(v : Double, x: Double, n: Long, s: Double, t: Long) extends SubMeanHolder(v, x, n,t) {
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