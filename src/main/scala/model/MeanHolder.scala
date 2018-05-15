package model

class MeanHolder(x: Double, n: Long) {

  var avg : Double = x
  var count : Long = n

  def mean() : Double = {
    this.avg
  }
}
