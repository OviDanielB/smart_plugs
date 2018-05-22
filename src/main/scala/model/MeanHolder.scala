package model

class MeanHolder(x: Double, n: Long) extends Serializable {

//  var timestamp: Long = t
  var avg: Double = x
  var count: Long = n

  def mean(): Double = {
    this.avg
  }
}

class SubMeanHolder(v: Double, x: Double, n: Long, t:Long) extends Serializable {

  var timestamp: Long = t
    var value: Double = v // previous value
    var avg: Double = x
    var count: Long = n

    def mean(): Double = {
      this.avg
    }
}

class MaxMinHolder(minH: Float, maxH: Float) extends Serializable {

  var min: Float = minH
  var max: Float = maxH

  def delta(): Double = {
    this.max-this.min
  }
}
