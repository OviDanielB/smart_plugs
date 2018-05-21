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

class MaxMinHolder(v: Double, minH: Double, maxH: Double, t: Long) extends Serializable {

  var value: Double = v
  var timestamp: Long = t
  var min: Double = minH
  var max: Double = maxH

  def mean(): Double = {
    this.max-this.min
  }
}
