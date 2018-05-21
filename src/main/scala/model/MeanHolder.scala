package model

class MeanHolder(x: Double, n: Long) extends Serializable {

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
