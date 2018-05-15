package model

class MeanHolder(x: Double, n: Long) extends Serializable {

  var avg : Double = x
  var count : Long = n

  def mean() : Double = {
    this.avg
  }
}
