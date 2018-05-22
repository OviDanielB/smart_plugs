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

  def this(measure: Double, t : Long) = this(measure,measure,measure, t)

  var value: Double = v
  var timestamp: Long = t
  var min: Double = minH
  var max: Double = maxH

  def mean(): Double = {
    this.max-this.min
  }


  override def toString = s"MaxMinHolder($value, $timestamp, $min, $max)"
}

class MeanStdHolderBD(x: BigDecimal,s: BigDecimal, n: Long) extends Serializable {

  //def this(value: Float) = this(BigDecimal.decimal(value), BigDecimal.decimal(0), 1)
  def this(valBD: BigDecimal) = this(valBD, BigDecimal.decimal(0), 1)


  var avg: BigDecimal = x
  var stdSumUndivided: BigDecimal = s
  var count: Long = n

  def variance(): Float = {
    (this.stdSumUndivided / (this.count - 1)).toFloat
  }

  def mean(): Float = {
    this.avg.toFloat
  }

  def std() : Double = {
    Math.sqrt(variance())
  }


  override def toString = s"MeanStdHolderBD($avg, $stdSumUndivided, $count)"
}



class MaxMinHolderBD(v: BigDecimal, minH: BigDecimal, maxH: BigDecimal) extends Serializable {

  def this(measure: Float) = this(BigDecimal.decimal(measure), BigDecimal.decimal(measure), BigDecimal.decimal(measure))

  //def this(measureBD: BigDecimal) = this(measureBD,measureBD,measureBD)

  var value: BigDecimal = v
  var min: BigDecimal = minH
  var max: BigDecimal = maxH

  def range(): BigDecimal = {
    this.max - this.min
  }

  override def toString = s"MaxMinHolder($value, $min, $max)"

}

