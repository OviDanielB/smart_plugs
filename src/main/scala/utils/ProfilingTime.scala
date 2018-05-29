package utils

/**
  * This object implements function to measure time in ms
  * to compare queries executions.
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object ProfilingTime {

  def time[A](f: => A): A = {
    val s = System.nanoTime()
    val ret = f
    println("Time: " + (System.nanoTime - s) / 1e6 + "ms")
    ret
  }

  def getTime[A](f: => A): Double = {
    val s = System.nanoTime()
    f
    (System.nanoTime - s) / 1e6
  }

  def getMeanTime[A](n: Int, f: => A): Double = {
    var t = 0.0
    for (i <- 0 until n) {
      t = t + getTime(f)
    }

    println("Time: " + t / 100.0 + "ms")
    t / n
  }

}
