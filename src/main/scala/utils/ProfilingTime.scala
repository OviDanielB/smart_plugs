package utils

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

}
