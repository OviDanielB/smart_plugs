package utils

object ProfilingTime {

  def time[A](f: => A): A = {
    val s = System.nanoTime()
    val ret = f
    printf("Time: " + (System.nanoTime - s) / 1e6 + "ms")
    ret
  }

}
