//import utils.ProfilingTime
//
//object QueryMain {
//
//  def main(args: Array[String]): Unit = {
//
//    if (args.length == 0) {
//      printf("Usage: QueryMain <query_number>\n")
//      return
//    }
//
//    val n = args(0).toInt
//
//    n match {
//      case 1 => ProfilingTime.time{Query1.execute()}
//      case 2 => ProfilingTime.time{Query2.execute()}
//      case 3 => ProfilingTime.time{Query3.execute()}
//    }
//
//  }
//
//}
