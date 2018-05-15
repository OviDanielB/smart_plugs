object QueryMain {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      printf("Usage: QueryMain <query_number>\n")
      return
    }

    val n = args(0).toInt

    // TODO resolve error "task not serializable"
//    n match {
//      case 1 => val q = new Query1; q.execute()
//      case 2 => val q = new Query2; q.execute()
//      case 3 => val q = new Query3; q.execute()
//    }


  }

}
