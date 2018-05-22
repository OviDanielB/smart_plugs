import model.{MaxMinHolder, MaxMinHolderBD, MeanStdHolder, MeanStdHolderBD}
import utils.{CSVParser, CalendarManager, Statistics}

object Query2Alt {

  val calendarManager = new CalendarManager

  def main(args: Array[String]) : Unit = {

    val sc = SparkController.defaultSparkContext()

    val list = sc.textFile("dataset/d14_filtered.csv")
      .map(
        line => CSVParser.parse(line)
      ).filter(el => el.isDefined && el.get.isWorkMeasurement())
      .map(d => {
        val data = d.get
        val holder = new MaxMinHolderBD(data.value)
        ( (data.house_id, data.plug_id, calendarManager.getInterval(data.timestamp), calendarManager.getDayAndMonth(data.timestamp)(0) ),
          holder)
      } )
      .reduceByKey( (x,y) => {
        //println(x +" "+ y)
        Statistics.computeOnlineMaxMinBD(x,y)})
      .map(el => {
        val house = el._1._1
        val plug = el._1._2
        val interval = el._1._3
        val fullDate = el._1._4
        val maxMinHolder = el._2

        val out = ((house, interval, fullDate), maxMinHolder.range())
        //println(out)
        out
      })
      .filter(el => true)//el._1._1 == 0 && el._1._2 == 0)
      .reduceByKey( _+_ )
      .map(el => {
        val house = el._1._1
        val interval = el._1._2
        val totalConsumedPerHouse = el._2

        ((house, interval), new MeanStdHolderBD(totalConsumedPerHouse) )
      }).sortByKey()
      .map(el => {println(el); el})
    .reduceByKey((x,y) => Statistics.computeOnlineMeanAndStdBD(x,y))
      .sortByKey()
      .map(el => {
        println(el._1 + "   " + el._2.mean().toString + "   " + el._2.std().toString)
      }).collect()

    //list.count()
  }
}
