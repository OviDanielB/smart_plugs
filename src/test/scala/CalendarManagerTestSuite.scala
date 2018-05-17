import org.joda.time.{DateTime,DateTimeZone}
import org.scalatest.FlatSpec
import utils.CalendarManager

class CalendarManagerTestSuite extends FlatSpec {

  "Calendar manager" should "get the correct time interval" in {

    val calendarManager  = new CalendarManager

    var interval = calendarManager.getInterval(getTimeStampForHourAndMinute(0,0))
    assert(interval == 0)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(5,59))
    assert(interval == 0)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(6,0))
    assert(interval == 1)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(11,59))
    assert(interval == 1)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(12,0))
    assert(interval == 2)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(17,59))
    assert(interval == 2)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(18,0))
    assert(interval == 3)

    interval = calendarManager.getInterval(getTimeStampForHourAndMinute(23,59))
    assert(interval == 3)
  }

  def getTimeStampForHourAndMinute(hourOfDay : Int, minuteOfHour : Int) : Long = {
    new DateTime(2013,9,1,
      hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000

  }
}
