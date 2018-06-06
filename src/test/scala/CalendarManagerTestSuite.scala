import org.joda.time.{DateTime,DateTimeZone}
import org.scalatest.FlatSpec
import utils.CalendarManager

class CalendarManagerTestSuite extends FlatSpec {

  "Calendar manager" should "get the correct time interval" in {

    val calendarManager  = CalendarManager

    var interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(0,0))
    assert(interval == 0)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(5,59))
    assert(interval == 0)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(6,0))
    assert(interval == 1)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(11,59))
    assert(interval == 1)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(12,0))
    assert(interval == 2)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(17,59))
    assert(interval == 2)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(18,0))
    assert(interval == 3)

    interval = calendarManager.getTimeSlot(getTimeStampForHourAndMinute(23,59))
    assert(interval == 3)

    var rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(1,17,59))
    assert(rate == 9)

    rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(1,18,0))
    assert(rate == -9)

    rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(1,6,0))
    assert(rate == 9)

    rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(1,5,59))
    assert(rate == -9)

    rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(6,17,59))
    assert(rate == -9)

    rate = calendarManager.getPeriodRate(getTimeStampForHourAndMinuteAndWeekDay(7,6,0))
    assert(rate == -9)
  }

  def getTimeStampForHourAndMinute(hourOfDay : Int, minuteOfHour : Int) : Long = {
    new DateTime(2013,9,1,
      hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000

  }

  def getTimeStampForHourAndMinuteAndWeekDay(weekDay: Int, hourOfDay : Int, minuteOfHour : Int) : Long = {

    weekDay match {
        // Monday
      case 1 => new DateTime(2013,9,2,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 2 => new DateTime(2013,9,3,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 3 => new DateTime(2013,9,4,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 4 => new DateTime(2013,9,5,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 5 => new DateTime(2013,9,6,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 6 => new DateTime(2013,9,7,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
      case 7 => new DateTime(2013,9,1,
        hourOfDay,minuteOfHour ,DateTimeZone.forID("Europe/Berlin")).getMillis / 1000
    }

  }
}
