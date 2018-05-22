package utils


import de.jollyday.HolidayManager
import org.joda.time.{DateTime, DateTimeZone}

class CalendarManager extends Serializable {

  val TIMEZONE: DateTimeZone = DateTimeZone.forID("Europe/Berlin")
  val HOLIDAY_HIERARCHY: String = "de"

  /**
    * Retrieve the interval index related to the value of timestamp
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param timestamp of measurement
    * @return interval index
    */
  def getTimeSlot(timestamp : Long) : Int = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)

    if (date.getHourOfDay <= 5 ) {                                  // [00:00,05:59]
      return 0
    } else if (date.getHourOfDay > 5 && date.getHourOfDay <= 11) {  // [06:00,11:59]
      return 1
    } else if (date.getHourOfDay > 11 && date.getHourOfDay <= 17) { // [12:00, 17:59]
      return 2
    } else if (date.getHourOfDay > 17 && date.getHourOfDay <= 23) { // [18:00, 23:59]
      return 3
    }
    -1
  }

  /**
    * Retrieve the interval index related to the date
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param date of measurement
    * @return interval index
    */
  def getInterval(date : DateTime) : Int = {

    if (date.getHourOfDay <= 5 ) {                                  // [00:00,05:59]
      return 0
    } else if (date.getHourOfDay > 5 && date.getHourOfDay <= 11) {  // [06:00,11:59]
      return 1
    } else if (date.getHourOfDay > 11 && date.getHourOfDay <= 17) { // [12:00, 17:59]
      return 2
    } else if (date.getHourOfDay > 17 && date.getHourOfDay <= 23) { // [18:00, 23:59]
      return 3
    }
    -1
  }

  /**
    * Retrieve from the measurement timestamp information about
    * time slot referring to among [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59],
    * day and month of measurement.
    *
    * @param timestamp of measurement
    * @return [time slot index, day, month]
    */
  def getTimeReference(timestamp: Long) : Array[Int] = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)
    val res = new Array[Int](3)
    res(0) = getInterval(date)
    res(1) = date.getDayOfMonth
    res(2) = date.getMonthOfYear
    res
  }

  /**
    * Retrieve the day and the month related to the
    * measurement timestamp.
    *
    * @param timestamp of measurement
    * @return [day, month]
    */
  def getDayAndMonth(timestamp : Long) : Array[Int] = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)
    val res = new Array[Int](2)
    res(0) = date.getDayOfMonth
    res(1) = date.getMonthOfYear
    res
  }

  /**
    * Retrieve the day number along a whole year
    * related to the specified timestamp
    *
    * @param timestamp of measurement
    * @return
    */
  def getDayOfYear(timestamp : Long) : Int = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)
    date.getDayOfYear
  }

  /**
    * Retrieve the day number along a month
    * related to the specified timestamp
    *
    * @param timestamp of measurement
    * @return
    */
  def getDayOfMonth(timestamp : Long) : Int = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)
    date.getDayOfMonth
  }

  /**
    * Retrieve the rate index related to:
    * - daily hours [06:00,17:59] from Monday to Wednesday - highest rate
    * - nightly hours [18:00, 05:59] from Monday to Wednesday,
    *     on Saturday, Sunday and holidays - lowest rate
    *
    * @param timestamp of measurement
    * @return rate month index positive if high rate, negative otherwise
    *         es. 5 highest rate on May, -5 lowest rate on May
    */
  def getPeriodRate(timestamp : Long) : Int = {

    val date = new DateTime(timestamp*1000L, TIMEZONE)

    if ( date.getHourOfDay >= 6 && date.getHourOfDay <= 17
          && date.getDayOfWeek < 6 && !isHoliday(date)) {
      date.getMonthOfYear
     } else 0-date.getMonthOfYear
  }

  /**
    * Check if the date is signed as holiday following
    * German holidays collection in "Holidays_de" file.
    *
    * @param dateTime possible holiday
    * @return if holiday
    */
  def isHoliday(dateTime: DateTime) : Boolean = {
    val hm: HolidayManager = HolidayManager.getInstance()
    hm.isHoliday(dateTime.toGregorianCalendar, HOLIDAY_HIERARCHY)
  }
}
