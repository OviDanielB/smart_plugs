package utils

import de.jollyday.HolidayManager
import org.joda.time.{DateTime, DateTimeZone}

/**
  * This class implements method to manage timestamp values
  * as a date, referring to the Europe/Berlin timezone,
  * to check if a day is a holiday (in Germany) or not,
  * weekend (Saturday, Sunday) or not and to retrieve
  * what rate (high or low) the date and hour are referring to
  * or what slot hour of the timestamp is referring to.
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object CalendarManager extends Serializable {

  val TIMEZONE: DateTimeZone = DateTimeZone.forID("Europe/Berlin")
  val HOLIDAY_HIERARCHY: String = "de"
  val hm: HolidayManager = HolidayManager.getInstance()

  def fullDateString(timestamp: Long) : String = {
    val date = getDateFromTimestamp(timestamp)
    val day = date.getDayOfMonth
    val month = date.getMonthOfYear
    val year = date.getYear
    s"$day-$month-$year"
  }

  /**
    * Retrieve the interval index related to the value of timestamp
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param hour of measurement
    * @return slot index
    */
  def getTimeSlot(hour : Int) : Int = {
    if (hour <= 5 ) {                                  // [00:00,05:59]
      return 0
    } else if (hour > 5 && hour <= 11) {  // [06:00,11:59]
      return 1
    } else if (hour > 11 && hour <= 17) { // [12:00, 17:59]
      return 2
    } else if (hour > 17 && hour <= 23) { // [18:00, 23:59]
      return 3
    }
    -1
  }

  /**
    * Retrieve the interval index related to the value of timestamp
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param timestamp timestamp
    * @return slot index
    */
  def getTimeSlot(timestamp: Long) : Int = {
    val date = getDateFromTimestamp(timestamp)
    getTimeSlot(date.getHourOfDay)
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
    val date = getDateFromTimestamp(timestamp)
    val res = new Array[Int](3)
    res(0) = getTimeSlot(date.getHourOfDay)
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
    val date = getDateFromTimestamp(timestamp)
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
    val date = getDateFromTimestamp(timestamp)
    date.getDayOfYear
  }

  /**
    * Retrieve the day number along a whole year
    * related to the specified timestamp
    *
    * @param timestamp of measurement
    * @return
    */
  def getHourOfDay(timestamp : Long) : Int = {
    val date = getDateFromTimestamp(timestamp)
    date.getHourOfDay
  }

  /**
    * Retrieve the day number along a month
    * related to the specified timestamp
    *
    * @param timestamp of measurement
    * @return
    */
  def getDayOfMonth(timestamp : Long) : Int = {
    val date = getDateFromTimestamp(timestamp)
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

    val date = getDateFromTimestamp(timestamp)

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

     hm.isHoliday(dateTime.toGregorianCalendar, HOLIDAY_HIERARCHY)
  }

  /**
    * Get date format equivalent to the given timestamp
    * following Europe/Berlin Timezone.
    *
    * @param timestamp timestamp
    * @return date
    */
  def getDateFromTimestamp(timestamp: Long) : DateTime = {
    new DateTime(timestamp * 1000L, TIMEZONE)
  }
}
