package utils

import org.joda.time.{DateTime, DateTimeZone}

class CalendarManager {

  val TIMEZONE: DateTimeZone = DateTimeZone.forID("Europe/Berlin")

  /**
    * Retrieve the interval index related to the value of timestamp
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param timestamp of measurement
    * @return interval index
    */
  def getInterval(timestamp : Long) : Int = {
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
    * Retrieve the rate index related to:
    * - daily hours [06:00,17:59] from Monday to Wednesday - highest rate
    * - nightly hours [18:00, 05:59] on Saturday, Sunday and holidays - lowest rate
    *
    * @param timestamp of measurement
    * @return rate month index, 0 if timestamp does not belong to
    *         es. 5 highest rate on May, -5 lowest rate on May
    */
  def getPeriodRate(timestamp : Long) : Int = {
    val date = new DateTime(timestamp*1000L, TIMEZONE)

    if ( date.getHourOfDay >= 6 && date.getHourOfDay <= 17
          && date.getDayOfWeek < 6 && !isHoliday(date)) {
      return date.getMonthOfYear
    } else if (date.getHourOfDay >= 18 && date.getHourOfDay <= 5
                && date.getDayOfWeek > 5 && isHoliday(date)) {
      return -date.getMonthOfYear
    }
    0
  }

  def isHoliday(dateTime: DateTime) : Boolean = {
    // TODO
    false
  }
}
