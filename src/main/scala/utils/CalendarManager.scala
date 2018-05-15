package utils

import org.joda.time.{DateTime, DateTimeZone}

class CalendarManager {

  /**
    * Retrieve the interval index related to the value of timestamp
    * among the four ranges:
    * [00:00,05:59], [06:00,11:59], [12:00, 17:59], [18:00, 23:59]
    *
    * @param timestamp of measurement
    * @return interval index
    */
  def getInterval(timestamp :Long): Int = {
    val date = new DateTime(timestamp*1000L, DateTimeZone.UTC)

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

}
