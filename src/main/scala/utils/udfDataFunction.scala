package utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/**
  * User Defined Functions to implement calendar utils
  * for Spark SQL through Calendar Manager to offer easier
  * work to resolve some queries, comporting an additional
  * overhead.
  *
  * @author Ovidiu Daniel Barba
  * @author Laura Trivelloni
  * @author Emanuele Vannacci
  */
object udfDataFunction {

  private val calendarManager = CalendarManager

  private val getTimeSlot: Long => Int = calendarManager.getTimeSlot

  private val getPeriodRate: Long => Int = calendarManager.getPeriodRate

  private val getDayOfMonth: Long => Int = calendarManager.getDayOfMonth

  private val getDayOfYear: Long => Int = calendarManager.getDayOfYear

  private val getHourOfDay: Long => Int = calendarManager.getHourOfDay

  /**
    * Retrieve the index of the referring time slot among [00:00,05:59],
    * [06:00,11:59], [12:00, 17:59], [18:00, 23:59], given the measurement timestamp.
    */
  val getTimeSlotUDF : UserDefinedFunction = udf(getTimeSlot)

  /**
    * Retrieve referring period rate between the high rate and the low one
    * from measurement timestamp, returned as positive index of the referring month
    * (if high rate) or negative index of the referring month (if low rate),
    * given the mesurement timestamp.
    */
  val getPeriodRateUDF : UserDefinedFunction = udf(getPeriodRate)

  /**
    * Retrieve day index of month given the measurement timestamp.
    */
  val getDayOfMonthUDF : UserDefinedFunction= udf(getDayOfMonth)

  /**
    * Retrieve hour index of day given the measurement timestamp.
    */
  val getHourOfDayUDF : UserDefinedFunction= udf(getHourOfDay)

  /**
    * Retrieve day index of year given the measurement timestamp.
    */
  val getDayOfYearUDF : UserDefinedFunction= udf(getDayOfYear)

  /**
    * Invert sign of the given value of energy consumption only if
    * the given rate is a low rate.
    */
  val invertSignUDF : UserDefinedFunction= udf{ (d:Double, s:Int) => if (s < 0) -d else d}

}
