package utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object udfDataFunction {

  private val calendarManager = new CalendarManager

  private val getTimeSlot: Long => Int = {
    calendarManager.getTimeSlot
  }

  private val getPeriodRate: Long => Int = {
    calendarManager.getPeriodRate
  }

  private val getDayOfMonth: Long => Int = {
    calendarManager.getDayAndMonth(_)(0)
  }

  private val getDayOfYear: Long => Int = {
    calendarManager.getDayOfYear
  }

  val getTimeSlotUDF : UserDefinedFunction = udf(getTimeSlot)

  val getPeriodRateUDF : UserDefinedFunction = udf(getPeriodRate)

  val getDayOfMonthUDF : UserDefinedFunction= udf(getDayOfMonth)

  val getDayOfYearUDF : UserDefinedFunction= udf(getDayOfYear)

  val invertSignUDF : UserDefinedFunction= udf{ (d:Double, s:Int) => if (s < 0) -d else d}

}
