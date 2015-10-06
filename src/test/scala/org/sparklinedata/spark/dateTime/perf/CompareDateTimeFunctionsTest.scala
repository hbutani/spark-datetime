package org.sparklinedata.spark.dateTime.perf

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.TestSQLContext._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

/*
 * Compare spark native functions vs sparklineDTFunctions
 * see docs folder for a profile.
 *
 * - significant portion of difference in the case of sparklinedateTime functions has to do
 *   with the Function object used for each function.
 */
class CompareDateTimeFunctionsTest extends PerfTest {

  test("nativeFunctions") {

    val df = sql("select year(dt), dayOfMonth(dt), month(dt), minute(dt), second(dt) from inputPerf")
    val t = timeAndCollect( df, { r =>
      val o = r.getInt(0)
      val dm = r.getInt(1)
      val m = r.getInt(2)
      val mi = r.getInt(3)
      val sec = r.getInt(4)
    })
    printTiming(t)
  }

  test("sparklineDateTimeFunction") {
    val dT = dateTime('dt)
    val year = dateTime('dt) year
    val dM = dateTime('dt) dayOfMonth
    val m = dateTime('dt) monthOfYear
    val mi = dateTime('dt) minuteOfDay
    val sec = dateTime('dt) secondOfDay

    val df = sql(date"select $year, $dM, $m, $mi, $sec from inputPerf")
    val t = timeAndCollect( df, { r =>
      val o = r.getInt(0)
      val dm = r.getInt(1)
      val m = r.getInt(2)
      val mi = r.getInt(3)
      val sec = r.getInt(4)
    })
    printTiming(t)
  }

}
