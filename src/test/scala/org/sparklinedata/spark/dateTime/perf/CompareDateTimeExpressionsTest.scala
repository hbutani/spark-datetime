package org.sparklinedata.spark.dateTime.perf

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.TestSQLContext._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps


class CompareDateTimeExpressionsTest extends PerfTest {

  override val numTimes : Int = 100

  test("spklFunctionDateTime2") {

    val yr = dateTime('dt) year

    val df = sql(date"select $yr from inputPerf")
    val t = timeAndCollect( df, { r =>
      val v1 = r.getInt(0)
      //println(s"$y1")
    })
    printTiming(t)
  }

  test("nativeFunctions2") {

    val df = sql("select year(dt) from inputPerf")
    val t = timeAndCollect( df, { r =>
      val v1 = r.getInt(0)
    })
    printTiming(t)
  }

  test("spklExpressionDateTime2") {

    val df = sql("select yearE(dateTimeE(dt)) from inputPerf")
    val t = timeAndCollect( df, { r =>
      val v1 = r.getInt(0)
      //println(s"$v1 $v2")
    })
    printTiming(t)
  }

}
