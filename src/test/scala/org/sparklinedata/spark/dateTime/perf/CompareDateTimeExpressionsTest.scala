package org.sparklinedata.spark.dateTime.perf

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.sparklinedata.spark.dateTime.TestSQLContext._
import org.sparklinedata.spark.dateTime.dsl.expressions._


class CompareDateTimeExpressionsTest extends PerfTest {

  test("spklFunctionDateTime") {

    val dT = dateTime('dt)

    val df = sql(date"select $dT from inputPerf")
    val t = timeAndCollect( df, { r =>
      val y1 = r.get(0)
      //println(s"$y1")
    })
    printTiming(t)
  }

  test("spklExpressionDateTime") {

    val df = sql(date"select dateTimeE(dt) from inputPerf")
    val t = timeAndCollect( df, { r =>
      val y1 = r.get(0)
      //println(s"$y1")
    })
    printTiming(t)
  }

}
