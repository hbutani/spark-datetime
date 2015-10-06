package org.sparklinedata.spark.dateTime.perf

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.sparklinedata.spark.dateTime.TestSQLContext._
import org.sparklinedata.spark.dateTime.Utils._
import org.sparklinedata.spark.dateTime.dsl.expressions._
import org.sparklinedata.spark.dateTime.{BaseTest, TRow}

import scala.language.postfixOps

abstract class PerfTest extends BaseTest {

  override def beforeAll() = {
    super.beforeAll()

    val fmt : DateTimeFormatter = ISODateTimeFormat.dateTime()

    val end = END_DATE
    val start = END_DATE - 20000.day
    val col = intervalToSeq((start to end) , 1.day).map(d => TRow(fmt.print(d)))

    val df = createDataFrame[TRow](sparkContext.parallelize(col))
    df.registerTempTable("inputPerf")
  }

  def timeAndCollect(df : DataFrame, f: Row => Unit, numTimes : Int = 25) : Array[Long] = {
    val res = new Array[Long](numTimes)
    df.foreach(f) // get any compilation thingys out of the way
    (0 until numTimes).foreach { i =>
      val sT = System.currentTimeMillis()
      df.foreach(f)
      val eT = System.currentTimeMillis()
      res(i) = (eT - sT)
    }
    res
  }

  def printTiming(res : Array[Long]) : Unit = {
    println(s"Avg. Time = ${res.sum/res.size.toDouble} mSecs, " +
      s"Min Time = ${res.min} mSecs, Max Time = ${res.max} mSecs")
  }

}
