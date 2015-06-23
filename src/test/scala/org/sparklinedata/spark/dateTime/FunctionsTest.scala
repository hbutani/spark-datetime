/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sparklinedata.spark.dateTime

import org.apache.spark.sql.test._
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import com.github.nscala_time.time.Imports._


import TestSQLContext._
import Utils._
import org.sparklinedata.spark.dateTime.Functions._
case class TRow(dt : String)

class FunctionsTest extends FunSuite with BeforeAndAfterAll {

  override def beforeAll() = {
    Functions.register(TestSQLContext)
    val fmt : DateTimeFormatter = ISODateTimeFormat.dateTime()

    val end = DateTime.now
    val start = end - 30.days
    val col = intervalToSeq((start to end) , 1.day).map(d => TRow(fmt.print(d)))

    val df = createDataFrame[TRow](sparkContext.parallelize(col))
    df.registerTempTable("input")
    df.printSchema()

  }

  test("sql") {
    val t = sql("select dt, " +
      "dateTime(dt), " +
      "dayOfWeek(dateTime(dt)), " +
      "dayOfWeekName(dateTime(dt)), " +
      "dayOfWeekName(dateTimeWithTZ(dt)) " +
      "from input")
    t.collect.foreach { r =>
      val o = r.getString(0)
      val d : DateTime = r.getAs[SparkDateTime](1)
      val dayOfWeekVal : Int = r.getInt(2)
      val dayOfWeekNameVal : String = r.getString(3)
      val dayOfWeekName2Val : String = r.getString(4)

      val oDt = DateTime.parse(o).withZone(DateTimeZone.UTC)
      assert(oDt == d)
      assert (oDt.getDayOfWeek == dayOfWeekVal)
      assert (oDt.dayOfWeek().getAsText == dayOfWeekNameVal)
      val oDtTz = DateTime.parse(o)
      assert (oDtTz.dayOfWeek().getAsText == dayOfWeekName2Val)
    }
  }

  test("dsl") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.sparklinedata.spark.dateTime.dsl.expressions._

    val dT = dateTime('dt)
    val dOW = dateTime('dt) dayOfWeek
    val dOWNm = dateTime('dt) dayOfWeekName
    val dOWNm2 = dateTimeWithTZ('dt) dayOfWeekName
    val dTFixed = dateTime("2015-05-22T08:52:41.903-07:00")

    val t = sql(date"select dt, $dT, $dOW, $dOWNm, $dOWNm2, $dTFixed," +
      " dateTime(\"2015-05-22T08:52:41.903-07:00\") from input")

    val may22 = DateTime.parse("2015-05-22T08:52:41.903-07:00").withZone(DateTimeZone.UTC)
    t.collect.foreach { r =>
      val o = r.getString(0)
      val d : DateTime = r.getAs[SparkDateTime](1)
      val dayOfWeekVal : Int = r.getInt(2)
      val dayOfWeekNameVal : String = r.getString(3)
      val dayOfWeekName2Val : String = r.getString(4)
      val d2  : DateTime = r.getAs[SparkDateTime](6)

      val oDt = DateTime.parse(o).withZone(DateTimeZone.UTC)
      assert(oDt == d)
      assert (oDt.getDayOfWeek == dayOfWeekVal)
      assert (oDt.dayOfWeek().getAsText == dayOfWeekNameVal)
      val oDtTz = DateTime.parse(o)
      assert (oDtTz.dayOfWeek().getAsText == dayOfWeekName2Val)
      assert(may22 == d2)
    }

  }

  test("period") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.sparklinedata.spark.dateTime.dsl.expressions._

    val dT = dateTime('dt)
    val dT1 = dateTime('dt) + 3.months
    val dT2 = dateTime('dt) - 3.months
    val dT3 = dateTime('dt) + 12.week
    val cE = dateTime('dt) + 3.months > (dateTime('dt) + 12.week)

    val t = sql(date"select dt, $dT, $dT1, $dT2, $dT3, $cE from input")
    t.collect.foreach { r =>
      val o = r.getString(0)
      val d : DateTime = r.getAs[SparkDateTime](1)
      val d1 : DateTime = r.getAs[SparkDateTime](2)
      val d2 : DateTime = r.getAs[SparkDateTime](3)
      val d3 : DateTime = r.getAs[SparkDateTime](4)
      val c : Boolean = r.getBoolean(5)
      val oDt = DateTime.parse(o).withZone(DateTimeZone.UTC)
      assert(oDt == d)
      assert (oDt + 3.months == d1)
      assert(oDt - 3.month == d2)
      assert(oDt + 12.week == d3)
      assert((oDt + 3.months > oDt + 12.weeks)== c)
    }

  }

  test("allDateFunctions") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    import org.sparklinedata.spark.dateTime.dsl.expressions._

    val dT = dateTime('dt)
    val millis = dateTime('dt) millis
    val timeZoneId = dateTime('dt) timeZoneId
    val era = dateTime('dt) era
    val centuryOfEra = dateTime('dt) centuryOfEra
    val yearOfEra = dateTime('dt) yearOfEra
    val yearOfCentury = dateTime('dt) yearOfCentury
    val year = dateTime('dt) year
    val weekyear = dateTime('dt) weekyear
    val monthOfYear = dateTime('dt) monthOfYear
    val monthOfYearName = dateTime('dt) monthOfYearName
    val weekOfWeekyear = dateTime('dt) weekOfWeekyear
    val dayOfYear = dateTime('dt) dayOfYear
    val dayOfMonth = dateTime('dt) dayOfMonth
    val dayOfWeek = dateTime('dt) dayOfWeek
    val dayOfWeekName = dateTime('dt) dayOfWeekName
    val hourOfDay = dateTime('dt) hourOfDay
    val minuteOfDay = dateTime('dt) minuteOfDay
    val secondOfDay = dateTime('dt) secondOfDay
    val secondOfMiunte = dateTime('dt) secondOfMinute
    val millisOfDay = dateTime('dt) millisOfDay
    val millisOfSecond = dateTime('dt) millisOfSecond

    val t = sql(date"select dt, $dT, $millis, $timeZoneId, $era, $centuryOfEra, $yearOfEra, $yearOfCentury, $year, " +
      date"$weekyear, $monthOfYear, $monthOfYearName, $weekOfWeekyear, $dayOfYear, $dayOfMonth, $dayOfWeek, " +
      date"$dayOfWeekName, $hourOfDay, $minuteOfDay, $secondOfDay, $secondOfMiunte, $millisOfDay, $millisOfSecond " +
      "from input")
    t.collect.foreach { r =>
      val o = r.getString(0)
      val d : DateTime = r.getAs[SparkDateTime](1)
      val millisVal : Long = r.getLong(2)
      val timeZoneIdVal : String = r.getString(3)
      val eraVal : Int = r.getInt(4)
      val centuryOfEraVal : Int = r.getInt(5)
      val yearOfEraVal : Int = r.getInt(6)
      val yearOfCenturyVal : Int = r.getInt(7)
      val yearVal : Int = r.getInt(8)
      val weekyearVal : Int = r.getInt(9)
      val monthOfYearVal : Int = r.getInt(10)
      val monthOfYearNameVal : String = r.getString(11)
      val weekOfWeekyearVal : Int = r.getInt(12)
      val dayOfYearVal : Int = r.getInt(13)
      val dayOfMonthVal : Int = r.getInt(14)
      val dayOfWeekVal : Int = r.getInt(15)
      val dayOfWeekNameVal : String = r.getString(16)
      val hourOfDayVal : Int = r.getInt(17)
      val minuteOfDayVal : Int = r.getInt(18)
      val secondOfDayVal : Int = r.getInt(19)
      val secondOfMiunteVal : Int = r.getInt(20)
      val millisOfDayVal : Int = r.getInt(21)
      val millisOfSecondVal : Int = r.getInt(22)

      val oDt = DateTime.parse(o).withZone(DateTimeZone.UTC)
      assert(oDt == d)
      assert(oDt.getMillis == millisVal)
      assert(oDt.getZone.getID == timeZoneIdVal)
      assert(oDt.getEra == eraVal)
      assert(oDt.getCenturyOfEra == centuryOfEraVal)
      assert(oDt.getYearOfEra == yearOfEraVal)
      assert(oDt.getYearOfCentury == yearOfCenturyVal)
      assert(oDt.getYear == yearVal)
      assert(oDt.getWeekyear == weekyearVal)
      assert(oDt.getMonthOfYear == monthOfYearVal)
      assert(oDt.month.getAsText == monthOfYearNameVal)
      assert(oDt.getWeekOfWeekyear == weekOfWeekyearVal)
      assert(oDt.getDayOfYear == dayOfYearVal)
      assert(oDt.getDayOfMonth == dayOfMonthVal)
      assert(oDt.getDayOfWeek == dayOfWeekVal)
      assert(oDt.dayOfWeek().getAsText == dayOfWeekNameVal)
      assert(oDt.getHourOfDay == hourOfDayVal)
      assert(oDt.getMinuteOfDay == minuteOfDayVal)
      assert(oDt.getSecondOfDay == secondOfDayVal)
      assert(oDt.getSecondOfMinute == secondOfMiunteVal)
      assert(oDt.getMillisOfDay == millisOfDayVal)
      assert(oDt.getMillisOfSecond == millisOfSecondVal)
    }

  }

}
