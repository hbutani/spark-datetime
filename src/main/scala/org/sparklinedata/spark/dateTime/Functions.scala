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

import java.io.Serializable

import org.joda.time.field.FieldUtils

import scala.language.implicitConversions
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

/**
 * Expose all the functions in [[DateTime]] and the concepts defined
 * [[http://joda-time.sourceforge.net/field.html here]]
 *
 * =function categories=
 *  - '''field access:''' all functions in the [[DateTime]] are available as sql functions.
 *  The first argument is the
 *  DateTime object on which the function is to be applied.
 *  - '''construction:''' functions are available to convert a String or a epoch value to DateTime
 *  - '''comparison:''' functions available to compare dates (=, <, <=, >, >=), also compare
 *  against __now__.
 *  - '''arithmetic:''' functions available to add/subtract [[Period]] from dates.
 */
object Functions {

  implicit def dateTimeToSpark(dt: DateTime) = SparkDateTime(dt.getMillis,
    dt.getChronology.getZone.getID)

  implicit def sparkToDateTime(dt: SparkDateTime) =
    new DateTime(dt.millis).withZone(DateTimeZone.forID(dt.tzId))

  implicit def periodToSpark(p: Period) = SparkPeriod(p.toString)

  implicit def sparkToPeriod(sp: SparkPeriod) = Period.parse(sp.periodIsoStr)

  implicit def sparkToInterval(si : SparkInterval) = Interval.parse(si.intervalIsoStr)

  implicit def intervalToSpark(i : Interval) = SparkInterval(i.toString)

  object dateTimeFromEpochFn extends Function1[Long, SparkDateTime] {
    def apply(l: Long): SparkDateTime = new DateTime(l)
  }

  object dateTimeWithFormatFn extends Function2[String, String, SparkDateTime] with Serializable {
    def apply(s: String, pattern: String): SparkDateTime =
      parseDate(s, DateTimeFormat.forPattern(pattern)).withZone(DateTimeZone.UTC)
  }

  object dateTimeFn extends Function1[String, SparkDateTime] with Serializable {
    def apply(s: String): SparkDateTime = parseDate(s).withZone(DateTimeZone.UTC)
  }

  object dateTimeWithTZFn extends Function1[String, SparkDateTime] with Serializable {
    def apply(s: String): SparkDateTime = parseDate(s)
  }

  object dateTimeWithFormatAndTZFn extends Function2[String, String, SparkDateTime]
  with Serializable {
    def apply(s: String, pattern: String): SparkDateTime =
      parseDate(s, DateTimeFormat.forPattern(pattern))
  }

  object periodFn extends Function1[String, SparkPeriod] with Serializable {
    def apply(s: String): SparkPeriod = SparkPeriod(s)
  }

  object millisFn extends Function1[SparkDateTime, Long] with Serializable {
    def apply(dT: SparkDateTime): Long = dT.getMillis
  }

  object timeZoneIdFn extends Function1[SparkDateTime, String] with Serializable {
    def apply(dT : SparkDateTime) : String = dT.getZone.getID
  }

  object withZoneFn extends Function2[SparkDateTime, String, SparkDateTime] with Serializable {
    /**
     *
     * @param dt
     * @param tzId from [[https://en.wikipedia.org/wiki/List_of_tz_database_time_zones]]
     * @return
     */
    def apply(dt: SparkDateTime, tzId: String): SparkDateTime =
      dt.withZone(DateTimeZone.forID(tzId))
  }

  object dateIsEqualNowFn extends Function1[SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime) : Boolean = dt1.isEqualNow
  }

  object dateIsBeforeNowFn extends Function1[SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime) : Boolean = dt1.isBeforeNow
  }

  object dateIsAfterNowFn extends Function1[SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime) = dt1.isAfterNow
  }

  object dateIsBeforeOrEqualNowFn extends Function1[SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime) = {
      val d1: DateTime = dt1
      d1.isBeforeNow || d1.isEqualNow
    }
  }

  object dateIsAfterOrEqualNowFn extends Function1[SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime) = {
      val d1: DateTime = dt1
      d1.isAfterNow || d1.isEqualNow
    }
  }

  object dateIsEqualFn extends Function2[SparkDateTime, SparkDateTime, Boolean] with Serializable {
    def apply(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isEqual(dt2)
  }

  object dateIsBeforeFn extends Function2[SparkDateTime, SparkDateTime, Boolean]
  with Serializable {
    def apply(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isBefore(dt2)
  }

  object dateIsAfterFn extends Function2[SparkDateTime, SparkDateTime, Boolean]
  with Serializable {
    def apply(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isAfter(dt2)
  }

  object dateIsBeforeOrEqualFn extends Function2[SparkDateTime, SparkDateTime, Boolean]
  with Serializable {
    def apply(dt1: SparkDateTime, dt2: SparkDateTime) = {
      val d1: DateTime = dt1
      val d2: DateTime = dt2
      d1.isBefore(d2) || d1.isEqual(d2)
    }
  }

  object dateIsAfterOrEqualFn extends Function2[SparkDateTime, SparkDateTime, Boolean]
  with Serializable {
    def apply(dt1: SparkDateTime, dt2: SparkDateTime) = {
      val d1: DateTime = dt1
      val d2: DateTime = dt2
      d1.isAfter(d2) || d1.isEqual(d2)
    }
  }

  object datePlusFn extends Function2[SparkDateTime, SparkPeriod, SparkDateTime]
  with Serializable {
    def apply(dt: SparkDateTime, p: SparkPeriod): SparkDateTime = dt.plus(p)
  }

  object dateMinusFn extends Function2[SparkDateTime, SparkPeriod, SparkDateTime]
  with Serializable {
    def apply(dt: SparkDateTime, p: SparkPeriod): SparkDateTime = dt.minus(p)
  }

  object eraFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply (dT: SparkDateTime): Int = dT.getEra
  }

  object centuryOfEraFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getCenturyOfEra
  }

  object yearOfEraFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getYearOfEra
  }

  object yearOfCenturyFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getYearOfCentury
  }

  object yearFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getYear
  }

  object weekyearFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getWeekyear
  }

  object monthOfYearFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getMonthOfYear
  }

  object monthOfYearNameFn extends Function1[SparkDateTime, String] with Serializable {
    def apply(dT: SparkDateTime): String = dT.monthOfYear().getAsText
  }

  object weekOfWeekyearFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getWeekOfWeekyear
  }

  object dayOfYearFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getDayOfYear
  }

  object dayOfMonthFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getDayOfMonth
  }

  object dayOfWeekFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getDayOfWeek
  }

  object dayOfWeekNameFn extends Function1[SparkDateTime, String] with Serializable {
    def apply(dT: SparkDateTime): String = dT.dayOfWeek().getAsText
  }

  object hourOfDayFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getHourOfDay
  }

  object minuteOfDayFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getMinuteOfDay
  }

  object secondOfDayFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getSecondOfDay
  }

  object secondOfMinuteFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getSecondOfMinute
  }

  object millisOfDayFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getMillisOfDay
  }

  object millisOfSecondFn extends Function1[SparkDateTime, Int] with Serializable {
    def apply(dT: SparkDateTime): Int = dT.getMillisOfSecond
  }

  object withEraFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, era: Int): SparkDateTime = dT.withEra(era)
  }

  object withCenturyOfEraFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, centuryOfEra: Int): SparkDateTime =
      dT.withCenturyOfEra(centuryOfEra)
  }

  object withYearOfEraFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, yearOfEra: Int): SparkDateTime =
      dT.withYearOfEra(yearOfEra)
  }

  object withYearOfCenturyFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, yearOfCentury: Int): SparkDateTime =
      dT.withYearOfCentury(yearOfCentury)
  }

  object withYearFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, year: Int): SparkDateTime = dT.withYear(year)
  }

  object withWeekyearFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, weekyear: Int): SparkDateTime =
      dT.withWeekyear(weekyear)
  }

  object withMonthOfYearFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, monthOfYear: Int): SparkDateTime =
      dT.withMonthOfYear(monthOfYear)
  }

  object withWeekOfWeekyearFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, weekOfWeekyear: Int): SparkDateTime =
      dT.withWeekOfWeekyear(weekOfWeekyear)
  }

  object withDayOfYearFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, dayOfYear: Int): SparkDateTime =
      dT.withDayOfYear(dayOfYear)
  }

  object withDayOfMonthFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, dayOfMonth: Int): SparkDateTime =
      dT.withDayOfMonth(dayOfMonth)
  }

  object withDayOfWeekFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, dayOfWeek: Int): SparkDateTime =
      dT.withDayOfWeek(dayOfWeek)
  }

  object withHourOfDayFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, hourOfDay: Int): SparkDateTime =
      dT.withHourOfDay(hourOfDay)
  }

  object withMinuteOfHourFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, minute: Int): SparkDateTime =
      dT.withMinuteOfHour(minute)
  }

  object withSecondOfMinuteFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, second: Int): SparkDateTime =
      dT.withSecondOfMinute(second)
  }

  object withMillisOfDayFn extends Function2[SparkDateTime, Int, SparkDateTime] with Serializable {
    def apply(dT: SparkDateTime, millisOfDay: Int): SparkDateTime =
      dT.withMillisOfDay(millisOfDay)
  }

  object withMillisOfSecondFn extends Function2[SparkDateTime, Int, SparkDateTime]
  with Serializable {
    def apply(dT: SparkDateTime, millis: Int): SparkDateTime =
      dT.withMillisOfSecond(millis)
  }

  // interval functions
  object intervalFn extends Function2[SparkDateTime, SparkDateTime, SparkInterval]
  with Serializable {
    def apply(from: SparkDateTime, to: SparkDateTime): SparkInterval =
      SparkInterval(new Interval(from.getMillis, to.getMillis).toString)
  }

  object intervalFromStrFn extends Function1[String, SparkInterval] with Serializable {
    def apply(s: String): SparkInterval = SparkInterval(s)
  }

  object intervalContainsDateTimeFn extends Function2[SparkInterval, SparkDateTime, Boolean]
  with Serializable {
    def apply(si: SparkInterval, dt: SparkDateTime): Boolean =
      si.contains(dt)
  }

  object intervalContainsIntervalFn extends Function2[SparkInterval, SparkInterval, Boolean]
  with Serializable {
    def apply(si1: SparkInterval, si2: SparkInterval): Boolean =
      si1.contains(si2)
  }

  object intervalOverlapsFn extends Function2[SparkInterval, SparkInterval, Boolean]
  with Serializable {
    def apply(si1: SparkInterval, si2: SparkInterval): Boolean = si1.overlaps(si2)
  }

  object intervalAbutsFn extends Function2[SparkInterval, SparkInterval, Boolean]
  with Serializable {
    def apply(si1: SparkInterval, si2: SparkInterval): Boolean = si1.abuts(si2)
  }

  object intervalIsBeforeFn extends Function2[SparkInterval, SparkDateTime, Boolean]
  with Serializable {
    def apply(si: SparkInterval, dt: SparkDateTime): Boolean = si.isBefore(dt)
  }

  object intervalIsAfterFn extends Function2[SparkInterval, SparkDateTime, Boolean]
  with Serializable {
    def apply(si: SparkInterval, dt: SparkDateTime): Boolean = si.isAfter(dt)
  }

  object intervalStartFn extends Function1[SparkInterval, SparkDateTime] with Serializable {
    def apply(si: SparkInterval): SparkDateTime = si.getStart
  }

  object intervalEndFn extends Function1[SparkInterval, SparkDateTime] with Serializable {
    def apply(si: SparkInterval): SparkDateTime = si.getEnd
  }

  object intervalGapFn extends Function2[SparkInterval,SparkInterval, SparkInterval]
  with Serializable {
    def apply(si1: SparkInterval, si2: SparkInterval): SparkInterval = si1.gap(si2)
  }

  object timeBucketFn extends Function3[SparkDateTime, SparkDateTime, SparkPeriod, Long]
  with Serializable {
    /**
     * Bucket the input dates dt into specified Periods p based on their distance from the origin
     * @param dt
     * @param origin
     * @param p
     * @return
     */
    def apply(dt: SparkDateTime, origin: SparkDateTime, p: SparkPeriod): Long = {
      val d0: Duration = p.toStandardDuration
      val d1: Duration = new Period(origin, dt).toStandardDuration
      FieldUtils.safeDivide(d1.getMillis, d0.getMillis)
    }
  }

  def register(implicit sqlContext: SQLContext) = {

    sqlContext.udf.register("dateTimeFromEpoch", dateTimeFromEpochFn)

    sqlContext.udf.register("dateTime", dateTimeFn)

    sqlContext.udf.register("dateTimeWithFormat", dateTimeWithFormatFn)

    sqlContext.udf.register("dateTimeWithTZ", dateTimeWithTZFn)

    sqlContext.udf.register("dateTimeWithFormatAndTZFn", dateTimeWithFormatAndTZFn)

    sqlContext.udf.register("period", periodFn)

    sqlContext.udf.register("millis", millisFn)

    sqlContext.udf.register("timeZoneId", timeZoneIdFn)

    sqlContext.udf.register("withZone", withZoneFn)

    sqlContext.udf.register("dateIsEqualNow", dateIsEqualNowFn)

    sqlContext.udf.register("dateIsBeforeNow", dateIsBeforeNowFn)

    sqlContext.udf.register("dateIsAfterNow", dateIsAfterNowFn)

    sqlContext.udf.register("dateIsBeforeOrEqualNow", dateIsBeforeOrEqualNowFn)

    sqlContext.udf.register("dateIsAfterOrEqualNow", dateIsAfterOrEqualNowFn)

    sqlContext.udf.register("dateIsEqual", dateIsEqualFn)

    sqlContext.udf.register("dateIsBefore", dateIsBeforeFn)

    sqlContext.udf.register("dateIsAfter", dateIsAfterFn)

    sqlContext.udf.register("dateIsBeforeOrEqual", dateIsBeforeOrEqualFn)

    sqlContext.udf.register("dateIsAfterOrEqual", dateIsAfterOrEqualFn)

    sqlContext.udf.register("datePlus", datePlusFn)

    sqlContext.udf.register("dateMinus", dateMinusFn)

    sqlContext.udf.register("era", eraFn)

    sqlContext.udf.register("centuryOfEra", centuryOfEraFn)

    sqlContext.udf.register("yearOfEra", yearOfEraFn)

    sqlContext.udf.register("yearOfCentury", yearOfCenturyFn)

    sqlContext.udf.register("year", yearFn)

    sqlContext.udf.register("weekyear", weekyearFn)

    sqlContext.udf.register("monthOfYear", monthOfYearFn)

    sqlContext.udf.register("monthOfYearName", monthOfYearNameFn)

    sqlContext.udf.register("weekOfWeekyear", weekOfWeekyearFn)

    sqlContext.udf.register("dayOfYear", dayOfYearFn)

    sqlContext.udf.register("dayOfMonth", dayOfMonthFn)

    sqlContext.udf.register("dayOfWeek", dayOfWeekFn)

    sqlContext.udf.register("dayOfWeekName", dayOfWeekNameFn)

    sqlContext.udf.register("hourOfDay", hourOfDayFn)

    sqlContext.udf.register("minuteOfDay", minuteOfDayFn)

    sqlContext.udf.register("secondOfDay", secondOfDayFn)

    sqlContext.udf.register("secondOfMinute", secondOfMinuteFn)

    sqlContext.udf.register("millisOfDay", millisOfDayFn)

    sqlContext.udf.register("millisOfSecond", millisOfSecondFn)

    sqlContext.udf.register("withEra", withEraFn)

    sqlContext.udf.register("withCenturyOfEra", withCenturyOfEraFn)

    sqlContext.udf.register("withYearOfEra", withYearOfEraFn)

    sqlContext.udf.register("withYearOfCentury", withYearOfCenturyFn)

    sqlContext.udf.register("withYear", withYearFn)

    sqlContext.udf.register("withWeekyear", withWeekyearFn)

    sqlContext.udf.register("withMonthOfYear", withMonthOfYearFn)

    sqlContext.udf.register("withWeekOfWeekyear", withWeekOfWeekyearFn)

    sqlContext.udf.register("withDayOfYear", withDayOfYearFn)

    sqlContext.udf.register("withDayOfMonth", withDayOfMonthFn)

    sqlContext.udf.register("intervalFn", intervalFn)

    sqlContext.udf.register("intervalFromStr", intervalFromStrFn)

    sqlContext.udf.register("intervalContainsDateTime", intervalContainsDateTimeFn)

    sqlContext.udf.register("intervalContainsInterval", intervalContainsIntervalFn)

    sqlContext.udf.register("intervalOverlaps", intervalOverlapsFn)

    sqlContext.udf.register("intervalAbuts", intervalAbutsFn)

    sqlContext.udf.register("intervalIsBefore", intervalIsBeforeFn)

    sqlContext.udf.register("intervalIsAfter", intervalIsAfterFn)

    sqlContext.udf.register("intervalStart", intervalStartFn)

    sqlContext.udf.register("intervalEnd", intervalEndFn)

    sqlContext.udf.register("intervalGap", intervalGapFn)

    sqlContext.udf.register("timeBucket", timeBucketFn)

  }

  def parseDate(s: String): DateTime = DateTime.parse(s)

  def parseDate(s: String, f : DateTimeFormatter): DateTime = DateTime.parse(s, f)

}
