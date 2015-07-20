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

  def dateTimeFromEpochFn(l : Long) : SparkDateTime = new DateTime(l)

  def dateTimeWithFormatFn(s: String, pattern : String): SparkDateTime =
    parseDate(s, DateTimeFormat.forPattern(pattern)).withZone(DateTimeZone.UTC)

  def dateTimeFn(s: String): SparkDateTime = parseDate(s).withZone(DateTimeZone.UTC)

  def dateTimeWithTZFn(s: String): SparkDateTime = parseDate(s)

  def dateTimeWithFormatAndTZFn(s: String, pattern : String): SparkDateTime =
    parseDate(s, DateTimeFormat.forPattern(pattern))

  def periodFn(s: String): SparkPeriod = SparkPeriod(s)

  def millisFn(dT : SparkDateTime) : Long = dT.getMillis

  def timeZoneIdFn(dT : SparkDateTime) : String = dT.getZone.getID

  /**
   *
   * @param dt
   * @param tzId from [[https://en.wikipedia.org/wiki/List_of_tz_database_time_zones]]
   * @return
   */
  def withZoneFn(dt : SparkDateTime, tzId : String) : SparkDateTime =
    dt.withZone(DateTimeZone.forID(tzId))

  def dateIsEqualNowFn(dt1: SparkDateTime) : Boolean = dt1.isEqualNow

  def dateIsBeforeNowFn(dt1: SparkDateTime) : Boolean = dt1.isBeforeNow

  def dateIsAfterNowFn(dt1: SparkDateTime) = dt1.isAfterNow

  def dateIsBeforeOrEqualNowFn(dt1: SparkDateTime) = {
    val d1: DateTime = dt1
    d1.isBeforeNow || d1.isEqualNow
  }

  def dateIsAfterOrEqualNowFn(dt1: SparkDateTime) = {
    val d1: DateTime = dt1
    d1.isAfterNow || d1.isEqualNow
  }

  def dateIsEqualFn(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isEqual(dt2)

  def dateIsBeforeFn(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isBefore(dt2)

  def dateIsAfterFn(dt1: SparkDateTime, dt2: SparkDateTime) = dt1.isAfter(dt2)

  def dateIsBeforeOrEqualFn(dt1: SparkDateTime, dt2: SparkDateTime) = {
    val d1: DateTime = dt1
    val d2: DateTime = dt2
    d1.isBefore(d2) || d1.isEqual(d2)
  }

  def dateIsAfterOrEqualFn(dt1: SparkDateTime, dt2: SparkDateTime) = {
    val d1: DateTime = dt1
    val d2: DateTime = dt2
    d1.isAfter(d2) || d1.isEqual(d2)
  }

  def datePlusFn(dt: SparkDateTime, p: SparkPeriod): SparkDateTime = dt.plus(p)

  def dateMinusFn(dt: SparkDateTime, p: SparkPeriod): SparkDateTime = dt.minus(p)

  def eraFn(dT: SparkDateTime): Int = dT.getEra

  def centuryOfEraFn(dT: SparkDateTime): Int = dT.getCenturyOfEra

  def yearOfEraFn(dT: SparkDateTime): Int = dT.getYearOfEra

  def yearOfCenturyFn(dT: SparkDateTime): Int = dT.getYearOfCentury

  def yearFn(dT: SparkDateTime): Int = dT.getYear

  def weekyearFn(dT: SparkDateTime): Int = dT.getWeekyear

  def monthOfYearFn(dT: SparkDateTime): Int = dT.getMonthOfYear

  def monthOfYearNameFn(dT: SparkDateTime): String = dT.monthOfYear().getAsText

  def weekOfWeekyearFn(dT: SparkDateTime): Int = dT.getWeekOfWeekyear

  def dayOfYearFn(dT: SparkDateTime): Int = dT.getDayOfYear

  def dayOfMonthFn(dT: SparkDateTime): Int = dT.getDayOfMonth

  def dayOfWeekFn(dT: SparkDateTime): Int = dT.getDayOfWeek

  def dayOfWeekNameFn(dT: SparkDateTime): String = dT.dayOfWeek().getAsText

  def hourOfDayFn(dT: SparkDateTime): Int = dT.getHourOfDay

  def minuteOfDayFn(dT: SparkDateTime): Int = dT.getMinuteOfDay

  def secondOfDayFn(dT: SparkDateTime): Int = dT.getSecondOfDay

  def secondOfMinuteFn(dT: SparkDateTime): Int = dT.getSecondOfMinute

  def millisOfDayFn(dT: SparkDateTime): Int = dT.getMillisOfDay

  def millisOfSecondFn(dT: SparkDateTime): Int = dT.getMillisOfSecond

  def withEraFn(dT: SparkDateTime, era : Int): SparkDateTime = dT.withEra(era)

  def withCenturyOfEraFn(dT: SparkDateTime, centuryOfEra : Int): SparkDateTime =
    dT.withCenturyOfEra(centuryOfEra)

  def withYearOfEraFn(dT: SparkDateTime, yearOfEra : Int): SparkDateTime =
    dT.withYearOfEra(yearOfEra)

  def withYearOfCenturyFn(dT: SparkDateTime, yearOfCentury : Int): SparkDateTime =
    dT.withYearOfCentury(yearOfCentury)

  def withYearFn(dT: SparkDateTime, year : Int): SparkDateTime = dT.withYear(year)

  def withWeekyearFn(dT: SparkDateTime, weekyear : Int): SparkDateTime =
    dT.withWeekyear(weekyear)

  def withMonthOfYearFn(dT: SparkDateTime, monthOfYear : Int): SparkDateTime =
    dT.withMonthOfYear(monthOfYear)

  def withWeekOfWeekyearFn(dT: SparkDateTime, weekOfWeekyear : Int): SparkDateTime =
    dT.withWeekOfWeekyear(weekOfWeekyear)

  def withDayOfYearFn(dT: SparkDateTime, dayOfYear : Int): SparkDateTime =
    dT.withDayOfYear(dayOfYear)

  def withDayOfMonthFn(dT: SparkDateTime, dayOfMonth : Int): SparkDateTime =
    dT.withDayOfMonth(dayOfMonth)

  def withDayOfWeekFn(dT: SparkDateTime, dayOfWeek : Int): SparkDateTime =
    dT.withDayOfWeek(dayOfWeek)

  def withHourOfDayFn(dT: SparkDateTime, hourOfDay : Int): SparkDateTime =
    dT.withHourOfDay(hourOfDay)

  def withMinuteOfHourFn(dT: SparkDateTime, minute : Int): SparkDateTime =
    dT.withMinuteOfHour(minute)

  def withSecondOfMinuteFn(dT: SparkDateTime, second : Int): SparkDateTime =
    dT.withSecondOfMinute(second)

  def withMillisOfDayFn(dT: SparkDateTime, millisOfDay : Int): SparkDateTime =
    dT.withMillisOfDay(millisOfDay)

  def withMillisOfSecondFn(dT: SparkDateTime, millis : Int): SparkDateTime =
    dT.withMillisOfSecond(millis)

  // interval functions
  def intervalFn(from : SparkDateTime, to : SparkDateTime) : SparkInterval =
    SparkInterval(new Interval(from.getMillis, to.getMillis).toString)

  def intervalFromStrFn(s: String): SparkInterval = SparkInterval(s)

  def intervalContainsDateTimeFn(si : SparkInterval, dt : SparkDateTime) : Boolean =
    si.contains(dt)

  def intervalContainsIntervalFn(si1 : SparkInterval, si2 : SparkInterval) : Boolean =
    si1.contains(si2)

  def intervalOverlapsFn(si1 : SparkInterval, si2 : SparkInterval) : Boolean = si1.overlaps(si2)

  def intervalAbutsFn(si1 : SparkInterval, si2 : SparkInterval) : Boolean = si1.abuts(si2)

  def intervalIsBeforeFn(si : SparkInterval, dt : SparkDateTime) : Boolean = si.isBefore(dt)

  def intervalIsAfterFn(si : SparkInterval, dt : SparkDateTime) : Boolean = si.isAfter(dt)

  def intervalStartFn(si : SparkInterval) : SparkDateTime = si.getStart

  def intervalEndFn(si : SparkInterval) : SparkDateTime = si.getEnd

  def intervalGapFn(si1 : SparkInterval, si2 : SparkInterval) : SparkInterval = si1.gap(si2)

  /**
   * Bucket the input dates dt into specified Periods p based on their distance from the origin
   * @param dt
   * @param origin
   * @param p
   * @return
   */
  def timeBucketFn(dt : SparkDateTime, origin : SparkDateTime, p : SparkPeriod) : Long = {
    val d0 : Duration = p.toStandardDuration
    val d1 : Duration = new Period(origin, dt).toStandardDuration
    FieldUtils.safeDivide(d1.getMillis, d0.getMillis)
  }

  def register(implicit sqlContext: SQLContext) = {

    sqlContext.udf.register("dateTimeFromEpoch", dateTimeFromEpochFn _)

    sqlContext.udf.register("dateTime", dateTimeFn _)

    sqlContext.udf.register("dateTimeWithFormat", dateTimeWithFormatFn _)

    sqlContext.udf.register("dateTimeWithTZ", dateTimeWithTZFn _)

    sqlContext.udf.register("dateTimeWithFormatAndTZFn", dateTimeWithFormatAndTZFn _)

    sqlContext.udf.register("period", periodFn _)

    sqlContext.udf.register("millis", millisFn _)

    sqlContext.udf.register("timeZoneId", timeZoneIdFn _)

    sqlContext.udf.register("withZone", withZoneFn _)

    sqlContext.udf.register("dateIsEqualNow", dateIsEqualNowFn _)

    sqlContext.udf.register("dateIsBeforeNow", dateIsBeforeNowFn _)

    sqlContext.udf.register("dateIsAfterNow", dateIsAfterNowFn _)

    sqlContext.udf.register("dateIsBeforeOrEqualNow", dateIsBeforeOrEqualNowFn _)

    sqlContext.udf.register("dateIsAfterOrEqualNow", dateIsAfterOrEqualNowFn _)

    sqlContext.udf.register("dateIsEqual", dateIsEqualFn _)

    sqlContext.udf.register("dateIsBefore", dateIsBeforeFn _)

    sqlContext.udf.register("dateIsAfter", dateIsAfterFn _)

    sqlContext.udf.register("dateIsBeforeOrEqual", dateIsBeforeOrEqualFn _)

    sqlContext.udf.register("dateIsAfterOrEqual", dateIsAfterOrEqualFn _)

    sqlContext.udf.register("datePlus", datePlusFn _)

    sqlContext.udf.register("dateMinus", dateMinusFn _)

    sqlContext.udf.register("era", eraFn _)

    sqlContext.udf.register("centuryOfEra", centuryOfEraFn _)

    sqlContext.udf.register("yearOfEra", yearOfEraFn _)

    sqlContext.udf.register("yearOfCentury", yearOfCenturyFn _)

    sqlContext.udf.register("year", yearFn _)

    sqlContext.udf.register("weekyear", weekyearFn _)

    sqlContext.udf.register("monthOfYear", monthOfYearFn _)

    sqlContext.udf.register("monthOfYearName", monthOfYearNameFn _)

    sqlContext.udf.register("weekOfWeekyear", weekOfWeekyearFn _)

    sqlContext.udf.register("dayOfYear", dayOfYearFn _)

    sqlContext.udf.register("dayOfMonth", dayOfMonthFn _)

    sqlContext.udf.register("dayOfWeek", dayOfWeekFn _)

    sqlContext.udf.register("dayOfWeekName", dayOfWeekNameFn _)

    sqlContext.udf.register("hourOfDay", hourOfDayFn _)

    sqlContext.udf.register("minuteOfDay", minuteOfDayFn _)

    sqlContext.udf.register("secondOfDay", secondOfDayFn _)

    sqlContext.udf.register("secondOfMinute", secondOfMinuteFn _)

    sqlContext.udf.register("millisOfDay", millisOfDayFn _)

    sqlContext.udf.register("millisOfSecond", millisOfSecondFn _)

    sqlContext.udf.register("withEra", withEraFn _)

    sqlContext.udf.register("withCenturyOfEra", withCenturyOfEraFn _)

    sqlContext.udf.register("withYearOfEra", withYearOfEraFn _)

    sqlContext.udf.register("withYearOfCentury", withYearOfCenturyFn _)

    sqlContext.udf.register("withYear", withYearFn _)

    sqlContext.udf.register("withWeekyear", withWeekyearFn _)

    sqlContext.udf.register("withMonthOfYear", withMonthOfYearFn _)

    sqlContext.udf.register("withWeekOfWeekyear", withWeekOfWeekyearFn _)

    sqlContext.udf.register("withDayOfYear", withDayOfYearFn _)

    sqlContext.udf.register("withDayOfMonth", withDayOfMonthFn _)

    sqlContext.udf.register("interval", intervalFn _)

    sqlContext.udf.register("intervalFromStr", intervalFromStrFn _)

    sqlContext.udf.register("intervalContainsDateTime", intervalContainsDateTimeFn _)

    sqlContext.udf.register("intervalContainsInterval", intervalContainsIntervalFn _)

    sqlContext.udf.register("intervalOverlaps", intervalOverlapsFn _)

    sqlContext.udf.register("intervalAbuts", intervalAbutsFn _)

    sqlContext.udf.register("intervalIsBefore", intervalIsBeforeFn _)

    sqlContext.udf.register("intervalIsAfter", intervalIsAfterFn _)

    sqlContext.udf.register("intervalStart", intervalStartFn _)

    sqlContext.udf.register("intervalEnd", intervalEndFn _)

    sqlContext.udf.register("intervalGap", intervalGapFn _)

    sqlContext.udf.register("timeBucket", timeBucketFn _)

  }

  def parseDate(s: String): DateTime = DateTime.parse(s)

  def parseDate(s: String, f : DateTimeFormatter): DateTime = DateTime.parse(s, f)

}
