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

import scala.language.implicitConversions
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.apache.spark.sql.types.StringType
import com.github.nscala_time.time.Imports._

package object dsl {

  private def fun(nm: String, args: Expression*) = new UnresolvedFunction(nm, args)

  private def toSQL(expr: Any): String = expr match {
    case l@Literal(_, StringType) if l.value != null => s""""$l""""
    case f: UnresolvedFunction => {
      val args = f.children.map(toSQL(_)).mkString("(", ",", ")")
      s"${f.name}$args"
    }
    case a: UnresolvedAttribute => s"`${a.name}`"
    case de: DateExpression => toSQL(de.expr)
    case e: Expression => e.toString()
  }

  implicit class DateExpression private[dsl](val expr: Expression) {

    def millis: Expression = fun("millis", expr)

    def timeZoneId: Expression = fun("timeZoneId", expr)

    def dateIsEqualNow: Expression = fun("dateIsEqualNow", expr)

    def dateIsBeforeNow: Expression = fun("dateIsBeforeNow", expr)

    def dateIsAfterNow: Expression = fun("dateIsAfterNow", expr)

    def dateIsBeforeOrEqualNow: Expression = fun("dateIsBeforeOrEqualNow", expr)

    def dateIsAfterOrEqualNow: Expression = fun("dateIsAfterOrEqualNow", expr)

    def datePlus: Expression = fun("datePlus", expr)

    def dateMinus: Expression = fun("dateMinus", expr)

    def era: Expression = fun("era", expr)

    def centuryOfEra: Expression = fun("centuryOfEra", expr)

    def yearOfEra: Expression = fun("yearOfEra", expr)

    def yearOfCentury: Expression = fun("yearOfCentury", expr)

    def year: Expression = fun("year", expr)

    def weekyear: Expression = fun("weekyear", expr)

    def monthOfYear: Expression = fun("monthOfYear", expr)

    def monthOfYearName: Expression = fun("monthOfYearName", expr)

    def weekOfWeekyear: Expression = fun("weekOfWeekyear", expr)

    def dayOfYear: Expression = fun("dayOfYear", expr)

    def dayOfMonth: Expression = fun("dayOfMonth", expr)

    def dayOfWeek: Expression = fun("dayOfWeek", expr)

    def dayOfWeekName: Expression = fun("dayOfWeekName", expr)

    def hourOfDay: Expression = fun("hourOfDay", expr)

    def minuteOfDay: Expression = fun("minuteOfDay", expr)

    def secondOfDay: Expression = fun("secondOfDay", expr)

    def secondOfMinute: Expression = fun("secondOfMinute", expr)

    def millisOfDay: Expression = fun("millisOfDay", expr)

    def millisOfSecond: Expression = fun("millisOfSecond", expr)

    def withEra(era: Int): DateExpression = new DateExpression(fun("withEra", Literal(era)))

    def withCenturyOfEra(centuryOfEra: Int): DateExpression =
      new DateExpression(fun("withCenturyOfEra",
      Literal(centuryOfEra)))

    def withYearOfEra(yearOfEra: Int): DateExpression =
      new DateExpression(fun("withYearOfEra", Literal(yearOfEra)))

    def withYearOfCentury(yearOfCentury: Int): DateExpression =
      new DateExpression(fun("withYearOfCentury", Literal(yearOfCentury)))

    def withYear(year: Int): DateExpression = new DateExpression(fun("withYear", Literal(year)))

    def withWeekyear(weekyear: Int): DateExpression =
      new DateExpression(fun("withWeekyear", Literal(weekyear)))

    def withMonthOfYear(monthOfYear: Int): DateExpression =
      new DateExpression(fun("withMonthOfYear",
      Literal(monthOfYear)))

    def withWeekOfWeekyear(weekOfWeekyear: Int): DateExpression =
      new DateExpression(fun("withWeekOfWeekyear", Literal(weekOfWeekyear)))

    def withDayOfYear(dayOfYear: Int): DateExpression = new DateExpression(fun("withDayOfYearyear",
      Literal(dayOfYear)))

    def withDayOfMonth(dayOfMonth: Int): DateExpression = new DateExpression(fun("withDayOfMonth",
      Literal(dayOfMonth)))

    def withDayOfWeek(dayOfWeek: Int): DateExpression =
      new DateExpression(fun("withDayOfWeek", Literal(dayOfWeek)))

    def withHourOfDay(hourOfDay: Int): DateExpression =
      new DateExpression(fun("withHourOfDay", Literal(hourOfDay)))

    def withMinuteOfHour(minute: Int): DateExpression =
      new DateExpression(fun("withMinuteOfHour", Literal(minute)))

    def withSecondOfMinute(second: Int): DateExpression =
      new DateExpression(fun("withSecondOfMinute", Literal(second)))

    def withMillisOfDay(millisOfDay: Int): DateExpression =
      new DateExpression(fun("withMillisOfDay", Literal(millisOfDay)))

    def withMillisOfSecond(millis: Int): DateExpression =
      new DateExpression(fun("withMillisOfSecond", Literal(millis)))


    def + (p: PeriodExpression) = new DateExpression(fun("datePlus", expr, p.expr))

    def - (p: PeriodExpression) = new DateExpression(fun("dateMinus", expr, p.expr))

    def ===(dE: DateExpression) = fun("dateIsEqual", expr, dE.expr)

    def >(dE: DateExpression) = fun("dateIsAfter", expr, dE.expr)

    def <(dE: DateExpression) = fun("dateIsBefore", expr, dE.expr)

    def >=(dE: DateExpression) = fun("dateIsAfterOrEqual", expr, dE.expr)

    def <=(dE: DateExpression) = fun("dateIsBeforeOrEqual", expr, dE.expr)

  }

  implicit class PeriodExpression private[dsl](val p: Period) {
    val expr = fun("period", Literal(p.toString))
  }

  // scalastyle:off
  object expressions {

    implicit def dateExpressionToExpression(dE: DateExpression) = dE.expr

    def dateTime(e: Expression): DateExpression = fun("dateTime", e)

    def dateTimeWithTZ(e: Expression): DateExpression = fun("dateTimeWithTZ", e)

    def dateTimeFromEpoch(e: Expression): DateExpression = fun("dateTimeFromEpoch", e)

    implicit class DateExpressionToSQLHelper(val sc: StringContext) {
      // Todo can this be done? (comment from sql dsl)
      // Note that if we make ExpressionConversions an object rather than a trait, we can
      // then make this a value class to avoid the small penalty of runtime instantiation.
      def date(args: Any*): String = {
        val sqls = args.map(toSQL(_))
        sc.s(sqls: _*)
      }
    }

  }

}
