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

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.TestSQLContext._
import org.sparklinedata.spark.dateTime.Functions._
import org.sparklinedata.spark.dateTime.dsl.expressions._

import scala.language.postfixOps

class IssuesTest extends BaseTest {

  test("issue2") {

    val dT = dateTime('dt)
    val dT1 = dateTime('dt) - 8.hour

    val t = sql(date"select dt, $dT, $dT1 from input")

    t.collect.foreach { r =>
      val o = r.getString(0)
      val d : DateTime = r.getAs[SparkDateTime](1)
      val d1 : DateTime = r.getAs[SparkDateTime](2)

      val oDt = DateTime.parse(o).withZone(DateTimeZone.UTC)
      assert(oDt == d)
      val oDt1 = DateTime.parse(o).withZone(DateTimeZone.UTC) - 8.hour
      assert(oDt1 == d1)
    }
  }
}
