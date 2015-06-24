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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[SparkDateTimeUDT])
case class SparkDateTime(millis : Long, tzId : String)

class SparkDateTimeUDT extends UserDefinedType[SparkDateTime] {

  override def sqlType: DataType =
    StructType(Seq(StructField("millis", LongType), StructField("tz", StringType)))

  override def serialize(obj: Any): Row = {
    obj match {
      case dt: SparkDateTime =>
        val row = new GenericMutableRow(2)
        row.setLong(0, dt.millis)
        row.setString(1, dt.tzId)
        row
    }
  }

  override def deserialize(datum: Any): SparkDateTime = {
    datum match {
      case row: Row =>
        require(row.length == 2,
          s"SparkDateTimeUDT.deserialize given row with length ${row.length} " +
            s"but requires length == 2")
        SparkDateTime(row.getLong(0), row.getString(1))
    }
  }

  override def userClass: Class[SparkDateTime] = classOf[SparkDateTime]

  override def asNullable: SparkDateTimeUDT = this
}

@SQLUserDefinedType(udt = classOf[SparkPeriodUDT])
case class SparkPeriod(periodIsoStr : String)

class SparkPeriodUDT extends UserDefinedType[SparkPeriod] {

  override def sqlType: DataType = StringType


  override def serialize(obj: Any): Any = {
    obj match {
      case p: SparkPeriod =>
        CatalystTypeConverters.convertToCatalyst(p.periodIsoStr)
    }
  }

  override def deserialize(datum: Any): SparkPeriod = {
    datum match {
      case s : UTF8String =>
        SparkPeriod(s.toString())
    }
  }

  override def userClass: Class[SparkPeriod] = classOf[SparkPeriod]

  override def asNullable: SparkPeriodUDT = this
}

@SQLUserDefinedType(udt = classOf[SparkIntervalUDT])
case class SparkInterval(intervalIsoStr : String)

class SparkIntervalUDT extends UserDefinedType[SparkInterval] {

  override def sqlType: DataType = StringType


  override def serialize(obj: Any): Any = {
    obj match {
      case i: SparkInterval =>
        CatalystTypeConverters.convertToCatalyst(i.intervalIsoStr)
    }
  }

  override def deserialize(datum: Any): SparkInterval = {
    datum match {
      case s : UTF8String =>
        SparkInterval(s.toString())
    }
  }

  override def userClass: Class[SparkInterval] = classOf[SparkInterval]

  override def asNullable: SparkIntervalUDT = this
}
