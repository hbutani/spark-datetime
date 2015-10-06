package org.sparklinedata.spark.dateTime2

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[SparkDateTimeUDT])
case class SparkDateTime2(millis : Long, tzId : String)

class SparkDateTimeUDT extends UserDefinedType[SparkDateTime2] {

  override def sqlType: DataType =
    StructType(Seq(StructField("millis", LongType), StructField("tz", StringType)))

  override def serialize(obj: Any): Any = SparkDateTimeUDT._serialize(obj)

  override def deserialize(datum: Any): SparkDateTime2 = SparkDateTimeUDT._deserialize(datum)

  override def userClass: Class[SparkDateTime2] = classOf[SparkDateTime2]

  override def asNullable: SparkDateTimeUDT = this
}

object SparkDateTimeUDT {

  def _serialize(obj: Any): Any = {
    obj match {
      case dt: SparkDateTime2 =>
        val row = new Array[Any](2)
        row(0) = dt.millis
        row(1) = CatalystTypeConverters.convertToCatalyst(dt.tzId)
        row
    }
  }

  def _deserialize(datum: Any): SparkDateTime2 = {
    datum match {
      case row: Array[Any] =>
        require(row.size == 2,
          s"SparkDateTimeUDT.deserialize given row with length ${row.size} " +
            s"but requires length == 2")
        SparkDateTime2(row(0).asInstanceOf[Long], row(1).asInstanceOf[UTF8String].toString)
      case x => throw new RuntimeException("oh shit")
    }
  }

}
