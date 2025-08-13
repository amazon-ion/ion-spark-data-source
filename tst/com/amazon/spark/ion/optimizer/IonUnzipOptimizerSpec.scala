// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import com.amazon.ion.IonValue
import com.amazon.ion.system.{IonBinaryWriterBuilder, IonSystemBuilder}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec

import java.io.ByteArrayOutputStream
class IonUnzipOptimizerSpec extends AnyFlatSpec with IonUnzipOptimizerBehavior {

  private val ionSystem = IonSystemBuilder.standard().build()
  private val ionWriterBuilder = IonBinaryWriterBuilder.standard()

  private def fixture =
    new {
      val zippedColumn = "other_product_attributes"
      val dfSchema: StructType = StructType(
        Seq(
          StructField("marketplace_id", LongType),
          StructField("item_id", StringType),
          StructField("domain_id", LongType),
          StructField(zippedColumn, BinaryType)
        ))
    }

  trait Input {

    val data: Seq[Row] = {
      val ionValues = getIonColumnValues

      Seq(
        Row(1L, "asin1", 11111L, getIonBinary(ionValues._1)),
        Row(2L, "asin2", 22222L, getIonBinary(ionValues._2)),
        Row(1L, "asin3", 33333L, getIonBinary(ionValues._3)),
        Row(4L, "asin4", 11111L, null)
      )
    }

    def getIonColumnValues: (IonValue, IonValue, IonValue)

    private def getIonBinary(ionVal: IonValue): Array[Byte] = {
      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()

      val ionWriter = ionWriterBuilder.build(stream)

      ionVal.writeTo(ionWriter)

      ionWriter.finish()
      ionWriter.close()

      stream.toByteArray
    }
  }

  trait NestedInput extends Input {

    override def getIonColumnValues: (IonValue, IonValue, IonValue) = {
      val ionValue1 = ionSystem.singleValue("{version:1, submission_id:1, child_customer_id:1, attr1:{field1:\"foo\"}}")
      val ionValue2 = ionSystem.singleValue(
        "{version:2, submission_id:2, child_customer_id:2, attr1:{field1:\"foo\"}, attr2:{field1:\"foo\"}}")
      val ionValue3 = ionSystem.singleValue("{version:2, submission_id:2, child_customer_id:3, attr2:{field1:\"foo\"}}")

      (ionValue1, ionValue2, ionValue3)
    }

    val nestedZippedSchema: StructType = StructType(
      Seq(
        StructField("version", LongType),
        StructField("submission_id", LongType),
        StructField("child_customer_id", LongType),
        StructField("attr1",
                    StructType(
                      Seq(
                        StructField("field1", StringType)
                      )))
      ))
  }

  trait IonMismatch extends Input {

    override def getIonColumnValues: (IonValue, IonValue, IonValue) = {
      val ionValue1 = ionSystem.singleValue("{version:1, submission_id:1, child_customer_id:1, attr1:{field1:\"foo\"}}")
      val ionValue2 = ionSystem.singleValue(
        "{version:2, submission_id:2, child_customer_id:2, attr1:{field1:\"foo\"}, attr2:{field1:\"foo\"}}")
      val ionValue3 = ionSystem.singleValue("{version:2, submission_id:2, child_customer_id:3, attr2:{field1:\"foo\"}}")

      (ionValue1, ionValue2, ionValue3)
    }

    val nestedZippedSchema: StructType = StructType(
      Seq(
        StructField("version", LongType),
        StructField("submission_id", StringType),
        StructField("child_customer_id", LongType),
        StructField("attr1",
                    StructType(
                      Seq(
                        StructField("field1", StringType)
                      )))
      ))
  }

  "Zipped Nested Binary Data" should behave like new NestedInput {
    private val f = fixture

    import f._

    unzip(data, dfSchema, nestedZippedSchema, zippedColumn)
  }

  "Mismatching Ion Binary Data" should behave like new IonMismatch {
    private val f = fixture

    import f._

    unzip(data, dfSchema, nestedZippedSchema, zippedColumn)
  }

}
