// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser
import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.{
  DateType,
  IntegerType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, Literal}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers.{a, convertToAnyShouldWrapper}

import java.sql.{Date, Timestamp}

class CreateIonStructSpec extends AnyFlatSpec with BeforeAndAfterEach {

  private def fixture =
    new {
      val optionsMap = Map("k1" -> "v1", "k2" -> "v2")
      val inputSchema = StructType(
        Seq(
          StructField("marketplace_id", LongType),
          StructField("item_id", StringType),
          StructField("domain_id", LongType),
          StructField("last_buyable_date", DateType),
          StructField("version_time", TimestampType)
        )
      )

      val children = Seq(Literal(9999L),
                         Literal("foo-bar"),
                         Literal(9999L),
                         Literal(Date.valueOf("1970-01-01")),
                         Literal(Timestamp.valueOf("1970-01-01 00:00:00")))

      // using children Seq(Literal) to create ion struct expression. Will use this across the tests.
      val expr = new CreateIonStruct(children, inputSchema, optionsMap)
    }

  it should "should generate a GenericInternalRow with Ion binary data for single input data, validate Ion data and out-put cannot be nullable. " in {
    val f = fixture
    import f._

    val inputRow = new GenericInternalRow(children.map(_.eval(null)).toArray)

    val result = expr.eval(inputRow)

    result shouldBe a[GenericInternalRow]

    val genericInternalRow = result.asInstanceOf[GenericInternalRow]
    assert(genericInternalRow.numFields == 1)

    val binaryData = genericInternalRow.getBinary(0)
    assert(binaryData.length > 0)

    /* Check that the output cannot be nullable */
    assert(result != null && binaryData != null)

    /*validating Ion data using Ion Extractor */
    val ionExtractor = new IonExtractor(inputSchema)
    val ionConverter = IonConverter(inputSchema, new IonOptions(optionsMap))
    val outputRow = ionExtractor.extractData(binaryData).map(ionConverter.parse).flatMap(_.toIterator).toArray

    assert(outputRow.head.asInstanceOf[GenericInternalRow].toSeq(inputSchema) === children.map(_.eval()))

  }

  it should "should generate a GenericInternalRow with Ion binary data for multiple input data rows" in {
    val f = fixture
    import f._

    val schema = StructType(
      Seq(
        StructField("marketplace_id", LongType),
        StructField("item_id", StringType),
        StructField("domain_id", LongType)
      )
    )

    val inputRows = Seq(
      Seq(Literal(1L), Literal("ASIN1"), Literal(1111L)),
      Seq(Literal(2L), Literal("ASIN2"), Literal(2222L)),
      Seq(Literal(3L), Literal("ASIN3"), Literal(3333L))
    )

    inputRows.foreach { inputRow =>
      val expr = new CreateIonStruct(inputRow, schema, optionsMap)

      val result = expr.eval(new GenericInternalRow(inputRow.map(_.eval(null)).toArray))

      result shouldBe a[GenericInternalRow]

      val genericInternalRow = result.asInstanceOf[GenericInternalRow]
      assert(genericInternalRow.numFields == 1)

      val binaryData = genericInternalRow.getBinary(0)
      assert(binaryData.length > 0)

      /* Check that the output cannot be nullable */
      assert(result != null && binaryData != null)

      /*validating Ion data using Ion Extractor */
      val ionExtractor = new IonExtractor(schema)
      val ionConverter = IonConverter(schema, new IonOptions(optionsMap))
      val outputRow = ionExtractor.extractData(binaryData).map(ionConverter.parse).flatMap(_.toIterator).toArray

      assert(outputRow.head.asInstanceOf[GenericInternalRow].toSeq(schema) === inputRow.map(_.eval()))
    }

  }

  it should "checkInputDataTypes should succeed for StructType input schema, if there is no 'all_attributes' column" in {
    val inputSchema = StructType(Seq(StructField("item_id", StringType), StructField("marketplace_id", LongType)))
    val createIonStruct = CreateIonStruct(Seq(), inputSchema, Map.empty[String, String])
    assert(createIonStruct.checkInputDataTypes() == TypeCheckResult.TypeCheckSuccess)
  }

  it should "checkInputDataTypes should fail for non-StructType input schema" in {
    val inputSchema = IntegerType
    val createIonStruct = CreateIonStruct(Seq(), inputSchema, Map.empty[String, String])
    assert(createIonStruct.checkInputDataTypes().isFailure)
  }

  it should "checkInputDataTypes should fail if input schema already has a column named 'all_attributes'" in {
    val structType = StructType(
      Seq(
        StructField("all_attributes", StringType),
        StructField("item_id", StringType),
        StructField("marketplace_id", LongType)
      ))
    val createIonStruct = CreateIonStruct(Seq(), structType, Map.empty[String, String])
    createIonStruct.checkInputDataTypes() shouldEqual TypeCheckResult.TypeCheckFailure(
      "Input schema already has an 'all_attributes' column.")
  }

  it should "throw IllegalArgumentException if options are not of MapType(StringType, StringType)" in {
    val optionsExp: Expression = Literal.create("non-map-literal", StringType)
    val result = intercept[IllegalArgumentException] {
      CreateIonStruct(Seq.empty, StructType(Seq.empty), optionsExp)
    }
    assert(result.getMessage.equals("Expected a map instead of of StringType"))
  }

  it should "parse options map correctly" in {
    val optionsExp: Expression = Literal.create(
      Map("option1" -> "value1", "option2" -> "value2"),
      MapType(StringType, StringType, false)
    )
    val result = CreateIonStruct(Seq.empty, StructType(Seq.empty), optionsExp)
    assert(result.options == Map("option1" -> "value1", "option2" -> "value2"))
  }

  it should "CreateIonStruct should return non-nullable struct" in {
    val f = fixture
    import f._

    assert(expr.nullable === false)

    val row = InternalRow("foo", 123)
    val result = expr.eval(row)
    val internalRow = result.asInstanceOf[InternalRow]

    assert(internalRow.isNullAt(0) === false)
  }
}
