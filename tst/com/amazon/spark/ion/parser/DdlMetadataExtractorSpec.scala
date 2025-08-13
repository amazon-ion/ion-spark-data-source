// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import org.apache.spark.sql.types.{
  ArrayType,
  IntegerType,
  MapType,
  Metadata,
  MetadataBuilder,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DdlMetadataExtractorSpec extends AnyFlatSpec with Matchers {

  behavior of "DdlMetadataExtractor"

  it should "return a Spark schema with updated metadata" in {
    val f = fixture; import f._

    val metadataWithTimestampMaskComment: Metadata = new MetadataBuilder()
      .putString("comment", "irrelevant[metadata]timestamp_mask=yyyy-MM-dd")
      .build()

    val schemaWithMaxLengthAndTimestampMask: StructType = StructType(
      Seq(
        StructField("a", IntegerType, nullable = true, null),
        StructField("b", StringType, nullable = true, metadataWithMaxLengthComment),
        StructField("c", TimestampType, nullable = true, metadataWithTimestampMaskComment)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithMaxLengthAndTimestampMask)

    val expectedMetadataForColumnB = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()
    val expectedMetadataForColumnC: Metadata = new MetadataBuilder()
      .putString("timestamp_mask", "yyyy-MM-dd")
      .build()
    val expectedSchema = StructType(
      Seq(
        StructField("a", IntegerType, nullable = true, null),
        StructField("b", StringType, nullable = true, expectedMetadataForColumnB).withComment("irrelevant"),
        StructField("c", TimestampType, nullable = true, expectedMetadataForColumnC).withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "ignore metadata placed before the identifier" in {

    val metadataBeforeIdentifier: Metadata = new MetadataBuilder()
      .putString("comment", "max_length=1000[metadata]byte_length=4")
      .build()

    val schemaWithMetadataBeforeIdentifier: StructType = StructType(
      Seq(
        StructField("a", StringType, nullable = true, metadataBeforeIdentifier)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithMetadataBeforeIdentifier)

    val expectedMetadata = new MetadataBuilder()
      .putString("byte_length", "4")
      .build()
    val expectedSchema = StructType(
      Seq(
        StructField("a", StringType, nullable = true, expectedMetadata).withComment("max_length=1000")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "ignore metadata with no identifier" in {

    val metadataNoIdentifier: Metadata = new MetadataBuilder()
      .putString("comment", "max_length=1000")
      .build()

    val schemaWithMetadataNoIdentifier: StructType = StructType(
      Seq(
        StructField("a", StringType, nullable = true, metadataNoIdentifier)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithMetadataNoIdentifier)

    val expectedMetadata = new MetadataBuilder()
      .putString("comment", "max_length=1000")
      .build()
    val expectedSchema = StructType(
      Seq(
        StructField("a", StringType, nullable = true, expectedMetadata)
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "ignore malformed metadata" in {

    val metadataWithMalformedContent: Metadata = new MetadataBuilder()
      .putString("comment", "irrelevant[metadata]max_length=1000;malformed;final=metadata;asdf==val2")
      .build()

    val schemaWithMaxLengthAndTimestampMask: StructType = StructType(
      Seq(
        StructField("a", StringType, nullable = true, metadataWithMalformedContent)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithMaxLengthAndTimestampMask)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("final", "metadata")
      .build()
    val expectedSchema = StructType(
      Seq(
        StructField("a", StringType, nullable = true, expectedMetadata).withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle array types when updating metadata" in {
    val f = fixture; import f._

    val schemaWithArray: StructType = StructType(
      Seq(
        StructField("a", ArrayType(StringType, containsNull = true), nullable = true, metadataWithMaxLengthComment)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithArray)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()

    val expectedSchema: StructType = StructType(
      Seq(
        StructField("a", ArrayType(StringType, containsNull = true), nullable = true, expectedMetadata)
          .withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle map types when updating metadata" in {
    val f = fixture; import f._

    val schemaWithMap: StructType = StructType(
      Seq(
        StructField("a",
                    MapType(StringType, StringType, valueContainsNull = true),
                    nullable = true,
                    metadataWithMaxLengthComment)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithMap)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()

    val expectedSchema: StructType = StructType(
      Seq(
        StructField("a", MapType(StringType, StringType, valueContainsNull = true), nullable = true, expectedMetadata)
          .withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle nested structs when updating metadata" in {
    val f = fixture; import f._

    val schemaWithNestedStruct: StructType = StructType(
      Seq(
        StructField("a",
                    StructType(
                      Seq(
                        StructField("a_1", StringType, nullable = true, metadataWithMaxLengthComment)
                      )
                    ),
                    nullable = true,
                    null)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithNestedStruct)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()
    val expectedSchema = StructType(
      Seq(
        StructField("a",
                    StructType(
                      Seq(
                        StructField("a_1", StringType, nullable = true, expectedMetadata).withComment("irrelevant")
                      )
                    ),
                    nullable = true,
                    null)
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle nested array types when updating metadata" in {
    val f = fixture; import f._

    val schemaWithNestedArray: StructType = StructType(
      Seq(
        StructField("a",
                    ArrayType(ArrayType(StringType, containsNull = true), containsNull = true),
                    nullable = true,
                    metadataWithMaxLengthComment)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithNestedArray)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()

    val expectedSchema: StructType = StructType(
      Seq(
        StructField("a",
                    ArrayType(ArrayType(StringType, containsNull = true), containsNull = true),
                    nullable = true,
                    expectedMetadata).withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle nested map types when updating metadata" in {
    val f = fixture; import f._

    val schemaWithNestedMap: StructType = StructType(
      Seq(
        StructField("a",
                    MapType(MapType(StringType, StringType, valueContainsNull = true),
                            StringType,
                            valueContainsNull = true),
                    nullable = true,
                    metadataWithMaxLengthComment)
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithNestedMap)

    val expectedMetadata = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()

    val expectedSchema: StructType = StructType(
      Seq(
        StructField("a",
                    MapType(MapType(StringType, StringType, valueContainsNull = true),
                            StringType,
                            valueContainsNull = true),
                    nullable = true,
                    expectedMetadata).withComment("irrelevant")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  it should "handle complex nesting when updating metadata" in {
    val f = fixture; import f._

    val metadataWithByteLength: Metadata = new MetadataBuilder()
      .putString("comment", "placeholder[metadata]byte_length=4")
      .build()

    val schemaWithComplexNesting: StructType = StructType(
      Seq(
        StructField(
          "a",
          StructType(
            Seq(
              StructField(
                "a_1",
                ArrayType(MapType(
                  StructType(
                    Seq(
                      StructField("a_2", StringType, nullable = true, metadataWithMaxLengthComment)
                    )
                  ),
                  ArrayType(
                    MapType(
                      ArrayType(StringType, containsNull = true),
                      IntegerType
                    )
                  )
                )),
                nullable = true,
                metadataWithMaxLengthComment
              )
            )
          ),
          nullable = true,
          metadataWithByteLength
        )
      )
    )

    val updatedSchema = DdlMetadataExtractor.createSchemaWithUpdatedMetadata(schemaWithComplexNesting)

    val expectedMetadataForMaxLength = new MetadataBuilder()
      .putString("max_length", "1000")
      .putString("other", "metadata1")
      .putString("final", "metadata2")
      .build()
    val expectedMetadataForByteLength = new MetadataBuilder()
      .putString("byte_length", "4")
      .build()
    val expectedSchema: StructType = StructType(
      Seq(
        StructField(
          "a",
          StructType(
            Seq(
              StructField(
                "a_1",
                ArrayType(MapType(
                  StructType(
                    Seq(
                      StructField("a_2", StringType, nullable = true, expectedMetadataForMaxLength).withComment(
                        "irrelevant")
                    )
                  ),
                  ArrayType(
                    MapType(
                      ArrayType(StringType, containsNull = true),
                      IntegerType
                    )
                  )
                )),
                nullable = true,
                expectedMetadataForMaxLength
              ).withComment("irrelevant")
            )
          ),
          nullable = true,
          expectedMetadataForByteLength
        ).withComment("placeholder")
      )
    )

    updatedSchema shouldEqual expectedSchema
  }

  private def fixture = new {
    val metadataWithMaxLengthComment: Metadata = new MetadataBuilder()
      .putString("comment", "irrelevant[metadata]max_length=1000;other=metadata1;final=metadata2")
      .build()
  }
}
