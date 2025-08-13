// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.spark.ion.Loggable
import org.apache.spark.sql.types.{ArrayType, MapType, Metadata, MetadataBuilder, StructField, StructType}

object DdlMetadataExtractor extends Loggable {

  private case class DdlMetadata(metadataFieldName: String, metadataFieldValue: String)

  private val MetadataIdentifier = "[metadata]"
  private val SqlCommentIdentifier = "COMMENT"
  private val SparkCommentKey = SqlCommentIdentifier.toLowerCase

  /**
    * Using the input schema, creates a new schema by iterating through all levels of nested fields to see if the metadata
    * contains a comment that should be parsed into a field in the Metadata object map. Any comment before the metadata identifier
    * will be set as the new comment in the Metadata map to be consistent with how comments are meant to be used. For example, given:
    * StructType(
    *   StructField("a", StringType, Metadata(Map(
    *     "comment" -> "irrelevant[metadata]max_length=1000"
    *   ))
    *  )
    * )
    * will result in:
    * StructType(
    *   StructField("a", StringType, Metadata(Map(
    *     "comment" -> "irrelevant",
    *     "max_length" -> "1000"
    *   ))
    *  )
    * )
    */
  def createSchemaWithUpdatedMetadata(sparkSchema: StructType): StructType = {
    processStruct(sparkSchema)
  }

  private def processStruct(struct: StructType): StructType = {
    val fields = struct.map(processField)
    StructType(fields)
  }

  private def processField(field: StructField): StructField = {
    val dataType = field.dataType match {
      case struct: StructType => processStruct(struct)
      case array: ArrayType   => processArray(array)
      case map: MapType       => processMap(map)
      case _                  => field.dataType
    }
    val metadata = processFieldComment(field)
    StructField(field.name, dataType, field.nullable, metadata)
  }

  private def processArray(array: ArrayType): ArrayType = {
    val elementType = array.elementType match {
      case struct: StructType => processStruct(struct)
      case array: ArrayType   => processArray(array)
      case map: MapType       => processMap(map)
      case _                  => array.elementType
    }
    ArrayType(elementType, array.containsNull)
  }

  private def processMap(map: MapType): MapType = {
    val keyType = map.keyType match {
      case struct: StructType => processStruct(struct)
      case array: ArrayType   => processArray(array)
      case map: MapType       => processMap(map)
      case _                  => map.keyType
    }
    val valueType = map.valueType match {
      case struct: StructType => processStruct(struct)
      case array: ArrayType   => processArray(array)
      case map: MapType       => processMap(map)
      case _                  => map.valueType
    }
    MapType(keyType, valueType, map.valueContainsNull)
  }

  /**
    * Extracts relevant metadata from a StructField's comment if any. This builds a new Metadata object with all updated
    * metadata fields added, and updates the comment on the field to reflect everything before the [metadata] identifier.
    * E.g.
    * DDL "a STRING COMMENT 'some comment[metadata]max_length=1000'" has a comment of "some comment[metadata]max_length=1000"
    *
    * This would result in:
    * StructField(
    *   name = "a",
    *   dataType = StringType,
    *   metadata = Metadata(Map("comment" -> "some comment", "max_length" -> "1000")) // not Map("comment" -> "max_length=1000")
    * )
    *
    * where calling .getComment() on this field would yield "some comment"
    */
  private def processFieldComment(field: StructField): Metadata = {
    if (field.metadata != null) {
      field.getComment() match {
        case Some(fullComment) =>
          val commentWithMaybeMetadata = extractColumnCommentAndMetadata(fullComment)
          val (allComments, allIndividualMetadata): (String, List[DdlMetadata]) = {
            // E.g. "comment[metadata]key1=val1;key2=val2" is the only accepted form in regard to [metadata]
            if (commentWithMaybeMetadata.length == 2) {
              val comments = extractComment(commentWithMaybeMetadata)
              val allMetadata = extractFullRelevantMetadata(commentWithMaybeMetadata)
              val individualMetadata = extractIndividualMetadata(allMetadata)
              val associatedMetadata = individualMetadata
                .map { extractMaybeMetadataKeyAndValue }
                .collect {
                  case Array(k, v) => Some(DdlMetadata(k, v))
                  case other =>
                    log.info(
                      s"Metadata `${other.mkString("Array(", ", ", ")")}` cannot be extracted. Expected format is one key and one value in the list")
                    None
                }
                .toList
              (comments, associatedMetadata.flatten)
            } else {
              log.info(
                s"Metadata via SQL comment `$fullComment` is malformed for DDL extraction and being applied to a Spark schema. Expected format is `comment[metadata]key=val`")
              (fullComment, List.empty)
            }
          }
          buildNewMetadata(allComments, allIndividualMetadata)
        case _ => field.metadata
      }
    } else {
      field.metadata
    }
  }

  private def buildNewMetadata(allComments: String, ddlMetadata: List[DdlMetadata]): Metadata = {
    val metadataBuilder = new MetadataBuilder()
    ddlMetadata.foreach(md => metadataBuilder.putString(md.metadataFieldName, md.metadataFieldValue))
    metadataBuilder.putString(SparkCommentKey, allComments)
    metadataBuilder.build()
  }

  /**
    * Given "irrelevant[metadata]max_length=1000;other=metadata", returns
    * ["irrelevant", "max_length=1000;other=metadata"]
    */
  private def extractColumnCommentAndMetadata(columnDdl: String): Array[String] = {
    columnDdl.split(s"""\\$MetadataIdentifier""")
  }

  private def extractComment(commentAndMetadata: Array[String]): String = {
    commentAndMetadata.head
  }

  /**
    * Given ["a STRING comment 'irrelevant", "max_length=1000;other=metadata"],
    * returns "max_length=1000;other=metadata"
    */
  private def extractFullRelevantMetadata(commentAndMetadata: Array[String]): String = {
    commentAndMetadata.last
  }

  /**
    * Given "max_length=1000;other=metadata", returns
    * ["max_length=1000", "other=metadata"]
    */
  private def extractIndividualMetadata(allMetadata: String): Array[String] = {
    allMetadata.split(";")
  }

  /**
    * Given "max_length=1000", returns
    * ["max_length", "1000"]
    */
  private def extractMaybeMetadataKeyAndValue(metadata: String): Array[String] = {
    metadata.split("=")
  }

}
