// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.IonReader
import com.amazon.spark.ion.datasource.IonOptions
import com.amazon.spark.ion.parser.exception.IonConversionException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** This converter is used for FAILFAST/DROPMALFORMED, or PERMISSIVE when malformed column is NOT in schema */
private class IonConverterSimple(schema: StructType, options: IonOptions) extends IonConverter(schema, options) {

  type ConvertStructReturn = InternalRow
  type ConvertArrayReturn = ArrayData
  type ConvertMapReturn = MapData

  override type FieldConverterReturn = Any

  def makeStructRootConverter(st: StructType): IonReader => InternalRow = {
    val fieldConverters = st.map(makeFieldConverter).toArray
    val fieldNameToIndex = st.fieldNames.zipWithIndex.toMap

    ionReader: IonReader =>
      convertStruct(ionReader, st, fieldConverters, fieldNameToIndex)
  }

  def safeIonValueReader(ionReader: IonReader, field: StructField, func: IonReader => Any): Any = {
    var data: Any = null

    try {
      if (!shouldConvertToNull(ionReader, field)) {
        data = func.apply(ionReader)
      }
    } catch {
      // Need to handle all NonFatal exceptions since the caller "IonConverter.parseRecord" expects so
      case NonFatal(e) => {
        val msg = s"Error parsing field [${ionReader.getFieldName}] of type [${ionReader.getType}] " +
          s"into schema type [${field.dataType}]. Error: ${e.getMessage}"
        if (options.parserMode == ParserMode.Permissive) { // we could be in permissive mode but without malformed column
          if (shouldLogWarn()) log.warn(msg)
        } else {
          throw new IonConversionException(msg, e)
        }
      }
    }
    data
  }

  def convertStruct(ionReader: IonReader,
                    st: StructType,
                    fieldConverters: Array[FieldConverter],
                    fieldNameToIndex: Map[String, Int]): InternalRow = {
    val row = new GenericInternalRow(st.length)

    val nonNullableIndices = findNonNullableIndices(st)

    safeStepIn(ionReader)

    try {
      // Start traversing fields in the struct
      while (ionReader.next() != null) {
        fieldNameToIndex
          .get(ionReader.getFieldName)
          .foreach(index => {
            // Note that a field may be a struct itself
            val data = fieldConverters(index).apply(ionReader)
            row.update(index, data)
          })
      }

      // Here we can be in either permissive/dropmalformed (throw), or permissive without malformed column (don't throw)
      violatesNullabilityConstraints(row, nonNullableIndices, st, options.parserMode != ParserMode.Permissive)

      row
    } finally {
      safeStepOut(ionReader)
    }
  }

  def convertArray(ionReader: IonReader, elementConverter: FieldConverter): ArrayData = {
    val values = ArrayBuffer.empty[Any]

    safeStepIn(ionReader)

    try {
      while (ionReader.next() != null) {
        values += elementConverter(ionReader)
      }

      new GenericArrayData(values.toArray)
    } finally {
      safeStepOut(ionReader)
    }
  }

  def convertMap(ionReader: IonReader, valueConverter: FieldConverter): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]

    safeStepIn(ionReader)

    try {
      while (ionReader.next() != null) {
        keys += UTF8String.fromString(ionReader.getFieldName)
        values += valueConverter(ionReader)
      }

      new ArrayBasedMapData(new GenericArrayData(keys.toArray), new GenericArrayData(values.toArray))
    } finally {
      safeStepOut(ionReader)
    }
  }
}
