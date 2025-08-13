// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.IonReader
import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** This converter is used for PERMISSIVE when malformed column is in schema */
private class IonConverterWithMalformed(schema: StructType, options: IonOptions) extends IonConverter(schema, options) {

  type ConvertStructReturn = ConvertResult[InternalRow]
  type ConvertArrayReturn = ConvertResult[ArrayData]
  type ConvertMapReturn = ConvertResult[MapData]

  override type FieldConverterReturn = ConvertResult[Any]

  // We need this case classes here (unlike IonConverterSimple) because for each field conversion
  // we want to keep track of the result, result as string, and whether field is malformed
  case class ConvertResult[T](data: T, dataAsString: String, var isMalformed: Boolean = false)

  def makeStructRootConverter(st: StructType): IonReader => InternalRow = {
    val fieldConverters = st.map(makeFieldConverter).toArray
    val fieldNameToIndex = st.fieldNames.zipWithIndex.toMap
    // Index of column in root struct which will contain malformed record
    val indexOfMalformed = fieldNameToIndex(options.columnNameOfCorruptRecord)

    ionReader: IonReader =>
      {
        val convertResult = convertStruct(ionReader, st, fieldConverters, fieldNameToIndex)
        // If a record is malformed, the malformed column will contain the original row as string with fields read as strings.
        // If a record is not malformed, the malformed column will contain null (but must still there to match # of columns in schema).
        if (convertResult.isMalformed) {
          convertResult.data.update(indexOfMalformed, UTF8String.fromString(convertResult.dataAsString))
        }
        convertResult.data
      }
  }

  def safeIonValueReader(ionReader: IonReader, field: StructField, func: IonReader => Any): ConvertResult[Any] = {
    var data: Any = null
    var dataAsString: String = null
    var isMalformed: Boolean = false

    try {
      if (!shouldConvertToNull(ionReader, field)) {
        data = func.apply(ionReader)
      }
    } catch {
      // Need to handle all NonFatal exceptions since the caller "IonConverter.parseRecord" expects so
      case NonFatal(e) => {
        isMalformed = true
        val msg = s"Error parsing field [${ionReader.getFieldName}] of type [${ionReader.getType}] " +
          s"into schema type [${field.dataType}]. Error: ${e.getMessage}"
        if (shouldLogWarn()) log.warn(msg) // here we are necessarily in permissive mode and with malformed column
      }
    }

    // data could've resulted from converting a:
    data match {
      // complex field (struct/map/array) - in which case we're done
      case convertResult: ConvertResult[Any] => convertResult
      // simple field (int/string...) - in which case we need to read the field as string
      case _ => {
        dataAsString = readIonCoerceToString(field, ionReader, false)
        ConvertResult(data, dataAsString, isMalformed)
      }
    }
  }

  def convertStruct(ionReader: IonReader,
                    st: StructType,
                    fieldConverters: Array[FieldConverter],
                    fieldNameToIndex: Map[String, Int]): ConvertResult[InternalRow] = {
    val data = new GenericInternalRow(st.length)
    val dataAsStrings = ArrayBuffer.empty[String]
    var isMalformed: Boolean = false

    val nonNullableIndices = findNonNullableIndices(st)

    safeStepIn(ionReader)

    try {
      // Start traversing fields in the struct
      // Note that for the top level struct, it will have an extra column somewhere for malformed record.
      // However, since that field will not be in Ion fields, it will not be processed here as
      // .get(ionReader.getFieldName) will simply cause that index to be skipped. Then, makeStructRootConverter
      // will take care of setting that column on the root row.
      while (ionReader.next() != null) {
        fieldNameToIndex
          .get(ionReader.getFieldName)
          .foreach(index => {
            // Note that a field may be a struct itself
            val convertResult = fieldConverters(index).apply(ionReader)
            data.update(index, convertResult.data)
            dataAsStrings += convertResult.dataAsString
            // struct field is malformed if any of its fields are malformed
            isMalformed ||= convertResult.isMalformed
          })
      }

      // Here we are necessarily in permissive mode so we don't throw, just mark struct as malformed
      isMalformed ||= violatesNullabilityConstraints(data, nonNullableIndices, st, false)

      ConvertResult(data, dataAsStrings.mkString("[", ",", "]"), isMalformed)
    } finally {
      safeStepOut(ionReader)
    }
  }

  def convertArray(ionReader: IonReader, elementConverter: FieldConverter): ConvertResult[ArrayData] = {
    val elements = ArrayBuffer.empty[Any]
    val elementsAsStrings = ArrayBuffer.empty[String]
    var isMalformed: Boolean = false

    safeStepIn(ionReader)

    try {
      while (ionReader.next() != null) {
        val convertResult = elementConverter(ionReader)
        elements += convertResult.data
        elementsAsStrings += convertResult.dataAsString
        // array field is malformed if any of its elements are malformed
        isMalformed ||= convertResult.isMalformed
      }

      ConvertResult(
        new GenericArrayData(elements.toArray),
        elementsAsStrings.mkString("Array(", ",", ")"),
        isMalformed
      )
    } finally {
      safeStepOut(ionReader)
    }
  }

  def convertMap(ionReader: IonReader, valueConverter: FieldConverter): ConvertResult[MapData] = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    val keysValuesAsStrings = ArrayBuffer.empty[String]
    var isMalformed: Boolean = false

    safeStepIn(ionReader)

    try {
      while (ionReader.next() != null) {
        val key = ionReader.getFieldName
        keys += UTF8String.fromString(key)
        val convertResult = valueConverter(ionReader)
        values += convertResult.data
        keysValuesAsStrings += s"$key -> ${convertResult.dataAsString}"
        // map field is malformed if any of its map values are malformed
        isMalformed ||= convertResult.isMalformed
      }

      ConvertResult(
        new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values)),
        keysValuesAsStrings.mkString("Map(", ",", ")"),
        isMalformed
      )
    } finally {
      safeStepOut(ionReader)
    }
  }
}
