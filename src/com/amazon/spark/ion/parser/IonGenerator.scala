// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.system.{IonBinaryWriterBuilder, IonReaderBuilder, IonTextWriterBuilder}
import com.amazon.ion.{Decimal => _, _}
import com.amazon.spark.ion.datasource.IonFileFormat.{
  IS_BLOB_DATA_MARKER_METADATA,
  IS_BUCKET_KEY_MARKER_METADATA,
  IS_PARTITION_KEY_MARKER_METADATA,
  IS_SORT_KEY_MARKER_METADATA
}
import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import java.io.OutputStream
import java.util.TimeZone
import scala.collection.mutable

private[ion] class IonGenerator(dataType: StructType, outputStream: OutputStream, options: IonOptions) {

  private type ValueWriter = (SpecializedGetters, Int) => Unit

  private val schema: StructType = dataType

  private val ionWriter: IonWriter = getIonWriter(options)

  private val rootFieldWriters: Array[ValueWriter] = schema.map(_.dataType).map(makeWriter).toArray[ValueWriter]

  private val recordAsByteArrayIndex: Option[Int] = getRecordAsByteArrayIndex

  private val keyColumnsMap: mutable.Map[String, (DataType, Int)] = getKeyColumnsMap

  private var remainingKeyColumns: mutable.Map[String, (DataType, Int)] = scala.collection.mutable.Map.empty

  private var bytesWritten = 0;

  private def getRecordAsByteArrayIndex: Option[Int] = {
    // Workaround because Metadata class doesn't have a method that's "safe" (like getBooleanOrDefault)
    val safeGetBlobDataMarker = (field: StructField) =>
      try { field.metadata.getBoolean(IS_BLOB_DATA_MARKER_METADATA) } catch { case _: Throwable => false }

    if (options.recordAsByteArray) {
      val indices = schema.fields.zipWithIndex.collect {
        case (field: StructField, index: Int)
            if field.dataType.isInstanceOf[BinaryType] && safeGetBlobDataMarker(field) =>
          index
      }
      if (indices.length > 1) {
        throw new IllegalArgumentException(
          s"There can be only one blob data field in the schema. Found: ${schema.mkString(", ")}")
      }
      indices.headOption
    } else {
      None
    }
  }

  private def getKeyColumnsMap: mutable.Map[String, (DataType, Int)] = {
    val keysMap = mutable.Map[String, (DataType, Int)]() // Initialize an empty mutable map

    // Populate the mutable map based on conditions
    schema.fields.zipWithIndex.foreach {
      case (field, index) =>
        val isPartitionKeyPresent = field.metadata.contains(IS_PARTITION_KEY_MARKER_METADATA) && field.metadata
          .getBoolean(IS_PARTITION_KEY_MARKER_METADATA)
        val isBucketKeyPresent = field.metadata.contains(IS_BUCKET_KEY_MARKER_METADATA) && field.metadata.getBoolean(
          IS_BUCKET_KEY_MARKER_METADATA)
        val isSortKeyPresent = field.metadata.contains(IS_SORT_KEY_MARKER_METADATA) && field.metadata.getBoolean(
          IS_SORT_KEY_MARKER_METADATA)

        if (isPartitionKeyPresent || isBucketKeyPresent || isSortKeyPresent) {
          keysMap += (field.name -> (field.dataType, index))
        }
    }
    keysMap
  }

  private def getIonWriter(options: IonOptions): IonWriter = {
    options.serialization match {
      case "text" => IonTextWriterBuilder.minimal().build(outputStream)
      case "binary" =>
        if (options.outputBufferSize > -1)
          IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(outputStream)
        else
          IonBinaryWriterBuilder.standard().build(outputStream)
      case _ =>
        throw new IllegalArgumentException(
          s"Expected 'text' or 'binary' serialization but got ${options.serialization}")
    }
  }

  private def makeWriter(dataType: DataType): ValueWriter = {
    dataType match {
      case BooleanType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.BOOL) else ionWriter.writeBool(row.getBoolean(ordinal))

      case ByteType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.INT) else ionWriter.writeInt(row.getByte(ordinal))

      case ShortType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.INT) else ionWriter.writeInt(row.getShort(ordinal))

      case IntegerType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.INT) else ionWriter.writeInt(row.getInt(ordinal))

      case LongType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.INT) else ionWriter.writeInt(row.getLong(ordinal))

      case FloatType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.FLOAT) else ionWriter.writeFloat(row.getFloat(ordinal))

      case DoubleType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.FLOAT)
          else ionWriter.writeFloat(row.getDouble(ordinal))

      case StringType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.STRING)
          else ionWriter.writeString(row.getUTF8String(ordinal).toString)

      case DateType =>
        (row: SpecializedGetters, ordinal: Int) =>
          // Add property to set different timezones?
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.TIMESTAMP)
          else
            ionWriter.writeTimestamp(
              Timestamp.forMillis(DateTimeUtils.microsToMillis(
                                    DateTimeUtils.daysToMicros(row.getInt(ordinal), TimeZone.getDefault.toZoneId)
                                  ),
                                  Timestamp.UTC_OFFSET))

      case TimestampType =>
        (row: SpecializedGetters, ordinal: Int) =>
          // Add property to set different timezones?
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.TIMESTAMP)
          else
            ionWriter.writeTimestamp(
              Timestamp.forMillis(DateTimeUtils.microsToMillis(row.getLong(ordinal)), Timestamp.UTC_OFFSET))

      case BinaryType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.BLOB) else ionWriter.writeBlob(row.getBinary(ordinal))

      case t: DecimalType =>
        (row: SpecializedGetters, ordinal: Int) =>
          if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.DECIMAL)
          else ionWriter.writeDecimal(row.getDecimal(ordinal, t.precision, t.scale).toJavaBigDecimal)

      case t: StructType                     => makeStructWriter(t)
      case t: ArrayType                      => makeArrayWriter(t)
      case MapType(StringType, valueType, _) => makeMapWriter(valueType)

      // TODO Adds IntervalType support
      case _ => sys.error(s"Unsupported data type $dataType.")
    }
  }

  private def makeStructWriter(structType: StructType) = {
    val fieldWriters = structType.map(_.dataType).map(makeWriter).toArray[ValueWriter]

    (row: SpecializedGetters, ordinal: Int) =>
      if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.STRUCT)
      else {
        writeStruct {
          writeFields(row.getStruct(ordinal, structType.length), structType, fieldWriters)
        }
      }
  }

  private def makeArrayWriter(arrayType: ArrayType): ValueWriter = {
    val elementWriter = makeWriter(arrayType.elementType)

    (row: SpecializedGetters, ordinal: Int) =>
      {
        if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.LIST)
        else {
          val arrayData = row.getArray(ordinal)
          writeList {
            var i = 0
            while (i < arrayData.numElements()) {
              elementWriter.apply(arrayData, i)
              i += 1
            }
          }
        }
      }
  }

  private def makeMapWriter(valueType: DataType): ValueWriter = {
    val valueWriter = makeWriter(valueType)

    (row: SpecializedGetters, ordinal: Int) =>
      if (row.isNullAt(ordinal)) ionWriter.writeNull(IonType.STRUCT)
      else {
        val mapData = row.getMap(ordinal)
        val keys = mapData.keyArray()
        val values = mapData.valueArray()
        writeStruct {
          for (i <- 0 until mapData.numElements()) {
            writeField(keys.getUTF8String(i).toString) {
              valueWriter.apply(values, i)
            }
          }
        }
      }
  }

  def write(row: InternalRow): Unit = {
    recordAsByteArrayIndex match {
      case Some(index) =>
        val bytes = row.getBinary(index)
        writeBlobContentWhileInjectingKeyColumnValues(row, bytes)
      case None =>
        writeStruct {
          writeFields(row, schema, rootFieldWriters)
        }
    }
  }

  private def writeBlobContentWhileInjectingKeyColumnValues(row: InternalRow, bytes: Array[Byte]): Unit = {
    keyColumnsMap.foreach(kvp => remainingKeyColumns.put(kvp._1, kvp._2))
    val reader = IonReaderBuilder.standard().build(bytes)

    try {
      var ionType = reader.next()
      if (ionType != null && ionType != IonType.STRUCT) {
        // We never expect the top-level row to be non-struct type as this is just a container for the row
        throw new IllegalArgumentException("Expected to read struct!")
      }
      reader.stepIn()
      ionWriter.stepIn(ionType)

      // Iterate through the IonReader fields
      while (ionType != null) {
        val fieldName = reader.getFieldName
        // If the field is a key column
        if (fieldName != null && remainingKeyColumns.contains(fieldName)) {
          // Write the corresponding value from the row (rather than the blob)
          val (_, index) = remainingKeyColumns(fieldName)
          writeField(fieldName) {
            rootFieldWriters(index)(row, index)
          }
          remainingKeyColumns -= fieldName
        } else {
          // Otherwise write the value from the blob
          ionWriter.writeValue(reader)
        }
        ionType = reader.next()
      }
      // Append any remaining key columns from the row that were not found in the blob
      remainingKeyColumns.foreach {
        case (fieldName, (_, index)) =>
          writeField(fieldName) {
            rootFieldWriters(index)(row, index)
          }
      }
      remainingKeyColumns.clear()
      reader.stepOut()
      ionWriter.stepOut()
    } finally {
      reader.close()
    }

    // Flush if necessary
    val byteLength = bytes.length
    val maxSize = options.outputBufferSize
    bytesWritten = bytesWritten + byteLength

    if (maxSize != -1 && bytesWritten > maxSize) {
      ionWriter.flush()
      bytesWritten = 0
    }
  }

  private def writeFields(row: InternalRow, schema: StructType, fieldWriters: Array[ValueWriter]): Unit = {
    var i = 0
    while (i < row.numFields) {
      writeField(schema(i).name) {
        fieldWriters(i).apply(row, i)
      }
      i += 1
    }
  }

  private def writeField(field: String)(f: => Unit): Unit = {
    ionWriter.setFieldName(field)
    f
  }

  private def writeStruct(f: => Unit): Unit = {
    ionWriter.stepIn(IonType.STRUCT)
    f
    ionWriter.stepOut()
  }

  private def writeList(f: => Unit): Unit = {
    ionWriter.stepIn(IonType.LIST)
    f
    ionWriter.stepOut()
  }

  def close(): Unit = {
    ionWriter.close()
  }

  def flush(): Unit = {
    /* internal buffer written to output stream */
    ionWriter.flush()
  }

  def finish(): Unit = {
    /* to mark end-of-stream marker */
    ionWriter.finish()
  }

}
