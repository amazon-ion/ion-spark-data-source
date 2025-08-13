// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.system.{IonBinaryWriterBuilder, IonReaderBuilder, IonSystemBuilder}
import com.amazon.ion.{IonReader, IonSystem, IonType, IonValue}
import com.amazon.spark.ion.Loggable
import com.amazon.spark.ion.datasource.IonOptions
import com.amazon.spark.ion.util.TimeUtils.{MICROS_PER_MILLIS, MICROS_PER_SECOND, NANOS_PER_MICROS}
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DecimalType, _}
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayOutputStream, InputStream}
import java.math.{BigDecimal, BigInteger}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}
import java.util.TimeZone
import com.amazon.spark.ion.parser.exception.IonConversionException

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
  * Parse Ion data into Spark Catalyst rows with given Spark schema (StructType) and parsing options.
  * Ion data can come as binary or IonValue from a SQL function like from_ion(), or as InputStream from
  * reading files in Ion format (e.g. from other data sources)
  * Note that since an input binary array could contain a sequence of a IonValues, or an input IonValue
  * could be an IonList, parsing may return multiple rows, hence the Array[InternalRow] return value type.
  */
abstract class IonConverter(schema: StructType, options: IonOptions) extends Loggable {

  protected type ConvertStructReturn
  protected type ConvertArrayReturn
  protected type ConvertMapReturn

  protected type FieldConverterReturn
  protected type FieldConverter = IonReader => FieldConverterReturn

  private val BYTE_LENGTH_FLAG = "byte_length"
  private val FLOAT_BYTE_LENGTH = 4
  private val DOUBLE_BYTE_LENGTH = 8

  // Set an upper bound of warn log messages to avoid overwhelming IO and disk space on executors
  private var logCount = 0
  private val maxLogCount = 10

  private val ionReaderBuilder: IonReaderBuilder = IonReaderBuilder.standard()

  private val rootConverter: IonReader => InternalRow = makeStructRootConverter(schema)

  private val ionSystem: IonSystem = IonSystemBuilder.standard().build()

  def parse(record: Array[Byte]): Array[InternalRow] = {
    val reader = ionReaderBuilder.build(record)
    try {
      parse(reader)
    } finally {
      reader.close()
    }
  }

  def parse(record: IonValue): Array[InternalRow] = {
    val reader = ionReaderBuilder.build(record)
    try {
      parse(reader)
    } finally {
      reader.close()
    }
  }

  def parse(record: IonReader): Array[InternalRow] = {
    parseToIterator(record).toArray
  }

  def parse(ions: InputStream): Iterator[InternalRow] = {
    val ionReader = ionReaderBuilder.build(ions)

    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ionReader.close()))
    parseToIterator(ionReader)
  }

  def parseToIterator(reader: IonReader): Iterator[InternalRow] = {
    val iterator = new Iterator[InternalRow] {
      // Note that the default value (_) is null, because java.lang.Boolean is a wrapper (box) class of boolean value type
      private var _hasNext: java.lang.Boolean = _

      override def hasNext: Boolean = {
        // hasNext() may be called multiple times (before next()) per spec, but we shouldn't call ionReader.next multiple times!
        if (_hasNext == null) {
          val ionType = reader.next()
          _hasNext = ionType != null
        }
        _hasNext
      }

      override def next(): InternalRow = {
        try {
          // if hasNext() isn't called before next() on the iterator, we must still advance the Ion reader
          if (_hasNext == null) reader.next()
          parseRecord(reader)
        } finally {
          _hasNext = null
        }
      }
    }

    // Filter out null rows which could result from malformed records in DROPMALFORMED mode
    iterator.filter(_ != null)
  }

  private def parseRecord(reader: IonReader): InternalRow = {
    // rootConverter(reader) should only bubble up exceptions in the following cases:
    // 1. parserMode=FAILFAST and exception is NonFatal (in which case rethrow and let it fail)
    // 2. parserMode=DROPMALFORMED and exception is NonFatal (in which case return null to ignore entire row)
    // 3. parserMode=PERMISSIVE and top level isn't an Ion struct (in which return null to ignore entire row)
    //   a. for other types of errors in PERMISSIVE, exception should be caught and handled before here
    // 4. exception is fatal (in which case don't catch and let it fail)
    try {
      rootConverter(reader)
    } catch {
      case NonFatal(e) if options.parserMode.equals(ParserMode.FailFast) => throw e
      case NonFatal(_)                                                   => null
    }
  }

  protected def makeStructRootConverter(st: StructType): IonReader => InternalRow

  /**
    * Note that IonJava has 4 main different readers depending on the source data and configuration:
    *
    * 1) Ion binary: IonReaderBinaryUserX extends IonReaderBinarySystemX
    *    https://github.com/amzn/ion-java/blob/master/src/com/amazon/ion/impl/IonReaderBinarySystemX.java
    * 2) IonValue: IonReaderTreeUserX extends IonReaderTreeSystem
    *    https://github.com/amzn/ion-java/blob/master/src/com/amazon/ion/impl/IonReaderTreeSystem.java
    * 3) Ion text: IonReaderTextUserX extends IonReaderTextSystemX
    *    https://github.com/amzn/ion-java/blob/0eb62ba92633f03e9f3175bb77f1b3214fcc1cdd/src/com/amazon/ion/impl/IonReaderTextSystemX.java
    * 4) Ion binary WHEN incremental reading is enabled: IonReaderBinaryIncremental
    *    https://github.com/amzn/ion-java/blob/e7efcbf1652681a40d14f0f725d055e043aed654/src/com/amazon/ion/impl/IonReaderBinaryIncremental.java
    *    This one is used when the builder is created like so: IonReaderBuilder.withIncrementalReadingEnabled(true),
    *    which we do NOT do, therefore we do not use this reader
    *
    * p.s. the fact that the data is IN an Array[Byte] or InputStream doesn't mean it's necessarily Ion binary, binary/text
    *      refers to the encoding of the Ion data, and not the type of data structure used to hold the data
    * p.s. the "User" subclasses handles defined symbol tables for the Ion data: https://amzn.github.io/ion-docs/docs/symbols.html
    *
    * The readers sometimes have different behavior. For example, IonReaderTreeSystem.timestampValue() throws an IllegalStateException
    * with a proper message on a value that isn't an IonTimestamp, whereas IonReaderBinarySystemX's equivalent attempts
    * reading it then ends up with a NullPointerException somewhere. Also, IonReaderBinarySystemX.longValue() throws a
    * _Private_ScalarConversions$CantConvertException on an IonInt that exceeds the bounds of a JVM long, whereas IonReaderTreeSystem's
    * equivalent does a narrowing primitive conversion. Also, IonReaderBinarySystemX succeeds when doing .bigDecimalValue()
    * on an IonFloat (it calls the corresponding IonFloatLite.getBigDecimal() and that one gets the value as double then wraps
    * it as a decimal), whereas IonReaderTreeSystem throws an IllegalStateException("current value is not an ion decimal").
    * So it's important to handle those cases in our code as much as possible in a deterministic manner.
    *
    * Make sure you understand the Ion data model: https://amzn.github.io/ion-docs/docs/spec.html#the-ion-data-model
    */
  def makeFieldConverter(field: StructField): FieldConverter =
    field.dataType match {
      case BooleanType =>
        safeIonValueReader(_, field, convertToBooleanType)
      case ByteType =>
        safeIonValueReader(_, field, convertToByteType)
      case ShortType =>
        safeIonValueReader(_, field, convertToShortType)
      case IntegerType =>
        safeIonValueReader(_, field, convertToIntegerType)
      case LongType =>
        safeIonValueReader(_, field, convertToLongType)
      case FloatType =>
        safeIonValueReader(_, field, convertToFloatType(_, field))
      case DoubleType =>
        safeIonValueReader(_, field, convertToDoubleType(_, field))
      case t: DecimalType =>
        safeIonValueReader(_, field, convertToDecimalType(_, t))
      case StringType =>
        safeIonValueReader(_, field, convertToStringType(_, field))
      case DateType =>
        // Spark's DateType is in days since epoch (in an Int), whereas IonTimestamp is in millis since epoch (in a Long),
        // so we get the millis since epoch and convert to days since epoch.
        (ionReader: IonReader) =>
          safeIonValueReader(
            ionReader,
            field,
            (ionReader: IonReader) =>
              ionReader.getType match {
                case IonType.TIMESTAMP =>
                  DateTimeUtils.microsToDays(DateTimeUtils.millisToMicros(ionReader.timestampValue().getMillis),
                                             TimeZone.getDefault.toZoneId)
                case other: IonType => {
                  val errorMsg =
                    s"Field [${field.name}] contains data of IonType [$other] that cannot be coerced to DateType"
                  throw new IonConversionException(errorMsg)
                }

            }
          )
      case TimestampType =>
        // Spark's TimestampType is in microseconds since epoch (in a Long), whereas IonTimestamp is in milliseconds,
        // so we multiply by 1000.
        (ionReader: IonReader) =>
          safeIonValueReader(
            ionReader,
            field,
            (ionReader: IonReader) =>
              ionReader.getType match {
                case IonType.TIMESTAMP => ionReader.timestampValue().getMillis * 1000L
                case other: IonType => {
                  val errorMsg =
                    s"Field [${field.name}] contains data of IonType [$other] that cannot be coerced to TimestampType"
                  throw new IonConversionException(errorMsg)
                }
            }
          )
      case BinaryType =>
        safeIonValueReader(_, field, convertToBinaryType(_, field))
      case at: ArrayType =>
        val arrayField = StructField(field.name, at.elementType)
        val fieldConverter = makeFieldConverter(arrayField)
        safeIonValueReader(_, field, convertArray(_, fieldConverter))
      case MapType(_: StringType, valueType, valueContainsNull) =>
        val valueField = StructField(field.name, valueType, valueContainsNull)
        val fieldConverter = makeFieldConverter(valueField)
        safeIonValueReader(_, field, convertMap(_, fieldConverter))
      case st: StructType =>
        val fieldConverters = st.map(makeFieldConverter).toArray
        val fieldNameToIndex = st.fieldNames.zipWithIndex.toMap
        safeIonValueReader(_, field, convertStruct(_, st, fieldConverters, fieldNameToIndex))
      case _ =>
        throw new IonConversionException(s"${field.dataType} not supported")
    }

  protected def safeIonValueReader(ionReader: IonReader,
                                   field: StructField,
                                   func: IonReader => Any): FieldConverterReturn

  // We always let null through. We also treat empty/whitespace string as null IF the target type is NOT StringType.
  // Because if the target type is StringType, then empty string is perfectly valid value and we should keep as-is.
  // p.s. If a field is marked as non-nullable, then the record will fail later when checking that constraint.
  protected def shouldConvertToNull(ionReader: IonReader, field: StructField) =
    ionReader.isNullValue ||
      (field.dataType != StringType && ionReader.getType == IonType.STRING && ionReader.stringValue.trim.isEmpty)

  protected def safeStepIn(ionReader: IonReader): Unit = {
    val ionType = ionReader.getType
    if (!List(IonType.STRUCT, IonType.LIST).contains(ionType)) {
      throw new IonConversionException(s"${ionType} is not a struct/list")
    }
    ionReader.stepIn()
  }

  protected def safeStepOut(ionReader: IonReader): Unit = {
    try {
      ionReader.stepOut()
    } catch {
      case NonFatal(e) =>
        val msg = s"Error stepping out of Ion value. Error: ${e.getMessage}"
        if (options.parserMode == ParserMode.Permissive) { // we could be in permissive mode but without malformed column
          if (shouldLogWarn()) log.warn(msg)
        } else {
          throw new IonConversionException(msg, e)
        }
    }
  }

  /**
    * This function gets called for a top-level Ion struct as well as nested structs.
    * Note InternalRow is the internal representation of a Spark struct - and since you can have a struct
    * nested in another, you can also have an InternalRow nested in another, though that still represents
    * one actual output row in the Spark dataframe.
    */
  protected def convertStruct(ionReader: IonReader,
                              st: StructType,
                              fieldConverters: Array[FieldConverter],
                              fieldNameToIndex: Map[String, Int]): ConvertStructReturn

  protected def convertArray(ionReader: IonReader, elementConverter: FieldConverter): ConvertArrayReturn

  protected def convertMap(ionReader: IonReader, valueConverter: FieldConverter): ConvertMapReturn

  protected def findNonNullableIndices(st: StructType): ArrayBuffer[Int] = {
    val indices = ArrayBuffer.empty[Int]
    for (i <- 0 until st.length) {
      if (!st.fields(i).nullable) indices += i
    }
    indices
  }

  protected def violatesNullabilityConstraints(row: InternalRow,
                                               nonNullableIndices: ArrayBuffer[Int],
                                               st: StructType,
                                               throwOnViolation: Boolean): Boolean = {
    var violates = false
    if (options.enforceNullabilityConstraints) {
      nonNullableIndices.foreach(index => {
        if (row.isNullAt(index)) {
          violates = true
          val msg = s"[${st.fields(index).name}] must not be null according to the schema"
          if (throwOnViolation) throw new IonConversionException(msg)
          else if (shouldLogWarn()) log.warn(msg)
        }
      })
    }
    violates
  }

  // Return null instead of Option because used directly to interop with Spark API
  protected def readIonCoerceToString(field: StructField, ionReader: IonReader, throwOnUnsupported: Boolean): String = {
    if (ionReader.isNullValue) {
      null
    } else {
      val errorMsg =
        s"Field [${field.name}] contains data of IonType [${ionReader.getType}] that cannot be coerced to StringType"
      ionReader.getType match {
        case IonType.BOOL            => String.valueOf(ionReader.booleanValue())
        case IonType.DECIMAL         => ionReader.bigDecimalValue().toString
        case IonType.FLOAT           => String.valueOf(ionReader.doubleValue())
        case IonType.INT             => String.valueOf(ionReader.bigIntegerValue())
        case IonType.SYMBOL          => ionReader.symbolValue().getText
        case IonType.TIMESTAMP       => ionReader.timestampValue().toString
        case IonType.STRING          => ionReader.stringValue()
        case IonType.STRUCT          => ionSystem.newValue(ionReader).toString
        case IonType.LIST            => ionSystem.newValue(ionReader).toString
        case _ if throwOnUnsupported => throw new IonConversionException(errorMsg)
        case _                       => null
      }
    }
  }

  // Return null instead of Option because used directly to interop with Spark API
  protected def readIonCoerceToBlob(field: StructField,
                                    ionReader: IonReader,
                                    throwOnUnsupported: Boolean): Array[Byte] = {
    if (ionReader.isNullValue) {
      null
    } else {
      val errorMsg =
        s"Field [${field.name}] contains data of IonType [${ionReader.getType}] that cannot be coerced to BinaryType"

      ionReader.getType match {
        case IonType.STRUCT | IonType.LIST =>
          val baos = new ByteArrayOutputStream(1024)
          val writer = IonBinaryWriterBuilder.standard().build(baos)

          writer.writeValue(ionReader)
          writer.close()

          baos.toByteArray

        case _ if throwOnUnsupported => throw new IonConversionException(errorMsg)
        case _                       => null
      }
    }
  }

  protected def shouldLogWarn(): Boolean = {
    if (logCount < maxLogCount) {
      logCount += 1
      true
    } else {
      false
    }
  }

  private def readIonNumber(ionReader: IonReader): Any = ionReader.getType match {
    // IonInt represents signed integers of arbitrary size
    // https://github.com/amzn/ion-java/blob/master/src/com/amazon/ion/impl/lite/IonIntLite.java
    case IonType.INT => ionReader.bigIntegerValue
    // IonFloat represents double-precision (64 bit) binary floating point numbers (IEEE 754)
    // https://github.com/amzn/ion-java/blob/master/src/com/amazon/ion/impl/lite/IonFloatLite.java
    case IonType.FLOAT => ionReader.doubleValue
    // IonDecimal represents decimal floating point numbers of arbitrary precision
    // https://github.com/amzn/ion-java/blob/master/src/com/amazon/ion/impl/lite/IonDecimalLite.java
    case IonType.DECIMAL => ionReader.bigDecimalValue
    // Ion format is too lenient, and some datasets may have a mix of numbers and STRING representing numeric values
    case IonType.STRING => ionReader.stringValue
    case other: IonType => {
      val fieldName = Option(ionReader.getFieldName).getOrElse("unknown")
      val errorMsg = s"Field [$fieldName] contains data of IonType [$other] that cannot be coerced to a number"
      throw new IonConversionException(errorMsg)
    }
  }

  private def convertToBooleanType(ionReader: IonReader): Boolean = ionReader.getType match {
    case IonType.BOOL => ionReader.booleanValue
    // Ion format is too lenient, and some datasets may have a mix of BOOL and STRING representing boolean values
    case IonType.STRING => ionReader.stringValue.toBoolean
    case other: IonType => {
      val fieldName = Option(ionReader.getFieldName).getOrElse("unknown")
      val errorMsg = s"Field [$fieldName] contains data of IonType [$other] that cannot be coerced to BooleanType"
      throw new IonConversionException(errorMsg)
    }
  }

  private def convertToByteType(ionReader: IonReader): Byte = readIonNumber(ionReader) match {
    case bigInteger: BigInteger => bigInteger.byteValueExact
    case double: Double         => new BigDecimal(double.toString).byteValueExact
    case bigDecimal: BigDecimal => bigDecimal.byteValueExact
    case string: String         => string.toByte // throws if out of range
  }

  private def convertToShortType(ionReader: IonReader): Short = readIonNumber(ionReader) match {
    case bigInteger: BigInteger => bigInteger.shortValueExact
    case double: Double         => new BigDecimal(double.toString).shortValueExact
    case bigDecimal: BigDecimal => bigDecimal.shortValueExact
    case string: String         => string.toShort // throws if out of range
  }

  private def convertToIntegerType(ionReader: IonReader): Int = readIonNumber(ionReader) match {
    case bigInteger: BigInteger => bigInteger.intValueExact
    case double: Double         => new BigDecimal(double.toString).intValueExact
    case bigDecimal: BigDecimal => bigDecimal.intValueExact
    case string: String         => string.toInt // throws if out of range
  }

  private def convertToLongType(ionReader: IonReader): Long = readIonNumber(ionReader) match {
    case bigInteger: BigInteger => bigInteger.longValueExact
    case double: Double         => new BigDecimal(double.toString).longValueExact
    case bigDecimal: BigDecimal => bigDecimal.longValueExact
    case string: String         => string.toLong // throws if out of range
  }

  private def convertToFloatType(ionReader: IonReader, field: StructField): Float = {
    if (!containsByteLengthMetadata(field, FLOAT_BYTE_LENGTH)) {
      if (shouldLogWarn()) {
        log.warn(
          s"For field [${ionReader.getFieldName}] of type [${ionReader.getType}]: ${IonConverter.floatDoubleConversionLossWarn}")
      }
    }

    val source = readIonNumber(ionReader)
    val float = source match {
      case bigInteger: BigInteger => bigInteger.floatValue
      case double: Double         => double.floatValue
      case bigDecimal: BigDecimal => bigDecimal.floatValue
      case string: String         => string.toFloat
    }

    // .floatValue() results in Float.PositiveInfinity/Float.NegativeInfinity if number is too large/small to fit in 32-bit IEEE-754
    if (!isSourceInfinite(source) && isFloatInfinite(float))
      throw new IonConversionException(IonConverter.floatDoubleInfinityError)

    float
  }

  private def convertToDoubleType(ionReader: IonReader, field: StructField): Double = {
    // If source value is IonFloat, then we don't require byte_length metadata because IonFloat IS a DoubleType
    if (ionReader.getType != IonType.FLOAT && !containsByteLengthMetadata(field, DOUBLE_BYTE_LENGTH)) {
      if (shouldLogWarn()) {
        log.warn(
          s"For field [${ionReader.getFieldName}] of type [${ionReader.getType}]: ${IonConverter.floatDoubleConversionLossWarn}")
      }
    }

    val source = readIonNumber(ionReader)
    val double = source match {
      case bigInteger: BigInteger => bigInteger.doubleValue
      case double: Double         => double
      case bigDecimal: BigDecimal => bigDecimal.doubleValue
      case string: String         => string.toDouble
    }

    // .doubleValue() results in Double.PositiveInfinity/Double.NegativeInfinity if number is too large/small to fit in 64-bit IEEE-754
    if (!isSourceInfinite(source) && isDoubleInfinite(double))
      throw new IonConversionException(IonConverter.floatDoubleInfinityError)

    double
  }

  private def containsByteLengthMetadata(field: StructField, length: Int): Boolean =
    field.metadata.contains(BYTE_LENGTH_FLAG) && field.metadata.getLong(BYTE_LENGTH_FLAG) == length

  // Source is either double (for IonFloat inputs) or string (for IonString inputs).
  // BigInteger and BigDecimal themselves don't have the notion of infinity.
  private def isSourceInfinite(source: Any): Boolean =
    source == Double.PositiveInfinity || source == Double.NegativeInfinity ||
      source == "Infinity" || source == "-Infinity"

  private def isFloatInfinite(float: Float): Boolean =
    float == Float.PositiveInfinity || float == Float.NegativeInfinity

  private def isDoubleInfinite(double: Double): Boolean =
    double == Double.PositiveInfinity || double == Double.NegativeInfinity

  private def convertToDecimalType(ionReader: IonReader, t: DecimalType): Decimal = readIonNumber(ionReader) match {
    case bigInteger: BigInteger => Decimal(new BigDecimal(bigInteger), t.precision, t.scale)
    case double: Double         => Decimal(new BigDecimal(double.toString), t.precision, t.scale)
    case bigDecimal: BigDecimal => Decimal(bigDecimal, t.precision, t.scale)
    case string: String         => Decimal(new BigDecimal(string), t.precision, t.scale)
  }

  private def convertToStringType(ionReader: IonReader, field: StructField): UTF8String = {
    val str = ionReader.getType match {
      case IonType.STRING | IonType.SYMBOL    => ionReader.stringValue()
      case _ if options.allowCoercionToString => readIonCoerceToString(field, ionReader, true)
      case other: IonType => {
        val errorMsg = s"Field [${field.name}] contains data of IonType [$other] that cannot be coerced to StringType"
        throw new IonConversionException(errorMsg)
      }
    }
    // returns null given null
    UTF8String.fromString(str)
  }

  private def convertToBinaryType(ionReader: IonReader, field: StructField): Array[Byte] = {
    ionReader.getType match {
      case IonType.BLOB | IonType.CLOB      => ionReader.newBytes()
      case _ if options.allowCoercionToBlob => readIonCoerceToBlob(field, ionReader, true)
      case other: IonType => {
        val errorMsg = s"Field [${field.name}] contains data of IonType [$other] that cannot be coerced to BinaryType"
        throw new IonConversionException(errorMsg)
      }
    }
  }
}

object IonConverter {

  // Not private to access from tests
  val floatDoubleConversionLossError =
    "Conversion into Spark float or double may lose information about the magnitude/precision/range of the numeric value. " +
      "To allow the conversion, you must specify the corresponding byte_length, 4 or 8, respectively, as metadata on the field."
  val floatDoubleConversionLossWarn =
    "Conversion into Spark Float or Double may lose information about the magnitude/precision/range of the numeric value. " +
      "Instead, you should parse Ion Decimal into Spark Decimal, and so on. You can also hide this warning by specify the " +
      "corresponding byte_length, 4 or 8, respectively for Float or Double, as metadata on the field."
  val floatDoubleInfinityError = "Value was too big/small to fit and resulted in positive/negative infinity"

  def apply(schema: StructType, options: IonOptions): IonConverter = {

    /** Malformed record column must be in the root StructType and not in a nested StructType field. **/
    if (options.parserMode == ParserMode.Permissive && schema.exists(_.name == options.columnNameOfCorruptRecord)) {
      new IonConverterWithMalformed(schema, options)
    } else {
      new IonConverterSimple(schema, options)
    }
  }
}
