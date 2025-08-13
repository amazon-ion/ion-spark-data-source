// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.TimeZone

import com.amazon.ion.{
  IonBlob,
  IonClob,
  IonDecimal,
  IonFloat,
  IonList,
  IonReader,
  IonString,
  IonSymbol,
  IonSystem,
  IonTimestamp,
  Timestamp
}
import com.amazon.ion.system.{IonBinaryWriterBuilder, IonReaderBuilder, IonSystemBuilder}
import com.amazon.spark.ion.datasource.IonOptions
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.{
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  Decimal,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  Metadata,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{LocalDateTime, ZoneOffset}

import com.amazon.spark.ion.parser.exception.IonConversionException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalacheck.Gen

class IonConverterSpec extends AnyFreeSpec with Matchers with MockitoSugar with ScalaCheckDrivenPropertyChecks {

  private val ION = IonSystemBuilder.standard.build
  private val ionReaderBuilder = IonReaderBuilder.standard()
  private val ionBinaryWriterBuilder = IonBinaryWriterBuilder.standard()

  // only works for single record (i.e. 1 IonValue)
  private def createIonReaderFromIonValue(ionText: String): IonReader = ionReaderBuilder.build(ION.singleValue(ionText))

  private def createIonReaderFromIonBinary(ionText: String): IonReader = {
    val baos = new ByteArrayOutputStream()
    val ionReader = ionReaderBuilder.build(ionText)
    val ionBinaryWriter = ionBinaryWriterBuilder.build(baos)

    while (ionReader.next() != null) {
      ionBinaryWriter.writeValue(ionReader) // can handle containers (e.g. IonType.STRUCT)
    }

    ionBinaryWriter.finish()

    val byteArray = baos.toByteArray()

    ionBinaryWriter.close()
    baos.close()
    ionReader.close()

    ionReaderBuilder.build(byteArray)
  }

  private def createIonReaderFromIonText(ionText: String): IonReader = {
    // Note that even though we're creating a ByteArrayInputStream, this is still Ion text encoded data
    val data: InputStream = new ByteArrayInputStream(ionText.getBytes(UTF_8))
    ionReaderBuilder.build(data)
  }

  "for single-record IonValue data" - {
    makeSingleRecordTests(createIonReaderFromIonValue, "com.amazon.ion.impl.IonReaderTreeUserX")
  }

  "for single-record Ion binary data" - {
    makeSingleRecordTests(createIonReaderFromIonBinary, "com.amazon.ion.impl.IonReaderBinaryUserX")
  }

  "for single-record Ion text data" - {
    makeSingleRecordTests(createIonReaderFromIonText, "com.amazon.ion.impl.IonReaderTextUserX")
  }

  private def makeSingleRecordTests(createIonReader: String => IonReader, expectedReaderTypeName: String): Unit = {

    "when converting to BooleanType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.BOOL" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, defaultIonOptions)
        val rows = converter.parse(boolData)

        rows.head.getBoolean(0) shouldEqual true
      }

      "should convert IonType.STRING with true/false strings" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, defaultIonOptions)

        val rows = converter.parse(stringDataTrue)
        rows.head.getBoolean(0) shouldEqual true

        val rows2 = converter.parse(stringDataFalse)
        rows2.head.getBoolean(0) shouldEqual false
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should throw for IonType.STRING invalid string" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [BooleanType]. Error: For input string: \"some string stuff\""
      }

      "should throw for IonType.INT in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(intDataMaxByte)
        } should have message s"Error parsing field [column_a] of type [INT] into schema type [BooleanType]. Error: Field [column_a] contains data of IonType [INT] that cannot be coerced to BooleanType"
      }

      "should nullify for IonType.INT in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, options_permissive)
        val rows = converter.parse(intDataMaxByte)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.INT in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(booleanSchema, options_dropMalformed)
        val rows = converter.parse(intDataMaxByte)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to BooleanType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = BooleanType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestBoolean(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestShort(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestInt(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestLong(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestFloat(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestDouble(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonFloat(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonDecimal(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    "when converting to ByteType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.INT within Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(intDataMaxByte)

        rows.head.getByte(0) shouldEqual 127
      }

      "should convert IonType.FLOAT within Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(floatDataNoFractionMaxByte)

        rows.head.getByte(0) shouldEqual 127
      }

      "should convert IonType.DECIMAL within Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(decimalDataNoFractionMaxByte)

        rows.head.getByte(0) shouldEqual 127
      }

      "should convert IonType.STRING containing a number within Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberMaxByte)

        rows.head.getByte(0) shouldEqual 127
      }

      "should throw for IonType.INT outside Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(bigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [ByteType]. Error: BigInteger out of byte range"
      }

      "should throw for IonType.FLOAT with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(floatData)
        } should have message "Error parsing field [column_a] of type [FLOAT] into schema type [ByteType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalData)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [ByteType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL outside Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalDataNoFractionMaxInt)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [ByteType]. Error: Overflow"
      }

      "should throw for IonType.STRING containing a number outside Byte limits" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ByteType]. Error: For input string: \"1483400968843\""
      }

      "should throw for IonType.STRING containing a float with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberFloat)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ByteType]. Error: For input string: \"0.55E1\""
      }

      "should throw for convert IonType.STRING to ByteType in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ByteType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for convert IonType.STRING to ByteType in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for convert IonType.STRING to ByteType in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(byteSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }
    }

    "when converting to ShortType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.INT within Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(intDataMaxByte)

        rows.head.getShort(0) shouldEqual 127
      }

      "should convert IonType.FLOAT within Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(floatDataNoFractionMaxByte)

        rows.head.getShort(0) shouldEqual 127
      }

      "should convert IonType.DECIMAL within Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(decimalDataNoFractionMaxByte)

        rows.head.getShort(0) shouldEqual 127
      }

      "should convert IonType.STRING containing a number within Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberMaxByte)

        rows.head.getShort(0) shouldEqual 127
      }

      "should throw for IonType.INT outside Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(bigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [ShortType]. Error: BigInteger out of short range"
      }

      "should throw for IonType.FLOAT with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(floatData)
        } should have message "Error parsing field [column_a] of type [FLOAT] into schema type [ShortType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalData)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [ShortType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL outside Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalDataNoFractionMaxInt)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [ShortType]. Error: Overflow"
      }

      "should throw for IonType.STRING containing a number outside Short limits" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ShortType]. Error: For input string: \"1483400968843\""
      }

      "should throw for IonType.STRING containing a float with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberFloat)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ShortType]. Error: For input string: \"0.55E1\""
      }

      "should throw for IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [ShortType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(shortSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to ShortType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = ShortType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestShort(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    "when converting to IntegerType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.INT within Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(intDataMaxInt)

        rows.head.getInt(0) shouldEqual 2147483647
      }

      "should convert IonType.FLOAT within Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(floatDataNoFractionMaxInt)

        rows.head.getInt(0) shouldEqual 2147483647
      }

      "should convert IonType.DECIMAL within Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(decimalDataNoFractionMaxInt)

        rows.head.getInt(0) shouldEqual 2147483647
      }

      "should convert IonType.STRING containing a number within Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberMaxInt)

        rows.head.getInt(0) shouldEqual 2147483647
      }

      "should throw for IonType.INT outside Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(bigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [IntegerType]. Error: BigInteger out of int range"
      }

      "should throw for IonType.FLOAT with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(floatData)
        } should have message "Error parsing field [column_a] of type [FLOAT] into schema type [IntegerType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalData)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [IntegerType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL outside Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(bigDecimalDataNoFraction)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [IntegerType]. Error: Overflow"
      }

      "should throw for IonType.STRING containing a number outside Integer limits" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [IntegerType]. Error: For input string: \"1483400968843\""
      }

      "should throw for IonType.STRING containing a float with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberFloat)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [IntegerType]. Error: For input string: \"0.55E1\""
      }

      "should throw for IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [IntegerType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(intSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to IntegerType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = IntegerType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestShort(schemaDataType, ionOptions, neverExpectException)
        generatorTestInt(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    "when converting to LongType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.INT within Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(bigIntData)

        rows.head.getLong(0) shouldEqual 1483400968843L
      }

      "should convert IonType.FLOAT within Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(floatDataNoFractionMaxInt)

        rows.head.getLong(0) shouldEqual 2147483647L
      }

      "should convert IonType.DECIMAL within Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(bigDecimalDataNoFraction)

        rows.head.getLong(0) shouldEqual 9223372036854L
      }

      "should convert IonType.STRING containing a number within Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberBigInt)

        rows.head.getLong(0) shouldEqual 1483400968843L
      }

      "should throw for IonType.INT outside Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(superBigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [LongType]. Error: BigInteger out of long range"
      }

      "should throw for IonType.FLOAT with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(floatData)
        } should have message "Error parsing field [column_a] of type [FLOAT] into schema type [LongType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(decimalData)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [LongType]. Error: Rounding necessary"
      }

      "should throw for IonType.DECIMAL outside Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(superBigDecimalDataNoFraction)
        } should have message "Error parsing field [column_a] of type [DECIMAL] into schema type [LongType]. Error: Overflow"
      }

      "should throw for IonType.STRING containing a number outside Long limits" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberSuperBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [LongType]. Error: For input string: \"148340096884334329432847328945734857438574365438\""
      }

      "should throw for IonType.STRING containing a float with fraction" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberFloat)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [LongType]. Error: For input string: \"0.55E1\""
      }

      "should throw for IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [LongType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(longSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to LongType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = LongType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestShort(schemaDataType, ionOptions, neverExpectException)
        generatorTestInt(schemaDataType, ionOptions, neverExpectException)
        generatorTestLong(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    "when converting to FloatType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      // Up to 6~7 decimal digits fits in a float without precision loss
      "should convert IonType.INT with <=6 digits" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(intDataMaxByte)

        rows.head.getFloat(0) shouldEqual 127
      }

      // A dyadic (or binary) rational is a number that can be expressed as a fraction whose denominator is a power of two,
      // i.e. can be represented exactly as a float or double
      "should convert IonType.FLOAT dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(floatData)

        rows.head.getFloat(0) shouldEqual 5.5f
      }

      // https://www.exploringbinary.com/why-0-point-1-does-not-exist-in-floating-point/
      // Although 0.1 cannot be represented exactly in binary (float/double), we can have binary floating literals of 0.1.
      // If the input data when printed as string is 0.1, then we should also be able to read it into a float with the
      // same string representation, 0.1.
      // P.s:
      // System.out.print(0.1f) prints 0.1
      // System.out.print(0.1d) prints 0.1
      // System.out.print((double)0.1f) prints 0.10000000149011612
      // System.out.print((float)0.1d) prints 0.1
      "should convert IonType.FLOAT non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(floatData01)

        rows.head.getFloat(0) shouldEqual 0.1f
      }

      "should convert IonType.DECIMAL dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(decimalData)

        rows.head.getFloat(0) shouldEqual 5.5f
      }

      "should convert IonType.DECIMAL non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(decimalData01)

        rows.head.getFloat(0) shouldEqual 0.1f
      }

      "should convert IonType.STRING containing a number within Float limits" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberMaxByte)

        rows.head.getFloat(0) shouldEqual 127f
      }

      "should convert IonType.STRING containing a dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberFloat)

        rows.head.getFloat(0) shouldEqual 5.5f
      }

      "should convert IonType.STRING containing a non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberFloat01)

        rows.head.getFloat(0) shouldEqual 0.1f
      }

      // More than >7 digits doesn't fit in a float without precision loss
      "should convert IonType.INT with precision loss (>7 digits)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(bigIntData)

        rows.head.getFloat(0) shouldEqual 1483400968841f // vs original 1483400968843
      }

      "should convert IonType.FLOAT Float.POSITIVE_INFINITY and Float.NEGATIVE_INFINITY" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)

        val rows = converter.parse(floatDataPositiveInfinity)
        rows.head.getFloat(0) shouldEqual java.lang.Float.POSITIVE_INFINITY

        val rows2 = converter.parse(floatDataNegativeInfinity)
        rows2.head.getFloat(0) shouldEqual java.lang.Float.NEGATIVE_INFINITY
      }

      "should convert IonType.FLOAT with precision loss (>7 digits)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(largeFloatData)

        rows.head.getFloat(0) shouldEqual 12345.123F // vs original 12345.12345
      }

      "should convert IonType.DECIMAL with precision loss (>7 digits)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(bigDecimalDataNoFraction)

        rows.head.getFloat(0) shouldEqual 9.223372E12F // vs original 9_223_372_036_854D0
      }

      // More than >7 digits doesn't fit in a float without precision loss
      "should convert IonType.STRING containing integer (>7 digits) with precision loss" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberBigInt)

        rows.head.getFloat(0) shouldEqual 1483400968841f // vs original "1483400968843"
      }

      "should convert IonType.STRING containing Float.POSITIVE_INFINITY and Float.NEGATIVE_INFINITY" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)

        val rows = converter.parse(stringDataNumberFloatPositiveInfinity)
        rows.head.getFloat(0) shouldEqual java.lang.Float.POSITIVE_INFINITY

        val rows2 = converter.parse(stringDataNumberFloatNegativeInfinity)
        rows2.head.getFloat(0) shouldEqual java.lang.Float.NEGATIVE_INFINITY
      }

      "should convert IonType.STRING containing rational (>7 digits) with precision loss" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberLargeFloat)

        rows.head.getFloat(0) shouldEqual 12345.123F // vs original "12345.12345"
      }

      "should throw for IonType.INT outside Float limits (resulting in +inf or -inf)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(superBigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [FloatType]. Error: " + IonConverter.floatDoubleInfinityError

        the[IonConversionException] thrownBy {
          converter.parse(superSmallIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [FloatType]. Error: " + IonConverter.floatDoubleInfinityError
      }

      "should throw for IonType.STRING containing number outside Float limits (resulting in +inf or -inf)" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberSuperBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [FloatType]. Error: " + IonConverter.floatDoubleInfinityError

        the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberSuperSmallInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [FloatType]. Error: " + IonConverter.floatDoubleInfinityError
      }

      "should throw for IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [FloatType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for convert IonType.STRING for FloatType in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for convert IonType.STRING to FloatType in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(floatSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to FloatType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = FloatType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestShort(schemaDataType, ionOptions, neverExpectException, floatMetadata)
        generatorTestInt(schemaDataType, ionOptions, neverExpectException, floatMetadata)
        generatorTestLong(schemaDataType, ionOptions, neverExpectException, floatMetadata)
        generatorTestFloat(schemaDataType, ionOptions, neverExpectException, floatMetadata)
        generatorTestIonFloat(schemaDataType, ionOptions, neverExpectException, floatMetadata)

        //Should not allow converting into type
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException, floatMetadata)
      }
    }

    "when converting to DoubleType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      // Up to 15~16 decimal digits fits in a double without precision loss
      "should convert IonType.INT with <=15 digits" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(bigIntData) // 1483400968843

        rows.head.getDouble(0) shouldEqual 1483400968843D
      }

      // A dyadic (or binary) rational is a number that can be expressed as a fraction whose denominator is a power of two,
      // i.e. can be represented exactly as a float or double
      "should convert IonType.FLOAT dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(floatData)

        rows.head.getDouble(0) shouldEqual 5.5d
      }

      // https://www.exploringbinary.com/why-0-point-1-does-not-exist-in-floating-point/
      // Although 0.1 cannot be represented exactly in binary (float/double), we can have binary floating literals of 0.1.
      // If the input data when printed as string is 0.1, then we should also be able to read it into a float with the
      // same string representation, 0.1.
      // P.s:
      // System.out.print(0.1f) prints 0.1
      // System.out.print(0.1d) prints 0.1
      // System.out.print((double)0.1f) prints 0.10000000149011612
      // System.out.print((float)0.1d) prints 0.1
      "should convert IonType.FLOAT non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(floatData01)

        rows.head.getDouble(0) shouldEqual 0.1d
      }

      "should convert.DECIMAL dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(decimalData)

        rows.head.getDouble(0) shouldEqual 5.5d
      }

      "should convert.DECIMAL non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(decimalData01)

        rows.head.getDouble(0) shouldEqual 0.1d
      }

      "should convert IonType.STRING containing a number within Double limits" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberMaxByte)

        rows.head.getDouble(0) shouldEqual 127d
      }

      "should convert IonType.STRING containing a dyadic rational (5.5 = 11/2)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberFloat)

        rows.head.getDouble(0) shouldEqual 5.5d
      }

      "should convert IonType.STRING containing a non-dyadic rational (0.1)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberFloat01)

        rows.head.getDouble(0) shouldEqual 0.1d
      }

      // More than >16 digits doesn't fit in a double without precision loss
      "should convert IonType.INT with precision loss (>16 digits)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(superBigIntData)

        rows.head.getDouble(0) shouldEqual 1.4834009688433433E47 // vs original 148340096884334329432847328945734857438574365438
      }

      "should convert IonType.FLOAT Double.POSITIVE_INFINITY and Double.NEGATIVE_INFINITY" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)

        val rows = converter.parse(floatDataPositiveInfinity)
        rows.head.getDouble(0) shouldEqual java.lang.Double.POSITIVE_INFINITY

        val rows2 = converter.parse(floatDataNegativeInfinity)
        rows2.head.getDouble(0) shouldEqual java.lang.Double.NEGATIVE_INFINITY
      }

      "should convert large IonType.FLOAT without precision loss (IonType.FLOAT is always within Double limits)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(largeFloatData) // 12345.12345

        rows.head.getDouble(0) shouldEqual 12345.12345D
      }

      "should convert IonType.DECIMAL with <=15 digits" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(bigDecimalDataNoFraction) // 9_223_372_036_854D0

        rows.head.getDouble(0) shouldEqual 9223372036854D
      }

      "should convert IonType.DECIMAL with precision loss (>16 digits)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(superBigDecimalDataNoFraction)

        rows.head.getDouble(0) shouldEqual 5.189123813123132E30 // vs original 5189123813123131224343433456789D0
      }

      // More than >16 digits doesn't fit in a double without precision loss
      "should convert IonType.STRING containing integer (>16 digits) with precision loss" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberSuperBigInt)

        rows.head.getDouble(0) shouldEqual 1.4834009688433433E47 // vs original "148340096884334329432847328945734857438574365438"
      }

      "should convert IonType.STRING containing Double.POSITIVE_INFINITY and Double.NEGATIVE_INFINITY" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)

        val rows = converter.parse(stringDataNumberFloatPositiveInfinity)
        rows.head.getDouble(0) shouldEqual java.lang.Double.POSITIVE_INFINITY

        val rows2 = converter.parse(stringDataNumberFloatNegativeInfinity)
        rows2.head.getDouble(0) shouldEqual java.lang.Double.NEGATIVE_INFINITY
      }

      "should convert IonType.STRING containing rational (>16 digits) with precision loss" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberSuperLargeFloat)

        rows.head.getDouble(0) shouldEqual 1.2345678901234512E14d // vs original "123456789012345.123456789012345"
      }

      "should throw for IonType.INT outside Double limits (resulting in +inf or -inf)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(superDuperBigIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [DoubleType]. Error: " + IonConverter.floatDoubleInfinityError

        the[IonConversionException] thrownBy {
          converter.parse(superDuperSmallIntData)
        } should have message "Error parsing field [column_a] of type [INT] into schema type [DoubleType]. Error: " + IonConverter.floatDoubleInfinityError
      }

      "should throw for IonType.STRING containing number outside Double limits (resulting in +inf or -inf)" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, defaultIonOptions)

        the[IonConversionException] thrownBy {
          converter.parse(stringDataSuperDuperBigInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [DoubleType]. Error: " + IonConverter.floatDoubleInfinityError

        the[IonConversionException] thrownBy {
          converter.parse(stringDataSuperDuperSmallInt)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [DoubleType]. Error: " + IonConverter.floatDoubleInfinityError
      }

      "should convert IonType.FLOAT EVEN WHEN DOUBLE DOES NOT have byte_length=8 because IonFloat IS a double" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchemaWithoutByteLength, defaultIonOptions)
        val rows = converter.parse(floatData)

        rows.head.getDouble(0) shouldEqual 5.5d
      }

      "should throw for IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(stringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [DoubleType]. Error: For input string: \"some string stuff\""
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(doubleSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to DoubleType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = DoubleType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestShort(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestInt(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestLong(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestFloat(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestDouble(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestIonFloat(schemaDataType, ionOptions, neverExpectException, doubleMetadata)
        generatorTestIonDecimal(schemaDataType, ionOptions, neverExpectException, doubleMetadata)

        //Should not allow converting into type
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException, doubleMetadata)
      }
    }

    "when converting to DecimalType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.INT when the data precision/scale is in-line with the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(bigIntData)

        rows.head.getDecimal(0, 38, 10) shouldEqual Decimal("1483400968843")
      }

      "should convert IonType.FLOAT when the data precision/scale is in-line with the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(largeFloatData)
        rows.head.getDecimal(0, 38, 10) shouldEqual Decimal("12345.1234500000")
      }

      "should convert IonType.DECIMAL when the data precision/scale is in-line with the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(largeDecimalData)
        rows.head.getDecimal(0, 38, 10) shouldEqual Decimal("518912381312313.1234567890")
      }

      "should convert IonType.STRING containing a number with precision/scale in-line with the schema " in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, defaultIonOptions)
        val rows = converter.parse(stringDataNumberLargeFloat)

        rows.head.getDecimal(0, 38, 0) shouldEqual Decimal("12345.1234500000")
      }

      "should throw for IonType.INT when the data precision/scale exceeds the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchemaSmall, defaultIonOptions)
        val exception = the[IonConversionException] thrownBy {
          converter.parse(bigIntData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [INT] into schema type [DecimalType(5,1)]. " +
          "Error: [DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision 14 exceeds max precision 5."
      }

      "should throw for IonType.FLOAT when the data precision/scale exceeds the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchemaSmall, defaultIonOptions)
        val exception = the[IonConversionException] thrownBy {
          converter.parse(largeFloatData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [FLOAT] into schema type [DecimalType(5,1)]. " +
          "Error: [DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision 6 exceeds max precision 5."
      }

      "should throw for IonType.DECIMAL when the data precision/scale exceeds the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchemaSmall, defaultIonOptions)
        val exception = the[IonConversionException] thrownBy {
          converter.parse(largeDecimalData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [DECIMAL] into schema type [DecimalType(5,1)]. " +
          "Error: [DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision 16 exceeds max precision 5."
      }

      "should throw for IonType.STRING containing a number with precision/scale that exceeds the schema" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchemaSmall, defaultIonOptions)
        val exception = the[IonConversionException] thrownBy {
          converter.parse(stringDataNumberLargeFloat)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [STRING] into schema type [DecimalType(5,1)]. " +
          "Error: [DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION] Decimal precision 6 exceeds max precision 5."
      }

      "should convert IonType.FLOAT with 64-bit precision (natural number) to DecimalType(38,0) without precision loss" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema380, defaultIonOptions)
        val rows = converter.parse(floatDataNaturalNumber1) // 1.00101706411108E14

        rows.head.getDecimal(0, 38, 0) shouldEqual Decimal("100101706411108")

        val rows2 = converter.parse(floatDataNaturalNumber2) // 1.00091561404123E14
        rows2.head.getDecimal(0, 38, 0) shouldEqual Decimal("100091561404123")
      }

      "should throw for convert IonType.STRING in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, options_failFast)

        val exception = the[IonConversionException] thrownBy {
          converter.parse(stringData)
        }

        // Exception thrown in jdk8 and jdk17 are bit different, they are as follows :
        // jdk8            => NumberFormatException()
        // jdk17 and crema => NumberFormatException("Character " + c + " is neither a decimal digit number, decimal point, nor" + " \"e\" notation exponential mark.")

        exception.getMessage should startWith(
          "Error parsing field [column_a] of type [STRING] into schema type [DecimalType(38,10)]. Error: "
        )
      }

      "should nullify for convert IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(decimalSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to DecimalType or throw an exception" in {
        val f = fixture; import f._
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        forAll { (value: BigDecimal) =>
          if (value.scale <= value.precision && value.precision <= 38 && value.scale > 0) {
            val minPrecisionMinScaleSchema = DecimalType(value.precision, value.scale)
            generatorTestInternal(minPrecisionMinScaleSchema, ionOptions, neverExpectException, value)

            val maxPrecisionMinScaleSchema = DecimalType(38, value.scale)
            generatorTestInternal(maxPrecisionMinScaleSchema, ionOptions, neverExpectException, value)
          }
        }

        //Should not allow converting into type
        val schemaDataType = DecimalType(38, 18)
        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    "when converting to StringType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should NOT nullify empty String if schema is of StringType" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)
        rows.head.getString(0) shouldEqual ""
      }

      "should convert IonType.STRING" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, defaultIonOptions)
        val rows = converter.parse(stringData)

        rows.head.getString(0) shouldEqual "some string stuff"
      }

      "should throw for convert IonType.INT in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast)
        val exception = intercept[IonConversionException] {
          converter.parse(intDataMaxByte)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [INT] into schema type [StringType]. Error: Field [column_a] contains data of IonType [INT] that cannot be coerced to StringType"
      }

      "should fail to coerce unsupported IonType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val exception = intercept[IonConversionException] {
          converter.parse(unsupportedData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [SEXP] into schema type [StringType]. " +
          "Error: Field [column_a] contains data of IonType [SEXP] that cannot be coerced to StringType"
      }

      "should nullify for coerce unsupported IonType when allowCoercionToString=true in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_permissive_allowCoercionToString)
        val rows = converter.parse(unsupportedData)

        rows.head shouldEqual rowWithNull
      }

      "should drop rows for coerce unsupported IonType when allowCoercionToString=true in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_dropMalformed_allowCoercionToString)
        val rows = converter.parse(unsupportedData)
        rows.isEmpty shouldEqual true
      }

      "should coerce IonType.BOOL to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(boolData)
        rows.head.getString(0) shouldEqual "true"
      }

      "should coerce IonType.INT to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(intDataMaxByte)
        rows.head.getString(0) shouldEqual "127"
      }

      "should coerce IonType.INT (long) to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(longData)
        rows.head.getString(0) shouldEqual "9123456789012345678"
      }

      "should coerce IonType.FLOAT to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(floatData)
        rows.head.getString(0) shouldEqual "5.5"
      }

      "should coerce IonType.DECIMAL to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(decimalData)

        rows.head.getString(0) shouldEqual "5.5"
      }

      "should coerce IonType.SYMBOL to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(symbolData)

        rows.head.getString(0) shouldEqual "test-symbol"
      }

      "should coerce IonType.TIMESTAMP to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(timestampData)

        rows.head.getString(0) shouldEqual timestamp.toString()
      }

      "should throw for coerce IonType.BLOB when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)

        the[IonConversionException] thrownBy {
          converter.parse(blobData)
        } should have message "Error parsing field [column_a] of type [BLOB] into schema type [StringType]. " +
          "Error: Field [column_a] contains data of IonType [BLOB] that cannot be coerced to StringType"
      }

      "should throw for coerce IonType.CLOB when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)

        the[IonConversionException] thrownBy {
          converter.parse(clobData)
        } should have message "Error parsing field [column_a] of type [CLOB] into schema type [StringType]. " +
          "Error: Field [column_a] contains data of IonType [CLOB] that cannot be coerced to StringType"
      }

      "should coerce IonType.LIST to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(listData)

        rows.head.getString(0) shouldEqual listStringWithNestedData
      }

      "should coerce IonType.STRUCT to StringType when allowCoercionToString=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(stringSchema, options_failFast_allowCoercionToString)
        val rows = converter.parse(structData)

        rows.head.getString(0) shouldEqual nestedStructString
      }

      "should for all inputs either properly convert to StringType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = StringType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestString(schemaDataType, ionOptions, neverExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestShort(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestInt(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestLong(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestFloat(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestDouble(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonFloat(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonDecimal(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptions, alwaysExpectException)

        //Should allow coercing into String
        val ionOptionAllowCoercion = new IonOptions(
          CaseInsensitiveMap(
            Map(
              "allowCoercionToString" -> "true"
            )))

        generatorTestShort(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestInt(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestLong(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        //Skipping Float as it produces unpredictable strings
        generatorTestDouble(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestBoolean(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestIonFloat(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestIonDecimal(schemaDataType, ionOptionAllowCoercion, neverExpectException)
        generatorTestIonTimestamp(schemaDataType, ionOptionAllowCoercion, neverExpectException)

        // Can't coerce those to string (per current implementation - though that can be supported by coercing to base64)
        generatorTestIonBlob(schemaDataType, ionOptionAllowCoercion, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptionAllowCoercion, alwaysExpectException)
      }
    }

    "when converting to BinaryType" - {

      "should let null through" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.BLOB" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, defaultIonOptions)
        val rows = converter.parse(blobData)

        rows.head.getBinary(0) shouldEqual testBytes()
      }

      "should convert IonType.CLOB" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, defaultIonOptions)
        val rows = converter.parse(clobData)

        rows.head.getBinary(0) shouldEqual testBytes()
      }

      "should throw for convert IonType.INT in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast)
        val exception = intercept[IonConversionException] {
          converter.parse(intDataMaxByte)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [INT] into schema type [BinaryType]. Error: Field [column_a] contains data of IonType [INT] that cannot be coerced to BinaryType"
      }

      "should fail to coerce unsupported IonType when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(unsupportedData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [SEXP] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [SEXP] that cannot be coerced to BinaryType"
      }

      "should nullify for coerce unsupported IonType when allowCoercionToBlob=true in PERMISSIVE" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_permissive_allowCoercionToBlob)
        val rows = converter.parse(unsupportedData)

        rows.head shouldEqual rowWithNull
      }

      "should drop rows for coerce unsupported IonType when allowCoercionToBlob=true in DROPMALFORMED" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_dropMalformed_allowCoercionToBlob)
        val rows = converter.parse(unsupportedData)
        rows.isEmpty shouldEqual true
      }

      "should throw for IonType.BOOL when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(boolData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [BOOL] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [BOOL] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.INT when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(intDataMaxByte)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [INT] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [INT] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.INT (long) when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(longData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [INT] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [INT] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.FLOAT when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(floatData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [FLOAT] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [FLOAT] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.DECIMAL when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(decimalData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [DECIMAL] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [DECIMAL] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.STRING when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(stringData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [STRING] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [STRING] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.SYMBOL when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(symbolData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [SYMBOL] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [SYMBOL] that cannot be coerced to BinaryType"
      }

      "should throw for IonType.TIMESTAMP when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val exception = intercept[IonConversionException] {
          converter.parse(timestampData)
        }
        exception.getMessage shouldEqual "Error parsing field [column_a] of type [TIMESTAMP] into schema type [BinaryType]. " +
          "Error: Field [column_a] contains data of IonType [TIMESTAMP] that cannot be coerced to BinaryType"
      }

      "should coerce IonType.LIST to BinaryType when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val rows = converter.parse(listData)

        ionSystem.singleValue(rows.head.getBinary(0)) shouldEqual ionSystem.singleValue(listStringWithNestedData)
      }

      "should coerce IonType.STRUCT to BinaryType when allowCoercionToBlob=true in FAILFAST" in {
        val f = fixture;
        import f._

        val converter = IonConverter(blobSchema, options_failFast_allowCoercionToBlob)
        val rows = converter.parse(structData)

        ionSystem.singleValue(rows.head.getBinary(0)) shouldEqual ionSystem.singleValue(nestedStructString)
      }
    }

    "when converting to DateType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.TIMESTAMP to DateType" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, options_failFast)
        val rows = converter.parse(timestampData)

        rows.head.getInt(0) shouldEqual daysSinceEpoch
      }

      "should throw for IonType.STRING (even if represents a timestamp) in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(timestampAsStringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [DateType]. Error: Field [column_a] contains data of IonType [STRING] that cannot be coerced to DateType"
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(dateSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }
    }

    "when converting to TimestampType" - {

      "should let null through" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, defaultIonOptions)
        val rows = converter.parse(nullData)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.STRING empty string to null" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, defaultIonOptions)
        val rows = converter.parse(stringDataEmpty)

        rows.head.isNullAt(0) shouldEqual true
      }

      "should convert IonType.TIMESTAMP" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, options_failFast)
        val rows = converter.parse(timestampData)

        rows.head.getLong(0) shouldEqual millisSinceEpoch * 1000L
      }

      "should throw for IonType.STRING (even if represents a timestamp) in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, options_failFast)

        the[IonConversionException] thrownBy {
          converter.parse(timestampAsStringData)
        } should have message "Error parsing field [column_a] of type [STRING] into schema type [TimestampType]. Error: Field [column_a] contains data of IonType [STRING] that cannot be coerced to TimestampType"
      }

      "should nullify for IonType.STRING in PERMISSIVE" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, options_permissive)
        val rows = converter.parse(stringData)
        rows.head shouldEqual rowWithNull
      }

      "should drop row for IonType.STRING in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(timestampSchema, options_dropMalformed)
        val rows = converter.parse(stringData)
        rows.isEmpty shouldBe true
      }

      "should for all inputs either properly convert to TimestampType or throw an exception" in {
        val f = fixture; import f._
        val schemaDataType = TimestampType
        val ionOptions = defaultIonOptions

        //Should convert type successfully
        generatorTestIonTimestamp(schemaDataType, ionOptions, neverExpectException)

        //Should not allow converting into type
        generatorTestShort(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestInt(schemaDataType, ionOptions, alwaysExpectException)

        generatorTestString(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestBoolean(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonBlob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonClob(schemaDataType, ionOptions, alwaysExpectException)
        generatorTestIonSymbol(schemaDataType, ionOptions, alwaysExpectException)
      }
    }

    def fixture = new {
      val ionSystem: IonSystem = IonSystemBuilder.standard().build()

      val booleanSchema: StructType = StructType(Seq(StructField("column_a", BooleanType)))
      val byteSchema: StructType = StructType(Seq(StructField("column_a", ByteType)))
      val shortSchema: StructType = StructType(Seq(StructField("column_a", ShortType)))
      val intSchema: StructType = StructType(Seq(StructField("column_a", IntegerType)))
      val longSchema: StructType = StructType(Seq(StructField("column_a", LongType)))
      val floatMetadata = new MetadataBuilder().putLong("byte_length", 4).build()
      val floatSchema: StructType = StructType(Seq(StructField("column_a", FloatType, metadata = floatMetadata)))
      val floatSchemaWithoutByteLength: StructType = StructType(Seq(StructField("column_a", FloatType)))
      val doubleMetadata = new MetadataBuilder().putLong("byte_length", 8).build()
      val doubleSchema: StructType = StructType(Seq(StructField("column_a", DoubleType, metadata = doubleMetadata)))
      val doubleSchemaWithoutByteLength: StructType = StructType(Seq(StructField("column_a", DoubleType)))
      val stringSchema: StructType = StructType(Seq(StructField("column_a", StringType)))
      val blobSchema: StructType = StructType(Seq(StructField("column_a", BinaryType)))
      val decimalSchema: StructType = StructType(Seq(StructField("column_a", DecimalType(38, 10))))
      val decimalSchemaSmall: StructType = StructType(Seq(StructField("column_a", DecimalType(5, 1))))
      val decimalSchema380: StructType = StructType(Seq(StructField("column_a", DecimalType(38, 0))))
      val dateSchema: StructType = StructType(Seq(StructField("column_a", DateType)))
      val timestampSchema: StructType = StructType(Seq(StructField("column_a", TimestampType)))

      val validTimeRange =
        for (n <- Gen.choose(-62135769600000L + 1, 253402300800000L - 1)) yield n

      def generatorTestShort(schemaDataType: org.apache.spark.sql.types.DataType,
                             ionOptions: IonOptions,
                             expectException: Any => Boolean,
                             schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Short) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value, schemaMetadata)
        }
      }

      def generatorTestInt(schemaDataType: org.apache.spark.sql.types.DataType,
                           ionOptions: IonOptions,
                           expectException: Any => Boolean,
                           schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Int) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value, schemaMetadata)
        }
      }

      def generatorTestLong(schemaDataType: org.apache.spark.sql.types.DataType,
                            ionOptions: IonOptions,
                            expectException: Any => Boolean,
                            schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Long) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value, schemaMetadata)
        }
      }

      def generatorTestFloat(schemaDataType: org.apache.spark.sql.types.DataType,
                             ionOptions: IonOptions,
                             expectException: Any => Boolean,
                             schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Float) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value.floatValue(), schemaMetadata)
        }
      }

      def generatorTestDouble(schemaDataType: org.apache.spark.sql.types.DataType,
                              ionOptions: IonOptions,
                              expectException: Any => Boolean,
                              schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Double) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value, schemaMetadata)
        }
      }

      def generatorTestString(schemaDataType: org.apache.spark.sql.types.DataType,
                              ionOptions: IonOptions,
                              expectException: Any => Boolean,
                              schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: String) =>
          // special characters aren't accepted by Ion text reader
          val cleanedValue = value.replaceAll("[^a-zA-Z0-9]+", "lkdjsflkdsjfs")
          whenever(cleanedValue != "") {
            generatorTestInternal(schemaDataType, ionOptions, expectException, cleanedValue, schemaMetadata)
          }
        }
      }

      def generatorTestBoolean(schemaDataType: org.apache.spark.sql.types.DataType,
                               ionOptions: IonOptions,
                               expectException: Any => Boolean,
                               schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Boolean) =>
          generatorTestInternal(schemaDataType, ionOptions, expectException, value, schemaMetadata)
        }
      }

      def generatorTestIonBlob(schemaDataType: org.apache.spark.sql.types.DataType,
                               ionOptions: IonOptions,
                               expectException: Any => Boolean,
                               schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Array[Byte]) =>
          whenever(value != None) {
            val blob = ionSystem.newBlob(value)
            blob.getBytes
            generatorTestInternal(schemaDataType, ionOptions, expectException, blob, schemaMetadata)
          }
        }
      }

      def generatorTestIonClob(schemaDataType: org.apache.spark.sql.types.DataType,
                               ionOptions: IonOptions,
                               expectException: Any => Boolean,
                               schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Array[Byte]) =>
          whenever(value != None) {
            val clob = ionSystem.newClob(value)
            generatorTestInternal(schemaDataType, ionOptions, expectException, clob, schemaMetadata)
          }
        }
      }

      def generatorTestIonFloat(schemaDataType: org.apache.spark.sql.types.DataType,
                                ionOptions: IonOptions,
                                expectException: Any => Boolean,
                                schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Float) =>
          val float = ionSystem.newFloat(value.floatValue())
          generatorTestInternal(schemaDataType, ionOptions, expectException, float, schemaMetadata)
        }
      }

      def generatorTestIonDecimal(schemaDataType: org.apache.spark.sql.types.DataType,
                                  ionOptions: IonOptions,
                                  expectException: Any => Boolean,
                                  schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: Double) =>
          val decimal = ionSystem.newDecimal(value)
          generatorTestInternal(schemaDataType, ionOptions, expectException, decimal, schemaMetadata)
        }
      }

      def generatorTestIonSymbol(schemaDataType: org.apache.spark.sql.types.DataType,
                                 ionOptions: IonOptions,
                                 expectException: Any => Boolean,
                                 schemaMetadata: Metadata = Metadata.empty) = {
        forAll { (value: String) =>
          val symbol = ionSystem.newSymbol(value)
          generatorTestInternal(schemaDataType, ionOptions, expectException, symbol, schemaMetadata)
        }
      }

      def generatorTestIonTimestamp(schemaDataType: org.apache.spark.sql.types.DataType,
                                    ionOptions: IonOptions,
                                    expectException: Any => Boolean,
                                    schemaMetadata: Metadata = Metadata.empty) = {
        forAll(validTimeRange) { (value: Long) =>
          val timestamp = ionSystem.newUtcTimestampFromMillis(value)
          generatorTestInternal(schemaDataType, ionOptions, expectException, timestamp, schemaMetadata)
        }
      }

      def generatorTestInternal(schemaDataType: org.apache.spark.sql.types.DataType,
                                ionOptions: IonOptions,
                                expectException: Any => Boolean,
                                value: Any,
                                schemaMetadata: Metadata = Metadata.empty): Unit = {
        val schema: StructType = StructType(Seq(StructField("column_a", schemaDataType, metadata = schemaMetadata)))
        val converter = IonConverter(schema, ionOptions)

        val maybeData: Option[IonReader] = try {
          Option(createIonReaderWithSingleColumn(value match {
            case string: String => "\"" + string + "\"" // otherwise it's invalid Ion
            case _              => value.toString
          }))
        } catch {
          //Catch data that is invalid syntax
          case e: Exception => None
        }

        //Either verify that expected exception is thrown or value is correct
        if (maybeData.nonEmpty) {
          if (expectException(value)) {
            assertThrows[IonConversionException] {
              converter.parse(maybeData.get)
            }
          } else {
            val row = converter.parse(maybeData.get).head

            if (ionOptions.allowCoercionToString) {
              value match {
                case _: IonBlob | _: IonClob =>
                  row shouldEqual rowWithNull
                case ionFloat: IonFloat =>
                  row.getString(0) shouldEqual String.valueOf(ionFloat.doubleValue())
                case ionDecimal: IonDecimal =>
                  row.getString(0) shouldEqual ionDecimal.bigDecimalValue().toString
                case _ =>
                  row.getString(0) shouldEqual value.toString
              }
            } else {
              value match {
                case ionFloat: IonFloat =>
                  row shouldEqual rowWithValue(ionFloat.doubleValue()) // remember IonFloat is 64-bit
                case float: Float =>
                  if (schemaDataType == DoubleType) {
                    row.getDouble(0).floatValue() shouldEqual float
                  } else {
                    row shouldEqual rowWithValue(float)
                  }
                case ionDecimal: IonDecimal =>
                  if (schemaDataType == DoubleType) {
                    row shouldEqual rowWithValue(ionDecimal.doubleValue())
                  } else {
                    row shouldEqual rowWithValue(ionDecimal.decimalValue())
                  }
                case ionSymbol: IonSymbol =>
                  row.getString(0) shouldEqual ionSymbol.stringValue()
                case bigDecimal: BigDecimal =>
                  val decType = schemaDataType.asInstanceOf[DecimalType]
                  row
                    .getDecimal(0, decType.precision, decType.scale)
                    .toString() shouldEqual bigDecimal.toString()
                case ionTimestamp: IonTimestamp =>
                  row shouldEqual rowWithValue(ionTimestamp.timestampValue().getMillis * 1000) // Spark timestamp is in microseconds
                case string: String =>
                  row shouldEqual rowWithValue(UTF8String.fromString(string))
                case _ =>
                  row shouldEqual rowWithValue(value)
              }
            }
          }
        }
      }

      def createIonReaderWithSingleColumn(columnData: String): IonReader = {
        createIonReader(
          s"""
             |{
             | column_a:$columnData
             |}
             |""".stripMargin
        )
      }

      val nullData: IonReader = {
        createIonReaderWithSingleColumn(null)
      }

      val boolData: IonReader = {
        createIonReaderWithSingleColumn("true")
      }

      val stringDataTrue: IonReader = {
        createIonReaderWithSingleColumn("\"TRUE\"")

      }

      val stringDataFalse: IonReader = {
        createIonReaderWithSingleColumn("\"false\"")
      }

      val intDataMaxByte: IonReader = {
        createIonReaderWithSingleColumn("127")
      }

      val stringDataNumberMaxByte: IonReader = {
        createIonReaderWithSingleColumn("\"127\"")
      }

      val floatDataNoFractionMaxByte: IonReader = {
        // E exponent --> IonFloat
        createIonReaderWithSingleColumn("1.27E2")
      }

      val decimalDataNoFractionMaxByte: IonReader = {
        // D exponent --> IonDecimal
        createIonReaderWithSingleColumn("1.27D2")
      }

      val floatData: IonReader = {
        createIonReaderWithSingleColumn("0.55E1")
      }

      val stringDataNumberFloat: IonReader = {
        createIonReaderWithSingleColumn("\"0.55E1\"")

      }

      val floatData01: IonReader = {
        createIonReaderWithSingleColumn("0.1E0")
      }

      val stringDataNumberFloat01: IonReader = {
        createIonReaderWithSingleColumn("\"0.1E0\"")
      }

      // Fits in float (with precision loss >6 digits) and in double (without precision loss <15)
      val largeFloatData: IonReader = {
        // E exponent --> IonFloat
        createIonReaderWithSingleColumn("12345.12345E0")
      }

      val stringDataNumberLargeFloat: IonReader = {
        createIonReaderWithSingleColumn("\"12345.12345E0\"")
      }

      // Fits in float/double both with precision loss
      val stringDataNumberSuperLargeFloat: IonReader = {
        createIonReaderWithSingleColumn("\"123456789012345.123456789012345\"")
      }

      // https://amzn.github.io/ion-docs/docs/float.html#special-values
      val floatDataPositiveInfinity: IonReader = {
        createIonReaderWithSingleColumn("+inf")
      }

      val stringDataNumberFloatPositiveInfinity: IonReader = {
        // note that in Ion floats, +inf is positive infinity, whereas it is Infinity in Java
        createIonReaderWithSingleColumn("\"Infinity\"")
      }

      // https://amzn.github.io/ion-docs/docs/float.html#special-values
      val floatDataNegativeInfinity: IonReader = {
        createIonReaderWithSingleColumn("-inf")
      }

      val stringDataNumberFloatNegativeInfinity: IonReader = {
        // note that in Ion floats, -inf is negative infinity, whereas it is -Infinity in Java
        createIonReaderWithSingleColumn("\"-Infinity\"")
      }

      val intDataMaxInt: IonReader = {
        createIonReaderWithSingleColumn("2147483647")
      }

      val stringDataNumberMaxInt: IonReader = {
        createIonReaderWithSingleColumn("\"2147483647\"")
      }

      val floatDataNoFractionMaxInt: IonReader = {
        // E exponent --> IonFloat
        // IonFloat is a JVM double which can safely fit 15 decimal digits, so this is fine
        createIonReaderWithSingleColumn("214748364.7E1")
      }

      val decimalDataNoFractionMaxInt: IonReader = {
        // D exponent --> IonDecimal
        createIonReaderWithSingleColumn("214748364.7D1")
      }

      // wouldn't fit in an byte/short/int but would fit in long/float(precision loss)/double
      val bigIntData: IonReader = {
        createIonReaderWithSingleColumn("1483400968843")
      }

      // wouldn't fit in an byte/short/int but would fit in long/float(precision loss)/double
      val stringDataNumberBigInt: IonReader = {
        createIonReaderWithSingleColumn("\"1483400968843\"")
      }

      // wouldn't fit in byte/short/int but would fit in long/float(precision loss)/double
      val bigDecimalDataNoFraction: IonReader = {
        // D exponent --> IonDecimal
        createIonReaderWithSingleColumn("9_223_372_036_854D0")
      }

      val stringDataNumberBigDecimalNoFraction: IonReader = {
        createIonReaderWithSingleColumn("\"9223372036854D0\"")
      }

      // wouldn't fit in a long/float (still would fit in a double, with precision loss since over 15 decimal digits)
      // https://docs.oracle.com/javase/7/docs/api/java/lang/Double.html#MAX_VALUE
      val superBigIntData: IonReader = {
        createIonReaderWithSingleColumn("148340096884334329432847328945734857438574365438")
      }

      val stringDataNumberSuperBigInt: IonReader = {
        createIonReaderWithSingleColumn("\"148340096884334329432847328945734857438574365438\"")
      }

      // wouldn't fit in a long/float
      val superSmallIntData: IonReader = {
        createIonReaderWithSingleColumn("-148340096884334329432847328945734857438574365438")
      }

      val stringDataNumberSuperSmallInt: IonReader = {
        createIonReaderWithSingleColumn("\"-148340096884334329432847328945734857438574365438\"")
      }

      // wouldn't fit in byte/short/int/long (still would fit in a double, with precision loss since over 15 decimal digits)
      val superBigDecimalDataNoFraction: IonReader = {
        createIonReaderWithSingleColumn("5189123813123131224343433456789D0")
      }

      // wouldn't fit in a double
      val superDuperBigIntData: IonReader = {
        createIonReaderWithSingleColumn(
          "148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438")
      }

      // wouldn't fit in a double
      val stringDataSuperDuperBigInt: IonReader = {
        createIonReaderWithSingleColumn(
          "\"148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438\"")
      }

      // wouldn't fit in a double
      val superDuperSmallIntData: IonReader = {
        createIonReaderWithSingleColumn(
          "-148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438")
      }

      // wouldn't fit in a double
      val stringDataSuperDuperSmallInt: IonReader = {
        createIonReaderWithSingleColumn(
          "\"-148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438148340096884334329432847328945734857438574365438\"")
      }

      // This is an IonFloat because of the E exponent, even though it's actually a "natural number", i.e. no fraction.
      // https://amzn.github.io/ion-docs/docs/spec.html#real-numbers
      val floatDataNaturalNumber1: IonReader = {
        createIonReaderWithSingleColumn("1.00101706411108E14")
      }

      val floatDataNaturalNumber2: IonReader = {
        createIonReaderWithSingleColumn("1.00091561404123E14")
      }

      val blobData: IonReader = {
        val blob = ionSystem.newBlob(testBytes)
        createIonReaderWithSingleColumn(blob.toString)
      }

      val clobData: IonReader = {
        val clob = ionSystem.newClob(testBytes)
        createIonReaderWithSingleColumn(clob.toString)
      }

      // def because this is mutable, prefer not to share it across tests
      def testBytes(): Array[Byte] = "test".getBytes(StandardCharsets.UTF_8)

      val longData: IonReader = {
        createIonReaderWithSingleColumn("9123456789012345678")
      }

      val decimalData: IonReader = {
        val decimal = ionSystem.newDecimal(5.5)
        createIonReaderWithSingleColumn(decimal.toString)
      }

      val decimalData01: IonReader = {
        createIonReaderWithSingleColumn("0.1D0")
      }

      val largeDecimalData: IonReader = {
        val bigDecimal = BigDecimal("518912381312313.123456789")
        createIonReaderWithSingleColumn(bigDecimal.toString)
      }

      val symbolData: IonReader = {
        val symbol = ionSystem.newSymbol("test-symbol")
        createIonReaderWithSingleColumn(symbol.toString)
      }

      // use system timezone, because that is what the Ion date converter uses
      val zone = TimeZone.getDefault.toZoneId
      val zonedDateTimeNow = LocalDateTime.now(ZoneOffset.UTC).atZone(zone)

      val daysSinceEpoch = zonedDateTimeNow.toLocalDate.toEpochDay.toInt

      val millisSinceEpoch = zonedDateTimeNow.toEpochSecond() * 1000

      // https://javadoc.io/doc/software.amazon.ion/ion-java/1.0.0/software/amazon/ion/Timestamp.html
      val timestamp = Timestamp.forMillis(millisSinceEpoch, 0)

      val timestampData: IonReader = {
        createIonReaderWithSingleColumn(timestamp.toString)
      }

      val timestampAsStringData: IonReader = {
        createIonReaderWithSingleColumn("\"2022/01/03 20:30:35\"")
      }

      val listStringWithNestedData =
        """[{testId:"testId",testTimestamp:1483400968812,testName:"testName",testNestedStruct:{testNestedStruct2:{testNestedStruct3:{testNestedStructField:"testNestedStructField"}}}},{testId:"testId2",testTimestamp:1483400968906,testName:null,testNestedStruct:{testNestedStruct2:{testNestedStruct3:{testNestedStructField:null}}}}]"""

      val listData: IonReader = {
        createIonReaderWithSingleColumn(listStringWithNestedData)
      }

      val nestedStructString =
        """{testId:"testId",testTimestamp:1483400968812,testName:"testName",testNestedStruct:{testNestedStruct2:{testNestedStruct3:{testNestedStructField:"testNestedStructField"}}}}"""

      val structData: IonReader = {
        createIonReaderWithSingleColumn(nestedStructString)
      }

      val unsupportedData: IonReader = {
        val unsupported = ionSystem.newEmptySexp()
        createIonReaderWithSingleColumn(unsupported.toString)
      }

      val stringData: IonReader = {
        createIonReaderWithSingleColumn("\"some string stuff\"")
      }

      val stringDataEmpty: IonReader = {
        createIonReaderWithSingleColumn("\"\"")
      }

      val defaultIonOptions = new IonOptions(CaseInsensitiveMap(Map.empty))

      val options_failFast: IonOptions = new IonOptions(Map("parserMode" -> "FAILFAST"))
      val options_permissive = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "PERMISSIVE")))
      val options_permissive_customMalformedName = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "columnNameOfCorruptRecord" -> "customMalformedName"
          )))

      val options_dropMalformed: IonOptions = new IonOptions(Map("parserMode" -> "DROPMALFORMED"))

      val options_failFast_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "FAILFAST",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_failFast_allowCoercionToBlob = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "FAILFAST",
            "allowCoercionToBlob" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_failFast_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "FAILFAST",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_permissive_allowCoercionToString_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_permissive_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_permissive_allowCoercionToBlob = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToBlob" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_permissive_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_dropMalformed_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "DROPMALFORMED",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_dropMalformed_allowCoercionToBlob = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "DROPMALFORMED",
            "allowCoercionToBlob" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_dropMalformed_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "DROPMALFORMED",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val rowWithNull = rowWithValue(null)

      def rowWithValue(value: Any) = {
        new GenericInternalRow(Array(value))
      }

      val alwaysExpectException: Any => Boolean = value => true

      val neverExpectException: Any => Boolean = value => false

      def doesNumberNotFitInBounds(minBound: Double, maxBound: Double): Any => Boolean = value => {
        value match {
          case short: Short =>
            short < minBound || short > maxBound
          case int: Int =>
            int < minBound || int > maxBound
          case long: Long =>
            long < minBound || long > maxBound
          case float: Float =>
            float < minBound || float > maxBound
          case double: Double =>
            double < minBound || double > maxBound
          case ionFloat: IonFloat =>
            ionFloat.doubleValue() < minBound || ionFloat.doubleValue() > maxBound
          case ionDecimal: IonDecimal =>
            ionDecimal.doubleValue() < minBound || ionDecimal.doubleValue() > maxBound
          case _ =>
            true
        }
      }
    }
  }

  "for multi-record Ion binary data" - {
    makeMultiRecordTests(createIonReaderFromIonBinary, "com.amazon.ion.impl.IonReaderBinaryUserX")
  }

  "for multi-record Ion text data" - {
    makeMultiRecordTests(createIonReaderFromIonText, "com.amazon.ion.impl.IonReaderTextUserX")
  }

  private def makeMultiRecordTests(createIonReader: String => IonReader, expectedReaderTypeName: String): Unit = {

    "should create a custom ion reading iterator that is idempotent on hasNext" in {
      val f = fixture; import f._

      val schema = StructType(
        StructField("testId", StringType, nullable = false) ::
          StructField("testTimestamp", LongType, nullable = false) ::
          Nil)
      val converter = IonConverter(schema, defaultIonOptions)
      val iterator = converter.parseToIterator(multiRecordTestData)

      // 0 calls to hasNext() before next()
      val firstRow = iterator.next()
      firstRow shouldEqual InternalRow(UTF8String.fromString("testId"), 1483400968812L)

      // multiple calls to hasNext() before next()
      iterator.hasNext
      iterator.hasNext
      iterator.hasNext
      val secondRow = iterator.next()
      secondRow shouldEqual InternalRow(UTF8String.fromString("testId2"), 1483400968906L)
    }

    "should convert row data based on columns specified in schema" in {
      val f = fixture; import f._

      val schema = StructType(
        StructField("testId", StringType, nullable = false) ::
          StructField("testTimestamp", LongType, nullable = false) ::
          Nil)

      val converter = IonConverter(schema, defaultIonOptions)
      val result = converter.parse(multiRecordTestData).toList

      val expected = List(
        InternalRow(
          UTF8String.fromString("testId"),
          1483400968812L
        ),
        InternalRow(
          UTF8String.fromString("testId2"),
          1483400968906L
        )
      )
      result shouldEqual expected
    }

    "should throw for top-level field type mismatch in FAILFAST" in {
      val f = fixture; import f._

      val schema = StructType(
        StructField("testId", StringType, nullable = false) ::
          StructField("testTimestamp", StringType, nullable = false) :: // type mismatch in data (number)
          Nil)

      val converter = IonConverter(schema, options_failFast)

      the[IonConversionException] thrownBy {
        converter.parse(multiRecordTestData).toList
      } should have message "Error parsing field [testTimestamp] of type [INT] into schema type [StringType]. Error: Field [testTimestamp] contains data of IonType [INT] that cannot be coerced to StringType"
    }

    "should nullify fields for top-level field type mismatch in PERMISSIVE" in {
      val f = fixture; import f._

      val schema = StructType(
        StructField("testId", StringType, nullable = false) ::
          StructField("testTimestamp", StringType, nullable = false) :: // type mismatch in data (number)
          Nil)

      val converter = IonConverter(schema, options_permissive)
      val result = converter.parse(multiRecordTestData).toList

      val expected = List(
        InternalRow(
          UTF8String.fromString("testId"),
          null
        ),
        InternalRow(
          UTF8String.fromString("testId2"),
          null
        )
      )
      result shouldEqual expected
    }

    "should drop rows for top-level field type mismatch in DROPMALFORMED" in {
      val f = fixture; import f._

      val schema = StructType(
        StructField("testId", StringType, nullable = false) ::
          StructField("testTimestamp", StringType, nullable = false) :: // type mismatch in data (number)
          Nil)

      val converter = IonConverter(schema, options_dropMalformed)

      val result = converter.parse(multiRecordTestData).toList

      result shouldEqual Nil
    }

    "should throw if top level Ion is not an Ion struct in FAILFAST" in {
      val f = fixture
      import f._

      val schema = StructType(StructField("test_column", StringType, nullable = false) :: Nil)
      val converter = IonConverter(schema, options_failFast)

      the[IonConversionException] thrownBy {
        converter.parse(nonStructData).toList
      } should have message "STRING is not a struct/list"
    }

    "should drop rows if top level Ion is not an Ion struct in PERMISSIVE" in {
      val f = fixture; import f._

      val schema = StructType(StructField("test_column", StringType) :: Nil)
      val converter = IonConverter(schema, options_permissive)

      val result = converter.parse(nonStructData).toList

      result shouldEqual Seq.empty
    }

    "should drop rows if top level Ion is not an Ion struct in DROPMALFORMED" in {
      val f = fixture; import f._

      val schema = StructType(StructField("test_column", StringType) :: Nil)
      val converter = IonConverter(schema, options_dropMalformed)

      val result = converter.parse(nonStructData).toList

      result shouldEqual Seq.empty
    }

    "should throw if nested StructType field is not an Ion struct in FAILFAST" in {
      val f = fixture; import f._

      val schema = StructType(
        Seq(
          StructField("testId", StringType, nullable = false),
          StructField("testTimestamp", LongType, nullable = false),
          StructField(
            "testNestedStruct",
            StructType(
              Seq(
                StructField(
                  "testNestedStruct2",
                  StructType(Seq(
                    StructField(
                      "testNestedStruct3",
                      StructType(Seq(
                        StructField("testNestedStructField",
                                    StructType(Seq( // in the data this field is string, not struct!
                                      StructField("doesntexist", StringType))))
                      ))
                    )
                  ))
                )
              ))
          )
        ))

      val converter = IonConverter(schema, options_failFast)

      the[IonConversionException] thrownBy {
        converter.parse(multiRecordTestData).toList
      } should have message "Error parsing field [null] of type [null] into schema type [StructType(StructField(testNestedStruct2,StructType(StructField(testNestedStruct3,StructType(StructField(testNestedStructField,StructType(StructField(doesntexist,StringType,true)),true)),true)),true))]. Error: Error parsing field [null] of type [null] into schema type [StructType(StructField(testNestedStruct3,StructType(StructField(testNestedStructField,StructType(StructField(doesntexist,StringType,true)),true)),true))]. Error: Error parsing field [null] of type [null] into schema type [StructType(StructField(testNestedStructField,StructType(StructField(doesntexist,StringType,true)),true))]. Error: Error parsing field [testNestedStructField] of type [STRING] into schema type [StructType(StructField(doesntexist,StringType,true))]. Error: STRING is not a struct/list"
    }

    "should nullify field if nested StructType field is not an Ion struct in PERMISSIVE" in {
      val f = fixture; import f._

      val schema = StructType(
        Seq(
          StructField("testId", StringType, nullable = false),
          StructField("testTimestamp", LongType, nullable = false),
          StructField(
            "testNestedStruct",
            StructType(
              Seq(
                StructField(
                  "testNestedStruct2",
                  StructType(Seq(
                    StructField(
                      "testNestedStruct3",
                      StructType(Seq(
                        StructField("testNestedStructField",
                                    StructType(Seq( // in the data this field is string, not struct!
                                      StructField("doesntexist", StringType))))
                      ))
                    )
                  ))
                )
              ))
          )
        ))

      val converter = IonConverter(schema, options_permissive)

      val result = converter.parse(multiRecordTestData).toList

      val expected = List(
        InternalRow(
          UTF8String.fromString("testId"),
          1483400968812L,
          InternalRow(
            InternalRow(
              InternalRow(
                null
              )
            )
          )
        ),
        InternalRow(
          UTF8String.fromString("testId2"),
          1483400968906L,
          InternalRow(
            InternalRow(
              InternalRow(
                null
              )
            )
          )
        )
      )
      result shouldEqual expected
    }

    "should drop rows if nested StructType field is not an Ion struct in DROPMALFORMED" in {
      val f = fixture; import f._

      val schema = StructType(
        Seq(
          StructField("testId", StringType, nullable = false),
          StructField("testTimestamp", LongType, nullable = false),
          StructField(
            "testNestedStruct",
            StructType(
              Seq(
                StructField(
                  "testNestedStruct2",
                  StructType(
                    Seq(
                      StructField(
                        "testNestedStruct3",
                        StructType(Seq(
                          StructField("testNestedStructField", StructType(Seq(StructField("doesntexist", StringType))))
                        ))
                      )
                    ))
                )
              ))
          )
        ))

      val converter = IonConverter(schema, options_dropMalformed)

      val result = converter.parse(multiRecordTestData).toList

      // In the data: first row has "testNestedStructField" as string, not struct, so that's malformed
      //              second row has "testNestedStructField" as null, so that's not malfromed
      val expected = List(
        InternalRow(
          UTF8String.fromString("testId2"),
          1483400968906L,
          InternalRow(
            InternalRow(
              InternalRow(null)
            )
          )
        )
      )
      result shouldEqual expected
    }

    // This is also Spark's behavior if there are more/less tokens in data than in schema
    "should nullify if nested StructType field is in schema but not in Ion value" in {
      val f = fixture; import f._

      val schema = StructType(
        Seq(
          StructField("testId", StringType, nullable = false),
          StructField("testTimestamp", LongType, nullable = false),
          StructField(
            "testNestedStruct",
            StructType(
              Seq(
                StructField(
                  "testNestedStruct2",
                  StructType(
                    Seq(
                      StructField("idontexist",
                                  StructType(Seq( // in the data this field doesn't exist
                                    StructField("idontexisteither", StringType))))
                    ))
                )
              ))
          )
        ))

      val converter = IonConverter(schema, options_failFast)
      val result = converter.parse(multiRecordTestData).toList

      val expected = List(
        InternalRow(
          UTF8String.fromString("testId"),
          1483400968812L,
          InternalRow(
            InternalRow(null)
          )
        ),
        InternalRow(
          UTF8String.fromString("testId2"),
          1483400968906L,
          InternalRow(
            InternalRow(null)
          )
        )
      )
      result shouldEqual expected
    }

    "when enforcing non-nullability" - {

      "should throw for top-level null in non-nullable column when enforceNullabilityConstraints=true in FAILFAST" in {
        val f = fixture; import f._

        val schema = StructType(
          StructField("testId", StringType, nullable = false) ::
            StructField("testTimestamp", LongType, nullable = false) ::
            StructField("testName", StringType, nullable = false) :: // one of the rows has null here
            Nil)

        val converter = IonConverter(schema, options_failFast_enforceNullabilityConstraints)

        the[IonConversionException] thrownBy {
          converter.parse(multiRecordTestData).toList
        } should have message "[testName] must not be null according to the schema"
      }

      "should nullify fields for top-level null in non-nullable column when enforceNullabilityConstraints=true in PERMISSIVE" in {
        val f = fixture; import f._

        val schema = StructType(
          StructField("testId", StringType, nullable = false) ::
            StructField("testTimestamp", LongType, nullable = false) ::
            StructField("testName", StringType, nullable = false) :: // one of the rows has null here
            Nil)

        val converter = IonConverter(schema, options_permissive_enforceNullabilityConstraints)
        val result = converter.parse(multiRecordTestData).toList

        val expected = List(
          InternalRow(
            UTF8String.fromString("testId"),
            1483400968812L,
            UTF8String.fromString("testName")
          ),
          InternalRow(
            UTF8String.fromString("testId2"),
            1483400968906L,
            null
          )
        )
        result shouldEqual expected
      }

      "should drop rows for top-level null in non-nullable column when enforceNullabilityConstraints=true in DROPMALFORMED" in {
        val f = fixture; import f._

        val schema = StructType(
          StructField("testId", StringType, nullable = false) ::
            StructField("testTimestamp", LongType, nullable = false) ::
            StructField("testName", StringType, nullable = false) :: // one of the rows has null here
            Nil)

        val converter = IonConverter(schema, options_dropMalformed_enforceNullabilityConstraints)
        val result = converter.parse(multiRecordTestData).toList

        val expected = List(
          InternalRow(
            UTF8String.fromString("testId"),
            1483400968812L,
            UTF8String.fromString("testName")
          )
        )
        result shouldEqual expected
      }

      "should allow null in nested nullable field when enforceNullabilityConstraints=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(nestedStructPassSchema, options_failFast_enforceNullabilityConstraints)

        val result = converter.parse(nestedStructTestData).toList

        val expected = List(
          InternalRow(
            InternalRow(
              InternalRow(null), // schema allows this to be null
              InternalRow(UTF8String.fromString("defined1")) // but not this
            )
          ),
          InternalRow(
            InternalRow(
              InternalRow(UTF8String.fromString("defined2")),
              InternalRow(UTF8String.fromString("defined3"))
            )
          )
        )
        result shouldEqual expected
      }

      "should allow null in nested non-nullable field when enforceNullabilityConstraints=false in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(nestedStructFailSchema, options_failFast)

        val result = converter.parse(nestedStructTestData).toList

        val expected = List(
          InternalRow(
            InternalRow(
              InternalRow(null), // schema says this should be non-nullable, but enforceNullabilityConstraints=false
              InternalRow(UTF8String.fromString("defined1"))
            )
          ),
          InternalRow(
            InternalRow(
              InternalRow(UTF8String.fromString("defined2")),
              InternalRow(UTF8String.fromString("defined3"))
            )
          )
        )
        result shouldEqual expected
      }

      "should fail for null in nested non-nullable field when enforceNullabilityConstraints=true in FAILFAST" in {
        val f = fixture; import f._

        val converter = IonConverter(nestedStructFailSchema, options_failFast_enforceNullabilityConstraints)

        val err = the[IonConversionException] thrownBy {
          converter.parse(nestedStructTestData).toList
        }
        err.getMessage shouldEqual "Error parsing field [null] of type [null] into schema type [StructType(StructField(b,StructType(StructField(d,StringType,false)),true),StructField(c,StructType(StructField(d,StringType,true)),true))]. Error: Error parsing field [null] of type [null] into schema type [StructType(StructField(d,StringType,false))]. Error: [d] must not be null according to the schema"
      }

      "should nullify for null in nested non-nullable field when enforceNullabilityConstraints=true in PERMISSIVE" in {
        val f = fixture; import f._

        // Basically enforceNullabilityConstraints=true does nothing in PERMISSIVE (not exactly nothing, it marks record as malformed)
        val converter = IonConverter(nestedStructFailSchema, options_permissive_enforceNullabilityConstraints)

        val result = converter.parse(nestedStructTestData).toList

        val expected = List(
          InternalRow(
            InternalRow(
              InternalRow(null), // schema says this should be non-nullable
              InternalRow(UTF8String.fromString("defined1"))
            )
          ),
          InternalRow(
            InternalRow(
              InternalRow(UTF8String.fromString("defined2")),
              InternalRow(UTF8String.fromString("defined3"))
            )
          )
        )
        result shouldEqual expected
      }

      "should drop rows for null in nested non-nullable field when enforceNullabilityConstraints=true in DROPMALFORMED" in {
        val f = fixture; import f._

        val converter = IonConverter(nestedStructFailSchema, options_dropMalformed_enforceNullabilityConstraints)

        val result = converter.parse(nestedStructTestData).toList

        val expected = List(
          InternalRow(
            InternalRow(
              InternalRow(UTF8String.fromString("defined2")),
              InternalRow(UTF8String.fromString("defined3"))
            )
          )
        )
        result shouldEqual expected
      }
    }

    "when adding malformed column in permissive" - {
      "should include malformed records in _corrupt_record (default) when in schema in PERMISSIVE" in {
        val f = fixture; import f._

        val schema = StructType(
          Seq(
            StructField("testId", StringType, nullable = false),
            StructField("testTimestamp", StringType, nullable = false), // type mismatch in data (number)
            StructField("_corrupt_record", StringType)
          ))

        val converter = IonConverter(schema, options_permissive)
        val result = converter.parse(multiRecordTestData).toList

        val expected = List(
          InternalRow(
            UTF8String.fromString("testId"),
            null,
            // this is the extra malformed record column with everything read as string
            UTF8String.fromString("[testId,1483400968812]")
          ),
          InternalRow(
            UTF8String.fromString("testId2"),
            null,
            // this is the extra malformed record column with everything read as string
            UTF8String.fromString("[testId2,1483400968906]")
          )
        )
        result shouldEqual expected
      }

      "should include malformed records in customMalformedName (config) when in schema in PERMISSIVE" in {
        val f = fixture; import f._

        val schema = StructType(
          Seq(
            StructField("testId", StringType, nullable = false),
            StructField("testTimestamp", StringType, nullable = false), // type mismatch in data (number)
            StructField("customMalformedName", StringType)
          ))

        val converter = IonConverter(schema, options_permissive_customMalformedName)
        val result = converter.parse(multiRecordTestData).toList

        val expected = List(
          InternalRow(
            UTF8String.fromString("testId"),
            null,
            // this is the extra malformed record column with everything read as string
            UTF8String.fromString("[testId,1483400968812]")
          ),
          InternalRow(
            UTF8String.fromString("testId2"),
            null,
            // this is the extra malformed record column with everything read as string
            UTF8String.fromString("[testId2,1483400968906]")
          )
        )
        result shouldEqual expected
      }

      "should treat null in non-nullable as malformed record when enforceNullabilityConstraints=true in PERMISSIVE" in {
        val f = fixture; import f._

        // a.b.d is non-nullable in the schema
        val schema = nestedStructFailSchema.add(StructField("_corrupt_record", StringType))

        val converter = IonConverter(schema, options_permissive_enforceNullabilityConstraints)

        val result = converter.parse(nestedStructTestData).toList

        val expected = List(
          InternalRow(
            InternalRow(
              InternalRow(null), // schema says this should be non-nullable, PERMISSIVE lets it pass but marks as malformed
              InternalRow(UTF8String.fromString("defined1"))
            ),
            // this is the extra malformed record column with everything read as string
            UTF8String.fromString("[[[null],[defined1]]]")
          ),
          InternalRow(
            InternalRow(
              InternalRow(UTF8String.fromString("defined2")),
              InternalRow(UTF8String.fromString("defined3"))
            ),
            // the second row isn't malformed so the additional column is null
            null
          )
        )
        result shouldEqual expected
      }
    }

    def fixture = new {
      val ionSystem: IonSystem = IonSystemBuilder.standard().build()

      val multiRecordTestData: IonReader = {
        createIonReader(
          """
            |{
            |    testId:"testId",
            |    testTimestamp:1483400968812,
            |    testName:"testName",
            |    testNestedStruct:{
            |        testNestedStruct2:{
            |            testNestedStruct3:{
            |                testNestedStructField:"testNestedStructField"
            |            }
            |        }
            |    }
            |}
            |{
            |    testId:"testId2",
            |    testTimestamp:1483400968906,
            |    testName:null,
            |    testNestedStruct:{
            |        testNestedStruct2:{
            |            testNestedStruct3:{
            |                testNestedStructField:null
            |            }
            |        }
            |    }
            |}
            |""".stripMargin
        )
      }

      val nestedStructTestData: IonReader = {
        createIonReader(
          """
            |{
            |    a:{
            |        b:{
            |            d:null
            |        },
            |        c: {
            |            d:"defined1"
            |        }
            |    }
            |}
            |{
            |    a:{
            |        b:{
            |            d:"defined2"
            |        },
            |        c: {
            |            d:"defined3"
            |        }
            |    }
            |}
            |""".stripMargin
        )
      }

      val nonStructData: IonReader = {
        createIonReader(""" "just a string" """)
      }

      val nestedStructPassSchema: StructType = StructType(
        Seq(
          StructField(
            "a",
            StructType(
              Seq(
                StructField("b",
                            StructType(
                              Seq(
                                StructField("d", StringType)
                              )
                            )),
                StructField("c",
                            StructType(
                              Seq(
                                StructField("d", StringType, nullable = false)
                              )
                            ))
              )
            )
          )
        )
      )

      val nestedStructFailSchema: StructType = StructType(
        Seq(
          StructField(
            "a",
            StructType(
              Seq(
                StructField("b",
                            StructType(
                              Seq(
                                StructField("d", StringType, nullable = false)
                              )
                            )),
                StructField("c",
                            StructType(
                              Seq(
                                StructField("d", StringType)
                              )
                            ))
              )
            )
          )
        )
      )

      val defaultIonOptions = new IonOptions(CaseInsensitiveMap(Map.empty))

      val options_failFast: IonOptions = new IonOptions(Map("parserMode" -> "FAILFAST"))
      val options_permissive = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "PERMISSIVE")))
      val options_permissive_customMalformedName = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "columnNameOfCorruptRecord" -> "customMalformedName"
          )))

      val options_dropMalformed: IonOptions = new IonOptions(Map("parserMode" -> "DROPMALFORMED"))

      val options_failFast_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "FAILFAST",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_failFast_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "FAILFAST",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_permissive_allowCoercionToString_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_permissive_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_permissive_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "PERMISSIVE",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val options_dropMalformed_allowCoercionToString = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "DROPMALFORMED",
            "allowCoercionToString" -> "true",
            "enforceNullabilityConstraints" -> "false"
          )))

      val options_dropMalformed_enforceNullabilityConstraints = new IonOptions(
        CaseInsensitiveMap(
          Map(
            "parserMode" -> "DROPMALFORMED",
            "allowCoercionToString" -> "false",
            "enforceNullabilityConstraints" -> "true"
          )))

      val rowWithNull = new GenericInternalRow(1)
      rowWithNull.update(0, null)
    }
  }
}
