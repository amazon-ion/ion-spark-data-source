// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.datasource.IonOptions.{
  ION_WRITE_COMPRESSION_CODEC_CONF_KEY,
  ION_WRITE_COMPRESSION_CODEC_DEFAULT
}
import com.amazon.spark.ion.parser.ParserMode
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IonOptionsSpec extends AnyFlatSpec with Matchers {

  behavior of "IonOptions"

  it should "extract compressionCodec" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("compression" -> "gzip")))
    val compressionCodec = options.compressionCodec.get
    compressionCodec shouldEqual CompressionCodecs.getCodecClassName("gzip")
  }

  it should "default compressionCodec to GZIP if no compression option is provided" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val compressionCodec = options.compressionCodec.get
    compressionCodec shouldEqual CompressionCodecs.getCodecClassName(ION_WRITE_COMPRESSION_CODEC_DEFAULT)
  }

  it should "support other compression formats supported by Spark's CompressionCodecs" in {
    SQLConf.get.setConfString(ION_WRITE_COMPRESSION_CODEC_CONF_KEY, "lz4")
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val compressionCodec = options.compressionCodec.get
    compressionCodec shouldEqual CompressionCodecs.getCodecClassName("lz4")
  }

  it should "throw IllegalArgumentException when an unsupported compression format is passed" in {
    intercept[IllegalArgumentException] {
      SQLConf.get.setConfString(ION_WRITE_COMPRESSION_CODEC_CONF_KEY, "unsupported_format")
      val options = new IonOptions(CaseInsensitiveMap(Map()))
      val compressionCodec = options.compressionCodec
    }
    SQLConf.get.setConfString(ION_WRITE_COMPRESSION_CODEC_CONF_KEY, ION_WRITE_COMPRESSION_CODEC_DEFAULT)
  }

  it should "extract serialization" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("serialization" -> "text")))
    val serialization = options.serialization
    serialization shouldEqual "text"
  }

  it should "default serialization to binary" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val serialization = options.serialization
    serialization shouldEqual "binary"
  }

  it should "extract recordAsByteArray" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("recordAsByteArray" -> "true")))
    val recordAsByteArray = options.recordAsByteArray
    recordAsByteArray shouldBe true
  }

  it should "default recordAsByteArray to false" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val recordAsByteArray = options.recordAsByteArray
    recordAsByteArray shouldBe false
  }

  it should "extract recordBlobFieldName" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("recordBlobFieldName" -> "testBlobFieldName")))
    val recordBlobFieldName = options.recordBlobFieldName.get
    recordBlobFieldName shouldEqual "testBlobFieldName"
  }

  it should "extract allowCoercionToString" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("allowCoercionToString" -> "true")))
    val allowCoercionToString = options.allowCoercionToString
    allowCoercionToString shouldBe true
  }

  it should "extract allowCoercionToString from Spark args" in {
    SQLConf.get.setConfString("spark.ion.allowCoercionToString", "true")
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val allowCoercionToString = options.allowCoercionToString
    SQLConf.get.unsetConf("spark.ion.allowCoercionToString")

    allowCoercionToString shouldBe true
  }

  it should "default allowCoercionToString to false" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val allowCoercionToString = options.allowCoercionToString
    allowCoercionToString shouldBe false
  }

  it should "extract allowCoercionToBlob" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("allowCoercionToBlob" -> "true")))
    val allowCoercionToBlob = options.allowCoercionToBlob
    allowCoercionToBlob shouldBe true
  }

  it should "extract allowCoercionToBlob from Spark args" in {
    SQLConf.get.setConfString("spark.ion.allowCoercionToBlob", "true")
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val allowCoercionToBlob = options.allowCoercionToBlob
    SQLConf.get.unsetConf("spark.ion.allowCoercionToBlob")

    allowCoercionToBlob shouldBe true
  }

  it should "default allowCoercionToBlob to false" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val allowCoercionToBlob = options.allowCoercionToBlob
    allowCoercionToBlob shouldBe false
  }

  it should "extract outputBufferSize from Spark args" in {
    SQLConf.get.setConfString("spark.ion.output.bufferSize", "1")
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val enableFlushWhileWrite = options.outputBufferSize
    SQLConf.get.unsetConf("spark.ion.output.bufferSize")

    enableFlushWhileWrite shouldBe 1
  }

  it should "default outputBufferSize to -1" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val enableFlushWhileWrite = options.outputBufferSize
    enableFlushWhileWrite shouldBe -1
  }

  it should "extract enforceNullabilityConstraints" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("enforceNullabilityConstraints" -> "true")))
    val enforceNullabilityConstraints = options.enforceNullabilityConstraints
    enforceNullabilityConstraints shouldBe true
  }

  it should "default enforceNullabilityConstraints to false" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val enforceNullabilityConstraints = options.enforceNullabilityConstraints
    enforceNullabilityConstraints shouldBe false
  }

  it should "extract parserMode given FAILFAST" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "FAILFAST")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.FailFast
  }

  it should "extract parserMode given FAIL_FAST" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "FAIL_FAST")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.FailFast
  }

  it should "extract parserMode given FailFast" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "FailFast")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.FailFast
  }

  it should "extract parserMode given DROPMALFORMED" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "DROPMALFORMED")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.DropMalformed
  }

  it should "extract parserMode given DROP_MALFORMED" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "DROP_MALFORMED")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.DropMalformed
  }

  it should "extract parserMode given DropMalformed" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "DropMalformed")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.DropMalformed
  }

  it should "extract parserMode given PERMISSIVE" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "PERMISSIVE")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.Permissive
  }

  it should "extract parserMode given Permissive" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "Permissive")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.Permissive
  }

  it should "default parserMode to FAILFAST given an invalid value" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("parserMode" -> "invalid")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.FailFast
  }

  it should "default parserMode to Permissive when ignoreIonConversionErrors is enabled and no parserMode/mode is specified" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("ignoreIonConversionErrors" -> "true")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.Permissive
  }

  it should "default parserMode to FailFast and no parserMode/mode is specified" in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.FailFast
  }

  it should "extract parserMode when parserMode is not specified but mode is" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("mode" -> "Permissive")))
    val parserMode = options.parserMode
    parserMode shouldEqual ParserMode.Permissive
  }

  it should "extract columnNameOfCorruptRecord given columnNameOfCorruptRecord" in {
    val options = new IonOptions(CaseInsensitiveMap(Map("columnNameOfCorruptRecord" -> "test_column_cr")))
    val columnNameOfCorruptRecord = options.columnNameOfCorruptRecord
    columnNameOfCorruptRecord shouldEqual "test_column_cr"
  }

  it should "extract columnNameOfCorruptRecord given columnNameOfMalformedRecord " in {
    val options = new IonOptions(CaseInsensitiveMap(Map("columnNameOfMalformedRecord" -> "test_column_mr")))
    val columnNameOfCorruptRecord = options.columnNameOfCorruptRecord
    columnNameOfCorruptRecord shouldEqual "test_column_mr"
  }

  it should "default to Spark's columnNameOfCorruptRecord when not given either columnNameOfCorruptRecord or columnNameOfMalformedRecord " in {
    val options = new IonOptions(CaseInsensitiveMap(Map()))
    val columnNameOfCorruptRecord = options.columnNameOfCorruptRecord
    columnNameOfCorruptRecord shouldEqual "_corrupt_record"
  }

}
