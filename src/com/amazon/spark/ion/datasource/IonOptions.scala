// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.Loggable
import com.amazon.spark.ion.datasource.IonOptions.{
  ALLOW_COERCION_TO_BLOB_SPARK_ARG,
  ALLOW_COERCION_TO_STRING_SPARK_ARG,
  ION_WRITE_COMPRESSION_CODEC_CONF_KEY,
  ION_WRITE_COMPRESSION_CODEC_DEFAULT,
  OUTPUT_BUFFER_SIZE
}
import com.amazon.spark.ion.parser.Constants.COLUMN_NAME_OF_CORRUPT_RECORD
import com.amazon.spark.ion.parser.ParserMode.{DropMalformed, FailFast, Permissive}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}
import org.apache.spark.sql.internal.SQLConf

class IonOptions(@transient val parameters: CaseInsensitiveMap[String]) extends Serializable with Loggable {

  def this(parameters: Map[String, String]) = {
    this(CaseInsensitiveMap[String](parameters))
  }

  /**
    * Defaults to GZIP compression and supports All Spark's CompressionCodecs
    * Reference: https://tinyurl.com/28hf6dd8
    */
  val compressionCodec: Option[String] = {
    if (parameters.contains("compression")) {
      parameters.get("compression").map(CompressionCodecs.getCodecClassName)
    } else {
      val codec = Option(
        SQLConf.get.getConfString(ION_WRITE_COMPRESSION_CODEC_CONF_KEY, ION_WRITE_COMPRESSION_CODEC_DEFAULT))
      val codecClassName = codec.map(CompressionCodecs.getCodecClassName)
      codecClassName
    }
  }

  val serialization: String = parameters.getOrElse("serialization", "binary")

  /**
    * When true, Ion writer will assume the FIRST column given to DataFrameWriter is an Ion blob column
    * and will extract it and write it to the output as an Ion record (i.e. not a single "blob" column).
    * Note that the DataFrame may have columns other than the blob for cases when it's needed to bucketBy,
    * sortBy or partitionBy on the non-blob columns but only want to write the blob column as an Ion record.
    */
  val recordAsByteArray: Boolean =
    parameters.get("recordAsByteArray").exists(_.toBoolean)

  /**
    * When set, Ion reader will read the entire row content as Ion binary data (blob) and store it into
    * the BinaryType column whose name is defined in this parameter. The column must be in the schema provided
    * to the DataFrameReader. Note that the schema may have columns other than the blob for cases when it's needed
    * to load bucket-sorted data because Spark requires that the bucket-sort columns used in the BucketSpec to be
    * read from the source (i.e. in FileScan) cto leverage bucket-sort optimizations.
    */
  val recordBlobFieldName: Option[String] = parameters.get("recordBlobFieldName")

  /** Defaults to false */
  val allowCoercionToString: Boolean = {
    val valueFromParams = parameters.get("allowCoercionToString")
    val valueFromSparkArgs = SQLConf.get.getConfString(ALLOW_COERCION_TO_STRING_SPARK_ARG, "false")

    valueFromParams.getOrElse(valueFromSparkArgs).toBoolean
  }

  /** Defaults to false */
  val allowCoercionToBlob: Boolean = {
    val valueFromParams = parameters.get("allowCoercionToBlob")
    val valueFromSparkArgs = SQLConf.get.getConfString(ALLOW_COERCION_TO_BLOB_SPARK_ARG, "false")

    valueFromParams.getOrElse(valueFromSparkArgs).toBoolean
  }

  /** Defaults to false */
  val enforceNullabilityConstraints: Boolean =
    parameters.get("enforceNullabilityConstraints").exists(_.toBoolean)

  /** This is being kept only for backward compatibility and will simply translate to PERMISSIVE mode */
  private val ignoreIonConversionErrors: Boolean =
    parameters.get("ignoreIonConversionErrors").exists(_.toBoolean)

  /** Defaults to -1 */
  val outputBufferSize: Long = SQLConf.get.getConfString(OUTPUT_BUFFER_SIZE, "-1").toLong

  /** Defaults to FAILFAST */
  val parserMode: String = {
    val value = parameters.get("mode").orElse(parameters.get("parserMode"))

    value match {
      case None if ignoreIonConversionErrors => Permissive
      case None                              => FailFast
      case Some("FAILFAST")                  => FailFast
      case Some("FAIL_FAST")                 => FailFast
      case Some("FailFast")                  => FailFast
      case Some("DROPMALFORMED")             => DropMalformed
      case Some("DROP_MALFORMED")            => DropMalformed
      case Some("DropMalformed")             => DropMalformed
      case Some("PERMISSIVE")                => Permissive
      case Some("Permissive")                => Permissive
      case other =>
        log.warn(s"Unknown parser mode: $other, defaulting to FAILFAST")
        FailFast
    }
  }

  /** Defaults to _corrupt_record */
  val columnNameOfCorruptRecord: String = {
    val value1 = parameters.get("columnNameOfCorruptRecord")
    val value2 = parameters.get("columnNameOfMalformedRecord") // For wider support
    // Default Spark is _corrupt_record unless overridden
    val defaultSpark = SQLConf.get.getConfString(COLUMN_NAME_OF_CORRUPT_RECORD, "_corrupt_record")

    value1.getOrElse(value2.getOrElse(defaultSpark))
  }
}

object IonOptions {
  val ALLOW_COERCION_TO_STRING_SPARK_ARG = "spark.ion.allowCoercionToString"

  val ALLOW_COERCION_TO_BLOB_SPARK_ARG = "spark.ion.allowCoercionToBlob"

  /**
    * By default this option is set to -1, which means flush feature is not enabled.
    * It is only supported when using recordAsByteArray and a buffer size greater than -1.
    */
  val OUTPUT_BUFFER_SIZE = "spark.ion.output.bufferSize"

  val ION_WRITE_COMPRESSION_CODEC_CONF_KEY = "spark.ion.write.compressionCodec"

  val ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY = "spark.sql.extensions.enableIonUnzipOptimizer"

  val ION_WRITE_COMPRESSION_CODEC_DEFAULT = "gzip"
}
