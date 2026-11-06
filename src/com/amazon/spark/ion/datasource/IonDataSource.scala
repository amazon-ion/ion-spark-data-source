// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.parser.{IonConverter, IonExtractor}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, PartitionedFile}
import org.apache.spark.sql.types.StructType

import java.io.InputStream

import scala.util.control.NonFatal

class IonDataSource(options: IonOptions, conf: Configuration, isExtractionRequired: Boolean) extends Serializable {

  def readFile(file: PartitionedFile, schema: StructType, converter: IonConverter): Iterator[InternalRow] = {
    if (isExtractionRequired && options.recordBlobFieldName.isDefined) {
      throw new IllegalStateException(
        "IonFileFormat cannot read data using both column_path extraction and blob mode extraction")
    }

    val path = new Path(file.filePath.toUri)
    val inputStream = CodecStreams.createInputStreamWithCloseResource(conf, path)

    // With ignoreCorruptFiles, FileScanRDD skips a file whose reader fails to initialize and never gets an
    // iterator to close, so the stream must be closed here rather than at task end.
    val rows =
      try {
        (isExtractionRequired, options.recordBlobFieldName) match {
          case (true, _) =>
            readFileWithColumnPathExtraction(schema, converter, inputStream)
          case (false, Some(blobFieldName)) if schema.fields.exists(x => x.name == blobFieldName) =>
            readFileWithBlobDataColumnExtraction(schema, converter, inputStream, blobFieldName)
          case _ =>
            converter.parse(inputStream)
        }
      } catch {
        case NonFatal(e) =>
          inputStream.close()
          throw e
      }

    // FileScanRDD closes a Closeable iterator before opening the next file; the task completion listener
    // registered above stays as the fallback for failed or cancelled tasks.
    new CloseOnExhaustionIterator(rows, inputStream)
  }

  private def readFileWithColumnPathExtraction(schema: StructType,
                                               converter: IonConverter,
                                               inputStream: InputStream): Iterator[InternalRow] = {
    val ionExtractor: IonExtractor = new IonExtractor(schema)
    ionExtractor.extractData(inputStream).map(converter.parse).flatMap(f => f.toList)
  }

  private def readFileWithBlobDataColumnExtraction(schema: StructType,
                                                   converter: IonConverter,
                                                   inputStream: InputStream,
                                                   blobFieldName: String): Iterator[InternalRow] = {
    val ionExtractor: IonExtractor = new IonExtractor(schema)

    val byteArrayToInternalRowConverter: Array[Byte] => InternalRow = {
      // If only reading the blob column, store the full row extracted
      // by IonExtractor as Array[Byte] in the designated blob column.
      if (schema.length == 1) { data: Array[Byte] =>
        val row = new GenericInternalRow(1)
        row.update(0, data)
        row
      } else {
        // If also reading other columns, do the above and also invoke
        // IonConverter on the Array[Byte] to parse the needed columns.
        val blobFieldIndex = schema.fieldIndex(blobFieldName)
        data: Array[Byte] =>
          val row = converter.parse(data).head
          row.update(blobFieldIndex, data)
          row
      }
    }

    ionExtractor
      .extractAsBlob(inputStream)
      .map(byteArrayToInternalRowConverter)
  }
}
