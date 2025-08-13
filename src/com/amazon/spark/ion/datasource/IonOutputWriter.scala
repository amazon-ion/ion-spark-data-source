// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.parser.IonGenerator
import java.io.OutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types.StructType

private[datasource] class IonOutputWriter(path: String,
                                          dataSchema: StructType,
                                          options: IonOptions,
                                          context: TaskAttemptContext)
    extends OutputWriter {

  private val outStream: OutputStream = CodecStreams.createOutputStream(context, new Path(path))

  private val ionGenerator: IonGenerator = new IonGenerator(dataSchema, outStream, options)

  override def write(row: InternalRow): Unit = {
    ionGenerator.write(row)
  }

  override def close(): Unit = ionGenerator.close()

  def path(): String = path
}
