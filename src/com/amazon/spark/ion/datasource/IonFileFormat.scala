// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.Loggable
import com.amazon.spark.ion.datasource.IonFileFormat.{
  IS_BLOB_DATA_MARKER_METADATA,
  getBlobFieldSchema,
  updateSchemaWithBlobDataMarking
}
import com.amazon.spark.ion.parser.{IonConverter, IonExtractor}
import com.amazon.spark.ion.util.{ConfigSerDe, HadoopConfigProvider}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources.{
  CodecStreams,
  FileFormat,
  OutputWriter,
  OutputWriterFactory,
  PartitionedFile
}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types._

/**
  * Ion datasource to read and write Ion format data using Spark.
  */
class IonFileFormat extends FileFormat with DataSourceRegister with Loggable {

  override def shortName(): String = "ion"

  /**
    * If user does not provide a schema, and recordBlobFieldName is set, then
    * each record is read as Ion bytes, represented by 'recordBlobFieldName' column
    */
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    val parsedOption: IonOptions = new IonOptions(options)

    parsedOption.recordBlobFieldName.map(x => StructType(getBlobFieldSchema(x) :: Nil))
  }

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    false
  }

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    val configuration = job.getConfiguration
    val parsedOptions: IonOptions = new IonOptions(options)

    parsedOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(configuration, codec)
    }

    val recordAsByteArray = parsedOptions.recordAsByteArray

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = {
        ".ion" + CodecStreams.getCompressionExtension(context)
      }

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        val outputSchema = getOutputSchema(dataSchema, recordAsByteArray)

        new IonOutputWriter(path, outputSchema, parsedOptions, context)
      }

      private def getOutputSchema(dataSchema: StructType, recordAsByteArray: Boolean): StructType = {
        if (recordAsByteArray) {
          val blobDataColumnIsAlreadyMarked =
            dataSchema.exists(x =>
              x.metadata.contains(IS_BLOB_DATA_MARKER_METADATA) && x.metadata.getBoolean(IS_BLOB_DATA_MARKER_METADATA))
          if (!blobDataColumnIsAlreadyMarked) {
            updateSchemaWithBlobDataMarking(dataSchema)
          } else {
            dataSchema
          }
        } else {
          dataSchema
        }
      }
    }
  }

  override def toString: String = "ION"

  override protected def buildReader(sparkSession: SparkSession,
                                     dataSchema: StructType,
                                     partitionSchema: StructType,
                                     requiredSchema: StructType,
                                     filters: Seq[Filter],
                                     options: Map[String, String],
                                     hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val configSerDe = new ConfigSerDe(hadoopConf)
    val parsedOptions: IonOptions = new IonOptions(options)

    // Even when reading in blob mode (when recordBlobFieldName is defined), it's possible that the requiredSchema
    // may be empty (e.g. for queries/actions like count() which don't need any data) or not empty but not actually have
    // the blob column itself (e.g. job that needs blob mode for some queries/actions but not others). Recall that
    // requiredSchema is not the full schema, it's whatever is being requested for the current query/action being
    // executed, since other columns are removed by Spark optimizers / schema pruning. We log a warning just for info.

    parsedOptions.recordBlobFieldName
      .filterNot(x => requiredSchema.exists(_.name == x))
      .foreach(x => {
        println(
          s"recordBlobFieldName is set ($x) but the blob column is not in the required schema: " +
            s"${requiredSchema.names.mkString("Array(", ", ", ")")}")
      })

    HadoopConfigProvider.init
    val isExtractionRequired = IonExtractor.isExtractionRequired(requiredSchema)

    // This lambda is serialized and dispatched to the executors, then executed for each file being read.
    // Keep that in mind in terms of memory consumption.
    file: PartitionedFile =>
      {
        // To conserve memory, we only want one copy of the configuration (which is a large object) per jvm
        val configuration = HadoopConfigProvider.getConf(configSerDe)

        val converter = IonConverter(requiredSchema, parsedOptions)

        new IonDataSource(parsedOptions, configuration, isExtractionRequired).readFile(file, requiredSchema, converter)
      }
  }
}

object IonFileFormat {

  val IS_PARTITION_KEY_MARKER_METADATA: String = "is_partition_key"
  val IS_BUCKET_KEY_MARKER_METADATA: String = "is_bucket_key"
  val IS_SORT_KEY_MARKER_METADATA: String = "is_sort_key"
  val IS_BLOB_DATA_MARKER_METADATA: String = "is_blob_data"

  def getBlobFieldSchema(recordBlobFieldName: String): StructField = {
    StructField(recordBlobFieldName,
                DataTypes.BinaryType,
                nullable = false,
                new MetadataBuilder()
                  .putBoolean(IS_BLOB_DATA_MARKER_METADATA, value = true)
                  .build())
  }

  def updateSchemaWithBlobDataMarking(dataSchema: StructType): StructType = {
    val updatedSchema = dataSchema.map { field =>
      if (field.dataType.isInstanceOf[BinaryType]) {
        val updatedMetadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .withMetadata(new MetadataBuilder().putBoolean(IS_BLOB_DATA_MARKER_METADATA, value = true).build())
          .build()
        StructField(field.name, field.dataType, field.nullable, updatedMetadata)
      } else {
        field
      }
    }
    StructType(updatedSchema)
  }
}
