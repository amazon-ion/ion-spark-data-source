// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion._
import com.amazon.ion.system.{IonBinaryWriterBuilder, IonReaderBuilder, IonSystemBuilder}
import com.amazon.ionpathextraction.pathcomponents.Wildcard
import com.amazon.ionpathextraction.{PathExtractor, PathExtractorBuilder}
import com.amazon.spark.ion.Loggable
import org.apache.spark.sql.types._
import java.io.{ByteArrayOutputStream, Closeable, InputStream}
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiFunction

import org.apache.spark.TaskContext

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Extract Ion data from an iterator/InputStream either as binary (with no transformation)
  * or as IonValues after applying some schema with "column_path" expressions to extract and
  * "pull-up" nested fields into the top-level of the IonValues.
  *
  * The latter "extraction" functionality was used to work around the lack of nested schema pruning
  * when reading Ion files, such that the user would use the column_path expressions to bind top-level
  * Spark schema fields to nested elements in IonValues which then lets Spark prune them out.
  *
  * As of March 2022, there's support for Ion nested schema pruning in EMR and thus this feature is
  * no longer necessary. Note that the use of this feature EXCLUDES "skip-scan" parsing, which is
  * parser optimization to only read the needed fields (per schema) from the input data, and that's
  * because the Ion path extractor has to materialize the entire input record into the Ion DOM in
  * order to pull-up nested fields to the top-level.
  *
  * This feature was primarily used by CDW users running directly on EMR and using SIDS.
  */
class IonExtractor(schema: StructType) extends Loggable {

  case class Context(currentRow: AtomicReference[IonStruct], results: IonList)

  private val ionSystem = IonSystemBuilder.standard.build
  private val ionReaderBuilder = IonReaderBuilder.standard()
  private val schemaFields = schema.fieldNames.toSet
  private val wildCardColumnName = "*"

  private def buildPathExtractor(schema: StructType): PathExtractor[Context] = {
    val pathExtractorBuilder = PathExtractorBuilder.standard[Context]()
    pathExtractorBuilder.withSearchPath("()", rowInitParser())
    val searchPathByColumnName: Map[String, String] = compileSearchPath(schema)
    searchPathByColumnName.foreach(x => {
      val callback: BiFunction[IonReader, Context, Integer] = getSearchPathCallback(x._1)

      pathExtractorBuilder.withSearchPath(x._2, callback)
    })
    pathExtractorBuilder.build()
  }

  private val pathExtractor: PathExtractor[Context] = buildPathExtractor(schema)

  def extractData(ion: InputStream): Iterator[IonValue] = {
    extractDataAsIonList(ion).iterator().asScala
  }

  def extractData(ion: Array[Byte]): Iterator[IonValue] = {
    extractDataAsIonList(ion).iterator().asScala
  }

  def extractAsBlob(ionValues: Iterator[IonValue]): Iterator[Array[Byte]] = toByteArrayIterator(ionValues)

  def extractAsBlob(ion: InputStream): Iterator[Array[Byte]] = {
    val stream = new ByteArrayOutputStream()
    val ionWriterBuilder = IonBinaryWriterBuilder.standard()
    val ionReader: IonReader = getIonReader(ion)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => ionReader.close()))

    new Iterator[Array[Byte]] with Closeable {
      // Note that the default value (_) is null, because java.lang.Boolean is a wrapper (box) class of boolean value type
      private var _hasNext: java.lang.Boolean = _

      override def hasNext: Boolean = {
        // hasNext() may be called multiple times (before next()) per spec, but we shouldn't call ionReader.next multiple times!
        if (_hasNext == null) {
          val ionType = ionReader.next()
          _hasNext = ionType != null
        }
        _hasNext
      }

      override def next(): Array[Byte] = {
        val ionWriter: IonWriter = ionWriterBuilder.build(stream)

        try {
          // if hasNext() isn't called before next() on the iterator, we must still advance the Ion reader
          if (_hasNext == null) ionReader.next()
          ionWriter.writeValue(ionReader)
          ionWriter.finish()
          stream.toByteArray
        } finally {
          _hasNext = null
          ionWriter.close()
          stream.reset()
        }
      }

      // No need to close the IonReader here since it's closed in the addTaskCompletionListener handler above,
      // which is called at the end of a task (whether succeeded, failed or canceled):
      // https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/TaskContext.html#addTaskCompletionListener(scala.Function1)
      override def close(): Unit = {}
    }
  }

  private def toByteArrayIterator(ionValues: Iterator[IonValue]): Iterator[Array[Byte]] = ionValues.map(binarySerialize)

  private def extractDataAsIonList(ion: Any): IonList = {
    val ionReader: IonReader = getIonReader(ion)

    try {
      val currentRow: AtomicReference[IonStruct] = new AtomicReference[IonStruct](null)
      val result: IonList = ionSystem.newEmptyList()

      val context: Context = Context(currentRow, result)

      pathExtractor.`match`(ionReader, context)

      val lastValue = context.currentRow.get
      if (lastValue != null) {
        context.results.add(lastValue)
      }
      result
    } finally {
      ionReader.close()
    }
  }

  private def getIonReader(ion: Any): IonReader = {
    ion match {
      case array: Array[Byte] => ionReaderBuilder.build(array)
      case str: String        => ionReaderBuilder.build(str)
      case is: InputStream    => ionReaderBuilder.build(is)
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected type ${ion.getClass}, " +
            s"StringType and BinaryType are the only supported types")
    }
  }

  private def compileSearchPath(schema: StructType): Map[String, String] = {
    val selectColumns: mutable.Map[String, String] = mutable.Map()
    var foundColWithCustomPath = false

    schema.foreach(field => {
      if (field.metadata.contains("column_path")) {
        selectColumns += (field.name -> field.metadata.getStringArray("column_path").mkString("('", "' '", "')"))
        foundColWithCustomPath = true
      } else {
        selectColumns += (field.name -> s"('${field.name}')")
      }
    })

    // Optimization to prevent the PathExtractor from trying to match each SearchPath with each field
    if (!foundColWithCustomPath) {
      log.info("Using wildcard search path")
      selectColumns.clear()
      selectColumns += (wildCardColumnName -> s"( ${Wildcard.TEXT} )")
    }

    selectColumns.toMap
  }

  private def rowInitParser(): BiFunction[IonReader, Context, Integer] = {
    new BiFunction[IonReader, Context, Integer] {
      override def apply(ionReader: IonReader, context: Context): Integer = {
        val previousStruct = context.currentRow.getAndSet(ionSystem.newEmptyStruct())
        if (previousStruct != null) {
          context.results.add(previousStruct)
        }
        0
      }
    }
  }

  private def getSearchPathCallback(columnName: String): BiFunction[IonReader, Context, Integer] = {
    if (wildCardColumnName.equals(columnName)) {
      log.info(s"Using wildcard callback handler for column $columnName")
      new BiFunction[IonReader, Context, Integer] {
        override def apply(ionReader: IonReader, context: Context): Integer = {
          val columnName = ionReader.getFieldName
          if (schemaFields.contains(columnName)) {
            handleMatchCallback(columnName, ionReader, context)
          } else {
            0
          }
        }
      }
    } else {
      new BiFunction[IonReader, Context, Integer] {
        override def apply(ionReader: IonReader, context: Context): Integer = {
          handleMatchCallback(columnName, ionReader, context)
        }
      }
    }
  }

  private def handleMatchCallback(columnName: String, ionReader: IonReader, context: Context): Integer = {
    val ionStruct: IonStruct = context.currentRow.get()
    val ionValue: IonValue = ionStruct.getSystem.newValue(ionReader)

    if (ionValue.isNullValue) ionStruct.put(columnName, null) else ionStruct.put(columnName, ionValue)
    0
  }

  private def binarySerialize(ion: IonValue): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val ionWriter = ionSystem.newBinaryWriter(baos)
    ion.writeTo(ionWriter)
    ionWriter.close()
    baos.toByteArray
  }

}

object IonExtractor {
  def isExtractionRequired(schema: StructType): Boolean = {
    schema.exists(_.metadata.contains("column_path"))
  }
}
