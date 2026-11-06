// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.ion.system.IonSystemBuilder
import com.amazon.spark.ion.{IonOperations, SparkTest}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.file.Files
import java.util.zip.GZIPOutputStream

import scala.collection.mutable.ListBuffer

class IonDataSourceStreamLifecycleSpec extends AnyFlatSpec with Matchers with SparkTest {

  private val fileCount = 24
  private val recordsPerFile = 3

  private val schema = StructType(Seq(StructField("value", IntegerType), StructField("name", StringType)))

  private val tempDirs = ListBuffer.empty[File]

  override def afterEach(): Unit = {
    try super.afterEach()
    finally {
      tempDirs.foreach(FileUtils.deleteDirectory)
      tempDirs.clear()
    }
  }

  behavior of "ion format reader input stream lifecycle"

  it should "close each file's input stream before opening the next one" in {
    assertStreamsClosedPerFile(dir => IonOperations.load(spark, Seq(dir), Option(schema), trackedOptions))
  }

  it should "close each file's input stream before opening the next one in blob mode" in {
    assertStreamsClosedPerFile(
      dir => IonOperations.loadWithRawIonData(spark, Seq(dir), schema, "blobdata", trackedOptions))
  }

  it should "close each file's input stream before opening the next one with column_path extraction" in {
    val columnPathSchema = StructType(
      Seq(
        StructField("value", IntegerType),
        StructField("city", StringType, nullable = true, Metadata.fromJson("""{"column_path":["address","city"]}"""))
      ))

    assertStreamsClosedPerFile(dir => IonOperations.load(spark, Seq(dir), Option(columnPathSchema), trackedOptions))
  }

  it should "close every input stream when the reader stops early" in {
    val dir = writeSmallIonFiles()
    OpenStreamTrackingFileSystem.reset()

    IonOperations.load(spark, Seq(dir), Option(schema), trackedOptions).limit(1).collect()

    OpenStreamTrackingFileSystem.currentlyOpen shouldBe 0
  }

  it should "close the input stream of a corrupt file skipped with ignoreCorruptFiles" in {
    val dir = writeCorruptIonFiles()
    OpenStreamTrackingFileSystem.reset()

    val df = IonOperations.load(spark, Seq(dir), Option(schema), trackedOptions + ("ignoreCorruptFiles" -> "true"))
    val rows = df.collect()

    df.rdd.getNumPartitions should be < fileCount
    rows shouldBe empty
    OpenStreamTrackingFileSystem.openedCount shouldBe fileCount
    OpenStreamTrackingFileSystem.maxConcurrentlyOpen should be <= spark.sparkContext.defaultParallelism
    OpenStreamTrackingFileSystem.currentlyOpen shouldBe 0
  }

  private def trackedOptions: Map[String, String] = OpenStreamTrackingFileSystem.hadoopOptions

  private def writeCorruptIonFiles(): String = {
    val dir = Files.createTempDirectory("ion-stream-lifecycle-corrupt").toFile
    tempDirs += dir
    for (fileIndex <- 0 until fileCount) {
      Files.write(new File(dir, s"part-$fileIndex${OpenStreamTrackingFileSystem.TrackedSuffix}").toPath,
                  s"not gzip and not ion $fileIndex".getBytes("UTF-8"))
    }
    dir.getAbsolutePath
  }

  private def assertStreamsClosedPerFile(read: String => DataFrame): Unit = {
    val dir = writeSmallIonFiles()
    OpenStreamTrackingFileSystem.reset()

    val df = read(dir)
    val rows: Array[Row] = df.collect()

    df.rdd.getNumPartitions should be < fileCount
    rows.length shouldBe fileCount * recordsPerFile
    OpenStreamTrackingFileSystem.openedCount shouldBe fileCount
    OpenStreamTrackingFileSystem.maxConcurrentlyOpen should be <= spark.sparkContext.defaultParallelism
    OpenStreamTrackingFileSystem.currentlyOpen shouldBe 0
  }

  private def writeSmallIonFiles(): String = {
    val ionSystem = IonSystemBuilder.standard().build()
    val dir = Files.createTempDirectory("ion-stream-lifecycle").toFile
    tempDirs += dir

    for (fileIndex <- 0 until fileCount) {
      val out = new GZIPOutputStream(
        new FileOutputStream(new File(dir, s"part-$fileIndex${OpenStreamTrackingFileSystem.TrackedSuffix}")))
      val writer = ionSystem.newBinaryWriter(out)
      try {
        for (recordIndex <- 0 until recordsPerFile) {
          val record = ionSystem.newEmptyStruct()
          record.put("value", ionSystem.newInt(fileIndex * recordsPerFile + recordIndex))
          record.put("name", ionSystem.newString(s"name-$fileIndex-$recordIndex"))
          val address = ionSystem.newEmptyStruct()
          address.put("city", ionSystem.newString(s"city-$fileIndex"))
          record.put("address", address)
          record.writeTo(writer)
        }
      } finally {
        writer.close()
      }
    }
    dir.getAbsolutePath
  }
}
