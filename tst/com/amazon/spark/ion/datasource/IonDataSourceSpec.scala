// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.{IonInt, IonStruct}
import com.amazon.spark.ion.IonOperations
import com.amazon.spark.ion.parser.IonFunctions.from_ion

import java.io.{ByteArrayOutputStream, File, FilenameFilter}
import java.nio.file.{Files}
import java.sql.{Date, Timestamp}
import java.util.zip.GZIPOutputStream
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer

class IonDataSourceSpec extends AnyFlatSpec {

  "ion format writer" should "run successfully" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i))
    val inputDF = data.toDF("value", "square")

    // Register the datasource so that we can use short-name "ion"
    inputDF.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .mode(SaveMode.Overwrite)
      .save(path)

    val files = listIonFiles(path)

    val head = files.head

    val actualData = ionSystem.getLoader.load(head)
    val expectedData = data.collect()

    assertResult(expectedData.length)(actualData.size())

    for (i <- expectedData.indices) {
      val actualRecord: IonStruct = actualData.get(i).asInstanceOf[IonStruct]
      val expectedRecord: (Int, Int) = expectedData(i)

      assertResult(expectedRecord._1)(actualRecord.get("value").asInstanceOf[IonInt].intValue())
      assertResult(expectedRecord._2)(actualRecord.get("square").asInstanceOf[IonInt].intValue())
    }
  }

  "ion format writer" should "run successfully for Ion bytes" in {
    val f = fixture
    import f._
    import spark.implicits._

    val expectedData1 = ionSystem.newEmptyStruct()
    expectedData1.put("ages", ionSystem.newList(Array(1, 2, 3)))

    val expectedData2 = ionSystem.newEmptyStruct()
    expectedData2.put("ages", ionSystem.newList(Array(4, 6, 8)))

    val data: ListBuffer[Array[Byte]] = new ListBuffer[Array[Byte]]

    var baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    var ionWriter = ionSystem.newBinaryWriter(new GZIPOutputStream(baos))

    expectedData1.writeTo(ionWriter)
    ionWriter.close()

    data += baos.toByteArray

    baos = new ByteArrayOutputStream()
    ionWriter = ionSystem.newBinaryWriter(new GZIPOutputStream(baos))

    expectedData2.writeTo(ionWriter)
    ionWriter.close()

    data += baos.toByteArray

    val inputDF = spark.sparkContext.makeRDD(data).toDF("data")

    // Register the datasource so that we can use short-name "ion"
    inputDF.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true)
      .mode(SaveMode.Overwrite)
      .save(path)

    val files = listIonFiles(path)

    val head = files.head

    val actualData = ionSystem.getLoader.load(head)

    assertResult(expectedData1)(actualData.get(0))
    assertResult(expectedData2)(actualData.get(1))
  }

  "ion format writer" should "run successfully for Ion bytes with flush enabled" in {
    val f = fixture
    import f._
    import spark.implicits._

    val expectedData1 = ionSystem.newEmptyStruct()
    expectedData1.put("ages", ionSystem.newList(Array(1, 2, 3)))

    val expectedData2 = ionSystem.newEmptyStruct()
    expectedData2.put("ages", ionSystem.newList(Array(4, 6, 8)))

    val data: ListBuffer[Array[Byte]] = new ListBuffer[Array[Byte]]

    var baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    var ionWriter = ionSystem.newBinaryWriter(new GZIPOutputStream(baos))

    expectedData1.writeTo(ionWriter)
    ionWriter.close()

    data += baos.toByteArray

    baos = new ByteArrayOutputStream()
    ionWriter = ionSystem.newBinaryWriter(new GZIPOutputStream(baos))

    expectedData2.writeTo(ionWriter)
    ionWriter.close()

    data += baos.toByteArray

    val inputDF = spark.sparkContext.makeRDD(data).toDF("data")
    spark.conf.set(IonOptions.OUTPUT_BUFFER_SIZE, value = 1)

    // Register the datasource so that we can use short-name "ion"
    inputDF.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true)
      .mode(SaveMode.Overwrite)
      .save(path)

    val files = listIonFiles(path)

    val head = files.head

    val actualData = ionSystem.getLoader.load(head)

    assertResult(expectedData1)(actualData.get(0))
    assertResult(expectedData2)(actualData.get(1))
  }

  behavior of "ion format reader"

  it should "run successfully" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i))
    val inputDF = data.toDF("value", "square")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), path, SaveMode.Overwrite)

    val readDf = IonOperations.load(spark, Seq(path), Option(schema))

    val actualData = readDf.select($"square")
    val expectedData = inputDF.select($"square")

    assertResult(expectedData.collect())(actualData.collect())
  }

  it should "run successfully for a path that needs escaping" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i))
    val inputDF = data.toDF("value", "square")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), pathNeedsEscaping, SaveMode.Overwrite)

    val readDf = IonOperations.load(spark, Seq(pathNeedsEscaping), Option(schema))

    val actualData = readDf.select($"square")
    val expectedData = inputDF.select($"square")

    assertResult(expectedData.collect())(actualData.collect())
  }

  it should "run successfully for Ion bytes" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i))
    val inputDF: Dataset[Row] = data.toDF("value", "square")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), path, SaveMode.Overwrite)

    val readDf =
      IonOperations.loadWithRawIonData(spark, Seq(path), schema, "blobdata")

    val actualData = readDf
      .withColumn("fields", from_ion(col("blobdata"), schema))
      .select("fields.*")

    val expectedData = inputDF.select($"value", $"square")

    assertResult(expectedData.collect())(actualData.collect())
  }

  it should "run successfully for Ion bytes for a path that needs escaping" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i))
    val inputDF: Dataset[Row] = data.toDF("value", "square")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), pathNeedsEscaping, SaveMode.Overwrite)

    val readDf =
      IonOperations.loadWithRawIonData(spark, Seq(pathNeedsEscaping), schema, "blobdata")

    val actualData = readDf
      .withColumn("fields", from_ion(col("blobdata"), schema))
      .select("fields.*")

    val expectedData = inputDF.select($"value", $"square")

    assertResult(expectedData.collect())(actualData.collect())
  }

  "ion format timestamp reader" should "run successfully" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(Seq(new Timestamp(1596236949761l)))
    val inputDF = data.toDF("value")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), path, SaveMode.Overwrite)

    val readDf = IonOperations.load(spark, Seq(path), Option(schema))

    readDf.show()

    assertResult(readDf.collect())(inputDF.collect())
  }

  "ion format date reader" should "run successfully" in {
    val f = fixture
    import f._
    import spark.implicits._

    val data = spark.sparkContext.makeRDD(Seq(new Date(1596236949761l)))
    val inputDF = data.toDF("value")

    val schema = inputDF.schema

    IonOperations.saveAsText(inputDF, Seq(), Map(), path, SaveMode.Overwrite)

    val readDf = IonOperations.load(spark, Seq(path), Option(schema))

    readDf.show()

    assertResult(readDf.collect())(inputDF.collect())
  }

  def fixture = {
    new {
      val ionSystem = IonSystemBuilder.standard().build()
      val path = Files.createTempDirectory("sids-staging").toString()
      val pathNeedsEscaping = s"$path/needs escaped/"
      val spark =
        SparkSession
          .builder()
          .master("local")
          .appName("SparkTest")
          .config("spark.driver.host", "127.0.0.1")
          .getOrCreate()

      def listIonFiles(directory: String) = {
        val outputDir = new File(directory)
        if (outputDir.exists() && outputDir.isDirectory) {

          outputDir
            .listFiles(new FilenameFilter {
              override def accept(dir: File, name: String): Boolean = {
                name.endsWith(".ion.gz")
              }
            })
            .toList
        } else {
          throw new IllegalArgumentException("Not a directory or empty")
        }
      }
    }
  }

}
