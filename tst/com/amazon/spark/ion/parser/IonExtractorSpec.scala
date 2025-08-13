// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.parser

import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.{IonDatagram, IonSystem, IonWriter}
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

class IonExtractorSpec extends AnyFlatSpec with Matchers {

  behavior of "IonExtractor"

  it should "extract (with column path schema) successfully from byte array" in {
    val f = fixture; import f._

    writeData(ionDatagram, ionWriter, baos)
    val byteArray = baos.toByteArray
    val ionExtractor = new IonExtractor(DataType.fromJson(jsonSchemaWithColumnPath).asInstanceOf[StructType])

    val ions = ionExtractor.extractData(byteArray)

    ions.toArray shouldEqual ionDatagramAfterExtraction.toArray
  }

  it should "extract (with column path schema) successfully from input stream" in {
    val f = fixture; import f._

    writeData(ionDatagram, ionWriter, baos)
    val byteArray = baos.toByteArray
    val inputStream = new ByteArrayInputStream(byteArray)
    val ionExtractor = new IonExtractor(DataType.fromJson(jsonSchemaWithColumnPath).asInstanceOf[StructType])

    val ions = ionExtractor.extractData(inputStream)

    ions.toArray shouldEqual ionDatagramAfterExtraction.toArray
  }

  it should "extract successfully as binary blob from ion values" in {
    val f = fixture; import f._

    val ionExtractor = new IonExtractor(blobSchema)

    val expected = ionDatagram.iterator().asScala.toList

    val results = ionExtractor.extractAsBlob(expected.toIterator)
    val actual = results
      .map(ba => {
        ionSystem.singleValue(ba)
      })
      .toList

    actual shouldEqual expected
  }

  it should "extract successfully as binary blob from input stream" in {
    val f = fixture; import f._

    val ionExtractor = new IonExtractor(blobSchema)
    writeData(ionDatagram, ionWriter, baos)
    val byteArray = baos.toByteArray
    val inputStream = new ByteArrayInputStream(byteArray)

    val expected = ionDatagram.iterator().asScala.toList

    val results = ionExtractor.extractAsBlob(inputStream)
    val actual = results
      .map(ba => {
        ionSystem.singleValue(ba)
      })
      .toList

    actual shouldEqual expected
  }

  it should "create a custom ion reading iterator that is idempotent on hasNext" in {
    val f = fixture; import f._

    val ionExtractor = new IonExtractor(blobSchema)
    writeData(ionDatagram, ionWriter, baos)
    val byteArray = baos.toByteArray
    val inputStream = new ByteArrayInputStream(byteArray)

    val expected = ionDatagram.iterator().asScala.toList

    val iterator = ionExtractor.extractAsBlob(inputStream)

    // multiple calls to hasNext() before next()
    iterator.hasNext
    iterator.hasNext
    iterator.hasNext
    ionSystem.singleValue(iterator.next()) shouldEqual expected(0)

    // multiple calls to hasNext() before next()
    iterator.hasNext
    iterator.hasNext
    ionSystem.singleValue(iterator.next()) shouldEqual expected(1)

    // 0 calls to hasNext() before next()
    ionSystem.singleValue(iterator.next()) shouldEqual expected(2)
  }

  private def writeData(table: IonDatagram, ionWriter: IonWriter, baos: ByteArrayOutputStream): Unit = {
    table.writeTo(ionWriter)
    closeAll(ionWriter, baos)
  }

  private def closeAll(ionWriter: IonWriter, baos: ByteArrayOutputStream): Unit = {
    ionWriter.close()
    baos.close()
  }

  private def fixture = new {
    val ionSystem: IonSystem = IonSystemBuilder.standard.build
    val baos = new ByteArrayOutputStream()
    val ionWriter: IonWriter = ionSystem.newBinaryWriter(baos)

    val ionDatagram: IonDatagram = ionSystem
      .newLoader()
      .load(
        "{name: \"zoe\", address: {city:\"Seattle\", state: \"WA\"}}\n" +
          "{name: \"jan\", age: [20,30], address: {city:\"San Francisco\", state: \"CA\"}}\n" +
          "{name: \"bill\", age: [19,10]}\n"
      )

    val jsonSchemaWithColumnPath: String = {
      """{"type":"struct","fields":[
          |{"name":"age","nullable":true,"type":{"type":"array","elementType":"long","containsNull":false}},
          |{"name":"city","nullable":true,"type":"string","metadata":{"column_path":["address", "city"]}}
        |]}""".stripMargin
    }

    val ionDatagramAfterExtraction: IonDatagram = ionSystem
      .newLoader()
      .load(
        "{city: \"Seattle\"}\n" +
          "{age: [20,30], city: \"San Francisco\"}" +
          "{age: [19,10]}"
      )

    val blobSchema: StructType = StructType(
      StructField("blobColumn",
                  DataTypes.BinaryType,
                  nullable = false,
                  new MetadataBuilder()
                    .putBoolean("is_blob_data", value = true)
                    .build()) :: Nil)
  }
}
