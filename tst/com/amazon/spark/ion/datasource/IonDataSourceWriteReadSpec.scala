// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import java.io.File
import java.sql.{Date, Timestamp}
import java.util.UUID
import com.amazon.spark.ion.{IonOperations, SparkTest}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  BooleanType,
  ByteType,
  DateType,
  DecimalType,
  DoubleType,
  FloatType,
  IntegerType,
  LongType,
  MapType,
  MetadataBuilder,
  ShortType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class IonDataSourceWriteReadSpec extends AnyFlatSpec with Matchers with MockitoSugar with SparkTest {

  behavior of "write and read ion in text format"

  it should "write and read a dataset in text format with integer, double and timestamp types" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00")),
      Row(null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with long, float and date types" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("column_b", DateType)
      ))

    val testData = Seq(Row(1L, 1.23f, Date.valueOf("2018-09-30")),
                       Row(2L, 3.343f, Date.valueOf("2019-09-30")),
                       Row(3L, 58.937f, Date.valueOf("2020-09-30")),
                       Row(null, null, null))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = cast(58.937 as float)").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30\" as Date)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with byte, short and boolean types" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", ShortType),
        StructField("column_a", ByteType),
        StructField("column_b", BooleanType)
      ))

    val testData = Seq(Row(1.toShort, 11.toByte, false),
                       Row(2.toShort, 12.toByte, true),
                       Row(3.toShort, 13.toByte, false),
                       Row(null, null, null))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = cast(12 as byte)").count shouldBe 1
    dfRead.filter("column_b = false").count shouldBe 2

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with string, decimal and binary types" in {
    val f = fixture
    import f._

    import org.apache.spark.sql.functions.udf
    def fromByteArray: Array[Byte] => String = (b => if (b != null) new String(b) else null)

    spark.udf.register("fromByteArrayUdf", fromByteArray)

    val tableSchema = StructType(
      Seq(
        StructField("region_id", StringType),
        StructField("column_a", DecimalType(38, 18)),
        StructField("column_b", BinaryType)
      ))

    val testData = Seq(
      Row("1", new java.math.BigDecimal("11.01"), "One".getBytes),
      Row("2", new java.math.BigDecimal("12.01"), "Two".getBytes),
      Row("3", new java.math.BigDecimal("13.01"), "Three".getBytes),
      Row(null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("region_id = \"1\"").count shouldBe 1
    dfRead.filter("column_a = 12.01").count shouldBe 1
    dfRead.filter("fromByteArrayUdf(column_b) = \"Two\"").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with array type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType, metadata = new MetadataBuilder().putLong("byte_length", 8).build()),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          ArrayType(
            new StructType().add(StructField("column_c_1", TimestampType)).add(StructField("column_c_2", StringType))))
      ))

    val testData = Seq(
      Row(
        1,
        1.23,
        Timestamp.valueOf("2018-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str11"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str12"))
      ),
      Row(
        2,
        3.343,
        Timestamp.valueOf("2019-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str21"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str22"))
      ),
      Row(
        3,
        58.937,
        Timestamp.valueOf("2020-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str31"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str32"))
      ),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c[0].column_c_2 = \"str11\"").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with struct type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType, metadata = new MetadataBuilder().putLong("byte_length", 8).build()),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          new StructType().add(StructField("column_c_1", TimestampType)).add(StructField("column_c_2", StringType)))
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str11")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str21")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str31")),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c.column_c_2 = \"str31\"").count shouldBe 1

    cleanUpFolder(filePath)
  }

  behavior of "write and read ion in binary format"

  it should "write and read a dataset in binary format with integer, double and timestamp types" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 3
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary format with array type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          ArrayType(
            new StructType().add(StructField("column_c_1", TimestampType)).add(StructField("column_c_2", StringType))))
      ))

    val testData = Seq(
      Row(
        1,
        1.23,
        Timestamp.valueOf("2018-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str11"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str2222"))
      ),
      Row(
        2,
        3.343,
        Timestamp.valueOf("2019-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str21"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str2222"))
      ),
      Row(
        3,
        58.937,
        Timestamp.valueOf("2020-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str31"),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str2222"))
      ),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c[0].column_c_2 = \"str11\"").count shouldBe 1
    dfRead.filter("column_c[1].column_c_2 = \"str2222\"").count shouldBe 3

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in text format with struct type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          new StructType().add(StructField("column_c_1", TimestampType)).add(StructField("column_c_2", StringType)))
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str11")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str21")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"), Row(Timestamp.valueOf("2018-09-30 00:00:00"), "str31")),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c.column_c_2 = \"str11\"").count shouldBe 1
    dfRead.filter("column_c.column_c_2 = \"str21\"").count shouldBe 1
    dfRead.filter("column_c.column_c_2 = \"str31\"").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary format with array of struct of array type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType),
        StructField("column_c",
                    ArrayType(
                      new StructType()
                        .add(StructField("column_c_1", TimestampType))
                        .add(StructField("column_c_2", ArrayType(IntegerType)))))
      ))

    val testData = Seq(
      Row(
        1,
        1.23,
        Timestamp.valueOf("2018-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2018-09-30 00:00:00"), Array(111, 112)),
              Row(Timestamp.valueOf("2018-09-30 00:00:00"), Array(121, 9999)))
      ),
      Row(
        2,
        2.23,
        Timestamp.valueOf("2019-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2019-08-30 00:00:00"), Array(211, 212)),
              Row(Timestamp.valueOf("2018-08-30 00:00:00"), Array(221, 9999)))
      ),
      Row(
        3,
        3.23,
        Timestamp.valueOf("2020-09-30 00:00:00"),
        Array(Row(Timestamp.valueOf("2020-09-30 00:00:00"), Array(311, 312)),
              Row(Timestamp.valueOf("2020-08-30 00:00:00"), Array(321, 9999)))
      ),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.show(4, truncate = false)
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 3.23").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c[0].column_c_2[0] = 111").count shouldBe 1
    dfRead.filter("column_c[1].column_c_2[1] = 9999").count shouldBe 3

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary format with struct of struct type and array of struct type" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          new StructType()
            .add(StructField("column_c_1", TimestampType))
            .add(
              StructField("column_c_2",
                          new StructType()
                            .add(StructField("column_c_2_1", StringType))
                            .add(StructField("column_c_2_2", StringType))))
        ),
        StructField(
          "column_d",
          ArrayType(
            new StructType().add(StructField("column_d_1", StringType)).add(StructField("column_d_2", StringType))))
      ))

    val testData = Seq(
      Row(
        1,
        1.23,
        Timestamp.valueOf("2018-09-30 00:00:00"),
        Row(Timestamp.valueOf("2018-09-30 00:00:00"), Row("str11", "str12")),
        Array(Row("str111", "str112"), Row("str121", "str322"))
      ),
      Row(
        2,
        3.343,
        Timestamp.valueOf("2019-09-30 00:00:00"),
        Row(Timestamp.valueOf("2018-09-30 00:00:00"), Row("str21", "str22")),
        Array(Row("str211", "str212"), Row("str221", "str322"))
      ),
      Row(
        3,
        58.937,
        Timestamp.valueOf("2020-09-30 00:00:00"),
        Row(Timestamp.valueOf("2018-09-30 00:00:00"), Row("str31", "str32")),
        Array(Row("str311", "str312"), Row("str321", "str322"))
      ),
      Row(null, null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.show(4, truncate = false)
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c.column_c_2.column_c_2_2 = \"str32\"").count shouldBe 1
    dfRead.filter("column_d[0].column_d_1 = 'str111'").count shouldBe 1
    dfRead.filter("column_d[1].column_d_2 = 'str322'").count shouldBe 3

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary format with Struct of Struct of Array of Struct" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType),
        StructField(
          "column_c",
          new StructType()
            .add(StructField("column_c_1", TimestampType))
            .add(
              StructField("column_c_2",
                          ArrayType(new StructType()
                            .add(StructField("column_c_2_1_1", StringType))
                            .add(StructField("column_c_2_1_2", ArrayType(IntegerType))))))
        )
      )
    )

    val testData = Seq(
      Row(
        1,
        1.23,
        Timestamp.valueOf("2018-09-30 00:00:00"),
        Row(Timestamp.valueOf("2018-09-30 00:00:00"), Array(Row("str1", List(1, 2)), Row("str12", List(11, 22))))
      ),
      Row(
        2,
        2.23,
        Timestamp.valueOf("2019-09-30 00:00:00"),
        Row(Timestamp.valueOf("2019-09-30 00:00:00"), Array(Row("str2", List(3, 4))))
      ),
      Row(
        3,
        3.23,
        Timestamp.valueOf("2020-09-30 00:00:00"),
        Row(Timestamp.valueOf("2020-09-30 00:00:00"), Array(Row("str3", List(5, 6))))
      ),
      Row(null, null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.cache
    dfRead.count() shouldBe 4
    dfRead.show(4, truncate = false)
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 3.23").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    dfRead.filter("column_c.column_c_2[1].column_c_2_1_1 = 'str12'").count shouldBe 1
    dfRead.filter("column_c.column_c_2[1].column_c_2_1_2[1] = 22").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "throw an exception if the passed serialization when writing is not valid" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("region_id_2", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    an[Exception] shouldBe thrownBy {
      df.write
        .format("com.amazon.spark.ion.datasource.IonFileFormat")
        .option("serialization", "binaryinvalid") // invalid serialization type passed
        .option("compression", "none")
        .mode(SaveMode.Overwrite)
        .save(filePath)
    }
    cleanUpFolder(filePath)
  }

  it should "write as Integer type and and read it as Decimal type for Ion Binary Format" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", IntegerType) // write it as an Integer
      ))

    val testData = Seq(
      Row(1, 11),
      Row(2, 22),
      Row(3, 33)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    // read the column column_a as a decimal now
    val tableSchemaRead = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DecimalType(38, 0)) // Read it as a decimal
      ))

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchemaRead))

    dfRead.cache()
    dfRead.count() shouldBe 3
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 11").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary gzip format" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00")),
      Row(null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    val options = Map("compression" -> "gzip")
    IonOperations.saveAsBinary(df, Seq(), options, filePath)

    getDataFiles(new File(filePath)).foreach(f => {
      f.getName.endsWith(".ion.gz") shouldBe true
    })

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  behavior of "write and read ion when dataset is partitioned"

  it should "write and read a dataset in text format with dataset partitioned by field" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq("region_id"), Map(), filePath, SaveMode.Overwrite)

    // read dataframe from root path. it should get back all rows
    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 3
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    // read dataframe from one of the partitioned paths. it should get back rows only for that partition
    val expectedRegion_1_Path = filePath + "/region_id=1"
    val dfReadRegion1 =
      IonOperations.load(spark, Seq(expectedRegion_1_Path), Some(tableSchema))

    dfReadRegion1.count() shouldBe 1 // only one row should be read
    dfReadRegion1.filter("column_a = 1.23").count shouldBe 1
    dfReadRegion1.filter("column_b = cast(\"2018-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read a dataset in binary format with dataset partitioned by field" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType),
        StructField("column_b", TimestampType)
      ))

    val testData = Seq(
      Row(1, 1.23, Timestamp.valueOf("2018-09-30 00:00:00")),
      Row(2, 3.343, Timestamp.valueOf("2019-09-30 00:00:00")),
      Row(3, 58.937, Timestamp.valueOf("2020-09-30 00:00:00"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq("column_a", "region_id"), Map(), filePath, SaveMode.Overwrite)

    // read dataframe from root path. it should get back all rows
    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 3
    dfRead.filter("region_id = 1").count shouldBe 1
    dfRead.filter("column_a = 58.937").count shouldBe 1
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    // read dataframe from one of the partitioned paths. it should get back rows only for that partition
    val expectedRegion_1_Path = filePath + "/column_a=1.23/region_id=1"
    val dfReadRegion1 =
      IonOperations.load(spark, Seq(expectedRegion_1_Path), Some(tableSchema))

    dfReadRegion1.count() shouldBe 1 // only one row should be read
    dfReadRegion1.filter("column_b = cast(\"2018-09-30 00:00:00\" as Timestamp)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  behavior of "Ion fields starting with digits without column_path"

  it should "write and read in text format" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("3d_technology", IntegerType)
      ))

    val testData = Seq(
      Row(1, 3),
      Row(2, 1),
      Row(3, 2),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsText(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("3d_technology = 1").count shouldBe 1
    dfRead.filter("3d_technology = 2").count shouldBe 1
    dfRead.filter("3d_technology = 3").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "write and read in binary format" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("3d_technology", IntegerType)
      ))

    val testData = Seq(
      Row(1, 3),
      Row(2, 1),
      Row(3, 2),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("3d_technology = 1").count shouldBe 1
    dfRead.filter("3d_technology = 2").count shouldBe 1
    dfRead.filter("3d_technology = 3").count shouldBe 1

    cleanUpFolder(filePath)
  }

  behavior of "Ion fields starting with digits using column_path"

  it should "write and read" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("3d_technology", IntegerType),
        StructField("deprecated", StructType(Seq(StructField("2d_technology", IntegerType))))
      ))

    val testData = Seq(
      Row(1, 3, Row(2)),
      Row(2, 1, Row(3)),
      Row(3, 2, Row(1)),
      Row(null, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val readSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("3d_technology",
                    IntegerType,
                    metadata = new MetadataBuilder().putStringArray("column_path", Array("3d_technology")).build()),
        StructField("2d_technology",
                    IntegerType,
                    metadata =
                      new MetadataBuilder().putStringArray("column_path", Array("deprecated", "2d_technology")).build())
      ))

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(readSchema))

    dfRead.count() shouldBe 4
    dfRead.filter("3d_technology = 1").count shouldBe 1
    dfRead.filter("3d_technology = 2").count shouldBe 1
    dfRead.filter("3d_technology = 3").count shouldBe 1

    dfRead.filter("2d_technology = 1").count shouldBe 1
    dfRead.filter("2d_technology = 2").count shouldBe 1
    dfRead.filter("2d_technology = 3").count shouldBe 1

    cleanUpFolder(filePath)
  }

  behavior of "ion with schema mismatch"

  it should "read null on type mismatch in PERMISSIVE" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", IntegerType)
      ))

    val testData = Seq(
      Row(1, 3),
      Row(2, 1),
      Row(3, 2),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val mismatchTableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", StringType)
      ))

    val dfRead =
      IonOperations.load(spark,
                         Seq(filePath),
                         Some(mismatchTableSchema),
                         Map[String, String]("parserMode" -> "PERMISSIVE"))

    dfRead.count() shouldBe 4
    dfRead.filter("column_a IS NULL").count shouldBe 4

    cleanUpFolder(filePath)
  }

  it should "fail when not in PERMISSIVE" in {
    val f = fixture
    import f._
    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", IntegerType)
      ))

    val testData = Seq(
      Row(1, 3),
      Row(2, 1),
      Row(3, 2),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val mismatchTableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", StringType)
      ))

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(mismatchTableSchema))

    dfRead.count() shouldBe 4

    an[Exception] should be thrownBy dfRead.show()

    cleanUpFolder(filePath)
  }

  behavior of "Schema with MapType"

  it should "read successfully Ion Struct as Map" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", StructType(Seq(StructField("map_col", StringType))))
      ))

    val testData = Seq(
      Row(1, Row("bar")),
      Row(2, Row("baz")),
      Row(3, Row(null)),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val mismatchTableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", MapType(StringType, StringType))
      ))

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(mismatchTableSchema))

    /**
      * +---------+----------------+
      * |region_id|        column_a|
      * +---------+----------------+
      * |        1|[map_col -> bar]|
      * |        2|[map_col -> baz]|
      * |        3|    [map_col ->]|
      * |     null|            null|
      * +---------+----------------+
      */
    dfRead.count() shouldBe 4
    dfRead.filter("column_a IS NULL").count shouldBe 1
    dfRead.filter("column_a IS NOT NULL AND column_a['map_col'] IS NULL").count shouldBe 1
    dfRead.filter("column_a['map_col'] IS NOT NULL").count shouldBe 2
    dfRead
      .filter("column_a['map_col'] IS NOT NULL")
      .selectExpr("column_a['map_col']")
      .distinct()
      .count() shouldBe 2

    cleanUpFolder(filePath)
  }

  it should "write Ion Struct successfully as Map" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", MapType(StringType, StructType(Seq(StructField("map_value_field", StringType)))))
      ))

    val testData = Seq(
      Row(1, Map("map_col" -> Row("bar"))),
      Row(2, Map("map_col" -> Row("baz"))),
      Row(3, Map("map_col" -> null)),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(tableSchema))

    /**
      * +---------+------------------+
      * |region_id|          column_a|
      * +---------+------------------+
      * |        1|[map_col -> [bar]]|
      * |        2|[map_col -> [baz]]|
      * |        3|      [map_col ->]|
      * |     null|              null|
      * +---------+------------------+
      */
    dfRead.count() shouldBe 4
    dfRead.filter("column_a IS NULL").count shouldBe 1
    dfRead.filter("column_a IS NOT NULL AND column_a['map_col'] IS NULL").count shouldBe 1
    dfRead.filter("column_a['map_col'] IS NOT NULL").count shouldBe 2
    dfRead.filter("column_a['map_col'].map_value_field IS NOT NULL").count shouldBe 2
    dfRead
      .filter("column_a['map_col'].map_value_field IS NOT NULL")
      .selectExpr("column_a['map_col'].map_value_field")
      .distinct()
      .count() shouldBe 2

    cleanUpFolder(filePath)
  }

  it should "read Ion Struct with repeated field names successfully" in {
    // https://amzn.github.io/ion-docs/docs/spec.html#struct
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", StructType(Seq(StructField("map_col", StringType), StructField("map_col", StringType))))
      ))

    val testData = Seq(
      Row(1, Row("bar", "baz"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq(), Map(), filePath)

    val mismatchTableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", MapType(StringType, StringType))
      ))

    val dfRead =
      IonOperations.load(spark, Seq(filePath), Some(mismatchTableSchema))

    dfRead.count() shouldBe 1
    dfRead.filter("column_a IS NULL").count shouldBe 0
    dfRead.filter("column_a['map_col'] IS NOT NULL").count shouldBe 1
    dfRead
      .filter("column_a['map_col'] IS NOT NULL")
      .selectExpr("size(column_a)")
      .collect()(0)
      .getInt(0) shouldBe 2

    cleanUpFolder(filePath)
  }

  def fixture = new {
    val uuid = UUID.randomUUID().toString
    val filePath = s"/tmp/TestIon_$uuid"
  }

  def cleanUpFolder(folder: String): Unit = {
    val folderPath = new File(folder)
    FileUtils.deleteDirectory(folderPath)
  }

  def getDataFiles(root: java.io.File): Array[java.io.File] = {
    val allFiles = root.listFiles.filter(f => {
      !f.isHidden && !f.getName.contains("_SUCCESS")
    })
    val files = allFiles.filter(!_.isDirectory)
    files ++ allFiles.filter(_.isDirectory).flatMap(getDataFiles)
  }
}
