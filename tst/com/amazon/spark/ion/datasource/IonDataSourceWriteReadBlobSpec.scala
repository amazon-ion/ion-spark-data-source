// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.datasource

import com.amazon.spark.ion.datasource.IonFileFormat.IS_BLOB_DATA_MARKER_METADATA

import java.sql.{Date, Timestamp}
import java.util.UUID
import com.amazon.spark.ion.parser.IonFunctions.from_ion
import com.amazon.spark.ion.{IonOperations, SparkTest}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.io.File

class IonDataSourceWriteReadBlobSpec extends AnyFlatSpec with Matchers with MockitoSugar with SparkTest {

  behavior of "write ion and read back as blob"

  it should "write and read a dataset as blob" in {
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

    // write a test dataset
    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "binary")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    // read it back as blob and validate
    val dfRead = IonOperations.loadWithRawIonData(spark,
                                                  Seq(filePath),
                                                  StructType(
                                                    Seq(
                                                      StructField("column_b", TimestampType)
                                                    )),
                                                  "blobdata")

    dfRead.count() shouldBe 4
    dfRead
      .filter(
        "column_b = cast(\"2020-09-30 00:00:00\" as Timestamp) " +
          "AND blobdata IS NOT NULL")
      .count shouldBe 1
    dfRead.schema.simpleString shouldEqual StructType(
      Seq(
        StructField("blobdata", BinaryType),
        StructField("column_b", TimestampType)
      )).simpleString

    // map blob data to actual schema rows, and validate
    val dfDeserialized = dfRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema))
      .select("fields.*")
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.filter("column_a = 58.937").count shouldBe 1
    dfDeserialized.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    tableSchema.simpleString shouldEqual dfDeserialized.schema.simpleString

    cleanUpFolder(filePath)
  }

  it should "write only blob data and read back and validate" in {
    ionBlobSerializationTest("binary")
  }

  it should "write blob data as Ion text and read back" in {
    ionBlobSerializationTest("text")
  }

  private def ionBlobSerializationTest(serialization: String) = {
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

    // write a ion test dataset
    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "text")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    // read it back as blob
    val dfTestDataSet = IonOperations.loadWithRawIonData(spark,
                                                         Seq(filePath),
                                                         StructType(
                                                           Seq(
                                                             StructField("region_id", LongType)
                                                           )),
                                                         "blobdata")

    // write only the blob back using specified encoding
    val blobDatasetPath = filePath + "-blobdataset"
    dfTestDataSet
      .select("blobdata")
      .write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .option("serialization", serialization)
      .mode(SaveMode.Overwrite)
      .save(blobDatasetPath)

    // read it back and validate it was correctly serialized
    val dfTestDataSetRead = IonOperations.load(spark, Seq(blobDatasetPath), Some(tableSchema))

    dfTestDataSetRead.count() shouldBe 4
    dfTestDataSetRead.filter("region_id = 1").count shouldBe 1
    dfTestDataSetRead.filter("column_a = cast(58.937 as float)").count shouldBe 1
    dfTestDataSetRead.filter("column_b = cast(\"2020-09-30\" as Date)").count shouldBe 1

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "throw an exception when trying to write dataset as a blob using recordAsByteArray option when it has more than one ion binary column in schema" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", IntegerType)
      ))

    val testData = Seq(Row(1L, 1))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "text")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    // read it back as blob
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("region_id", LongType),
          StructField("column_a",
                      BinaryType,
                      metadata = new MetadataBuilder().putBoolean(IS_BLOB_DATA_MARKER_METADATA, value = true).build())
        )),
      "blobdata"
    )

    val blobDatasetPath = filePath + "-blobdataset"

    (the[SparkException] thrownBy {
      dfTestDataSet.write
        .format("com.amazon.spark.ion.datasource.IonFileFormat")
        .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
        .option("serialization", "binary")
        .mode(SaveMode.Overwrite)
        .save(blobDatasetPath)
    }).getCause.getMessage shouldEqual "There can be only one blob data field in the schema. Found: StructField(blobdata,BinaryType,true), StructField(region_id,LongType,true), StructField(column_a,BinaryType,true)"

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  behavior of "write and read ion as blob when dataset is partitioned"

  it should "write and read a dataset as blob with dataset partitioned by field" in {
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
                       Row(3L, 58.937f, Date.valueOf("2020-09-30")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset. This is not partitioned.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob. Assume region_id is the partitioned field
    val dfTestDataSet = IonOperations.loadWithRawIonData(spark,
                                                         Seq(filePath),
                                                         StructType(
                                                           Seq(
                                                             StructField("region_id", LongType)
                                                           )),
                                                         "blobdata")

    // write only the blob back
    val blobDatasetPath = filePath + "-blobdataset"
    val writer = dfTestDataSet
      .select("region_id", "blobdata") // the partition columns is to be selected
      .write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
    writer.partitionBy("region_id") // partition it
    writer.save(blobDatasetPath)

    // read it back and validate it was correctly serialized
    val dfTestDataSetRead = IonOperations.load(spark, Seq(blobDatasetPath), Some(tableSchema))

    dfTestDataSetRead.count() shouldBe 3
    dfTestDataSetRead.filter("region_id = 1").count shouldBe 1
    dfTestDataSetRead.filter("column_a = cast(58.937 as float)").count shouldBe 1
    dfTestDataSetRead.filter("column_b = cast(\"2020-09-30\" as Date)").count shouldBe 1

    // also read back only from one of the partitioned folders, and validate all fields can be read.
    val blobDatasetPathRegion1 = blobDatasetPath + "/region_id=1" // read only from region_id 1 folder
    val dfRegion1 = IonOperations.load(spark, Seq(blobDatasetPathRegion1), Some(tableSchema))
    dfRegion1.count() shouldBe 1
    dfRegion1.filter("region_id = 1").count shouldBe 1
    dfRegion1.filter("column_a = cast(1.23 as float)").count shouldBe 1
    dfRegion1.filter("column_b = cast(\"2018-09-30\" as Date)").count shouldBe 1

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "write and read a dataset as blob with dataset bucket-sorted by field" in {
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
                       Row(3L, 58.937f, Date.valueOf("2020-09-30")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset. This is not bucket-sorted.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob. Assume region_id is the bucket-sort field
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("region_id",
                      LongType,
                      metadata = new MetadataBuilder().putBoolean("is_bucket_key", value = true).build())
        )),
      "blobdata"
    )

    // write only the blob back
    val blobDatasetPath = filePath + "-blobdataset"
    dfTestDataSet
      .select("region_id", "blobdata")
      .write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
      .bucketBy(2, "region_id") // bucket it
      .sortBy("region_id") // sort it
      .option("path", blobDatasetPath)
      .saveAsTable("test_table")

    // read it back and validate it was correctly serialized
    val dfTestDataSetRead = IonOperations.load(spark, Seq(blobDatasetPath), Some(tableSchema))

    dfTestDataSetRead.count() shouldBe 3
    dfTestDataSetRead.filter("region_id = 1").count shouldBe 1
    dfTestDataSetRead.filter("column_a = cast(58.937 as float)").count shouldBe 1
    dfTestDataSetRead.filter("column_b = cast(\"2020-09-30\" as Date)").count shouldBe 1

    val dfTestDataSetRawIonRead = IonOperations.loadWithRawIonData(spark,
                                                                   Seq(filePath),
                                                                   StructType(
                                                                     Seq(
                                                                       StructField("region_id", LongType)
                                                                     )),
                                                                   "blobdata")

    // deserialse the blob
    val dfDeserialized = dfTestDataSetRawIonRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema))
      .select("fields.*")
    dfTestDataSetRead.count() shouldBe 3
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    tableSchema.simpleString shouldEqual dfDeserialized.schema.simpleString

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "write and read a dataset as blob with multi-column dataset" in {
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
                       Row(3L, 58.937f, Date.valueOf("2020-09-30")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob. Assume region_id is the bucket-sort field
    val dfTestDataSet = IonOperations.loadWithRawIonData(spark,
                                                         Seq(filePath),
                                                         StructType(
                                                           Seq(
                                                             StructField("region_id", LongType)
                                                           )),
                                                         "blobdata")

    // write only the blob back
    val blobDatasetPath = filePath + "-blobdataset"
    dfTestDataSet
      .select("region_id", "blobdata")
      .write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
      .save(blobDatasetPath)

    // read it back and validate it was correctly serialized
    val dfTestDataSetRead = IonOperations.load(spark, Seq(blobDatasetPath), Some(tableSchema))

    dfTestDataSetRead.count() shouldBe 3
    dfTestDataSetRead.filter("region_id = 1").count shouldBe 1
    dfTestDataSetRead.filter("column_a = cast(58.937 as float)").count shouldBe 1
    dfTestDataSetRead.filter("column_b = cast(\"2020-09-30\" as Date)").count shouldBe 1

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "read field from the folder path when the actual blob does not contain the folder path column" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("column_b", DateType)
      ))

    val testData = Seq(Row(1, 1.23f, Date.valueOf("2018-09-30")),
                       Row(2, 3.343f, Date.valueOf("2019-09-30")),
                       Row(3, 58.937f, Date.valueOf("2020-09-30")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset. Partition it, so that the actual Ion file only contains the non-partition columns, and the partition column is in the folder path name
    IonOperations.saveAsBinary(df, Seq("region_id"), Map(), filePath, SaveMode.Overwrite)

    // Read it back as blob, and validate that all columns including the partition column can be read
    val dfBlob = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()))), // only request column_a, and not the folder path column
      "blobData"
    )
    dfBlob.cache()
    dfBlob.schema.fields.size shouldBe 3
    dfBlob.schema.fields
      .filter(f => f.name == "region_id")
      .size shouldBe 1 // should read the field region_id from the folder path
    dfBlob.schema.fields.filter(f => f.name == "column_a").size shouldBe 1
    dfBlob.schema.fields.filter(f => f.name == "blobData").size shouldBe 1
    dfBlob.count() shouldBe 3
    dfBlob.filter("region_id = 1").count() shouldBe 1
    dfBlob.filter("column_a = cast(58.937 as float)").count shouldBe 1

    cleanUpFolder(filePath)
  }

  it should "read a dataset as blob with partial schema directly from Scan operation" in {
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

    // write a test dataset
    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "binary")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    // read it back as blob and validate
    val dfRead = IonOperations.loadWithRawIonData(spark,
                                                  Seq(filePath),
                                                  StructType(
                                                    Seq(
                                                      StructField("column_b", TimestampType)
                                                    )),
                                                  "blobdata")

    val expectedDataSchema = Array(StructField("blobdata", BinaryType), StructField("column_b", TimestampType))

    val optimizedPlan = dfRead.queryExecution.optimizedPlan
    optimizedPlan shouldBe a[LogicalRelation]

    val optimizedRelation = optimizedPlan.asInstanceOf[LogicalRelation]
    optimizedRelation.relation shouldBe a[HadoopFsRelation]

    val hadoopFsRelation = optimizedRelation.relation.asInstanceOf[HadoopFsRelation]
    hadoopFsRelation.dataSchema.fields.map(sf => (sf.name, sf.dataType)) should equal(
      expectedDataSchema.map(sf => (sf.name, sf.dataType)))

    dfRead.count() shouldBe 4
    dfRead
      .filter(
        "column_b = cast(\"2020-09-30 00:00:00\" as Timestamp) " +
          "and blobdata IS NOT NULL")
      .count shouldBe 1
    dfRead.schema.simpleString shouldEqual StructType(
      Seq(
        StructField("blobdata", BinaryType),
        StructField("column_b", TimestampType)
      )).simpleString

    // map blob data to actual schema rows, and validate
    val dfDeserialized = dfRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema))
      .select("fields.*")
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.filter("column_a = 58.937").count shouldBe 1
    dfDeserialized.filter("column_b = cast(\"2020-09-30 00:00:00\" as Timestamp)").count shouldBe 1
    tableSchema.simpleString shouldEqual dfDeserialized.schema.simpleString

    cleanUpFolder(filePath)
  }

  it should "read a dataset as blob with partial schema and bucket-spec" in {
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

    val bucketSortJoinColumns = Seq("column_b")

    // write a test dataset
    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "binary")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .bucketBy(2, bucketSortJoinColumns.head)
      .sortBy(bucketSortJoinColumns.head)
      .option("path", filePath)
      .saveAsTable("foo_table")

    val bucketSpec = BucketSpec(2, bucketSortJoinColumns, bucketSortJoinColumns)

    def readWithPartialSchemaAndBucketSpec = {
      IonOperations.loadWithRawIonData(spark,
                                       Seq(filePath),
                                       StructType(tableSchema.filter(sf => bucketSortJoinColumns.contains(sf.name))),
                                       "blobdata",
                                       bucketSpec = Option(bucketSpec))
    }

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val dfReadLeft = readWithPartialSchemaAndBucketSpec
    val dfReadRight = readWithPartialSchemaAndBucketSpec
    val dfRead = dfReadLeft.join(dfReadRight, bucketSortJoinColumns)

    val executedPlan = dfRead.queryExecution.executedPlan

    noException should be thrownBy executedPlan.execute()

    val shufflePlans = executedPlan collect {
      case shuffle: ShuffleExchangeLike => shuffle
    }

    shufflePlans shouldBe empty

    cleanUpFolder(filePath)
  }

  it should "read requested columns from a dataset in blob mode even if blob column is not in the requiredSchema" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType)
      ))

    val testData = Seq(
      Row(1, 1.23),
      Row(2, 3.343),
      Row(3, 58.937),
      Row(null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a test dataset
    df.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("serialization", "binary")
      .option("compression", "none")
      .mode(SaveMode.Overwrite)
      .save(filePath)

    // read it back in blob mode with additional column_a
    var dfRead = IonOperations.loadWithRawIonData(spark,
                                                  Seq(filePath),
                                                  StructType(
                                                    Seq(
                                                      StructField("column_a", DoubleType)
                                                    )),
                                                  "blobdata")

    // select only column_a, such that now Spark will do the read without the blob column
    dfRead = dfRead.select(col("column_a"))

    dfRead.count() shouldBe 4
    dfRead
      .filter("column_a = 1.23")
      .count shouldBe 1
    dfRead.schema.simpleString shouldEqual StructType(
      Seq(StructField("column_a", DoubleType))
    ).simpleString

    cleanUpFolder(filePath)
  }

  it should "throw an exception when trying to read in blob mode and a column requires column_path extraction" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id",
                    LongType,
                    nullable = true,
                    Metadata.fromJson("""{ "column_path": "some_column_path_syntax" }"""))
      ))

    val testData = Seq(Row(1L))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    IonOperations.saveAsBinary(df, Seq.empty, Map.empty, filePath)

    val dfRead = IonOperations.load(spark, Seq(filePath), Option(tableSchema), Map("recordBlobFieldName" -> "blobdata"))

    (the[SparkException] thrownBy {
      dfRead.show
    }).getCause.getMessage shouldEqual "IonFileFormat cannot read data using both column_path extraction and blob mode extraction"

    cleanUpFolder(filePath)
  }

  it should "write partition column to the output in blob mode when it’s not present in the Ion blob" in {

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
                       Row(3L, 58.937f, Date.valueOf("2020-09-30")))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset. This is not partitioned.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob.
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("snapshot_date",
                      StringType,
                      metadata = new MetadataBuilder().putBoolean("is_partition_key", value = true).build())
        )),
      "blobdata"
    )

    // write only the blob back
    val blobDatasetPath = filePath + "-blobdataset"

    // Creating a copy of partition col to keep the column in schema
    // withColumn drops the metadata, so we need to explicitly copy the metdata as well
    val originalMetadata = dfTestDataSet.schema("snapshot_date").metadata
    val dfTestDataSet_updated =
      dfTestDataSet
        .withColumn("snapshot_date",
                    coalesce(col("snapshot_date"), lit("2024-10-18")).as("snapshot_date", originalMetadata))
        .withColumn("snapshot_date_temp",
                    coalesce(col("snapshot_date"), lit("2024-10-18")).as("snapshot_date", originalMetadata))

    val writer = dfTestDataSet_updated.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
    writer.partitionBy("snapshot_date_temp")
    writer.save(blobDatasetPath)

    val tableSchema_withDate = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("column_b", DateType),
        StructField("snapshot_date", StringType)
      ))

    // read it back and validate as blob
    val dfTestDataSetRead = IonOperations.loadWithRawIonData(spark,
                                                             Seq(blobDatasetPath),
                                                             StructType(
                                                               Seq(
                                                                 StructField("snapshot_date", StringType)
                                                               )),
                                                             "blobdata")

    // deserialse the blob
    val dfDeserialized = dfTestDataSetRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema_withDate))
      .select("fields.*")
    dfTestDataSetRead.count() shouldBe 3
    dfDeserialized.filter("snapshot_date = '2024-10-18'").count shouldBe 3
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.filter("column_a = 58.937f").count shouldBe 1

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "write bucket and sort column inside blob when they are already present and metadata is marked with column type" in {

    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("column_b", DateType),
        StructField("bucket_key", StringType),
        StructField("sort_key", StringType)
      ))

    val testData = Seq(
      Row(1L, 1.23f, Date.valueOf("2018-09-30"), "bk1", "sk1"),
      Row(2L, 3.343f, Date.valueOf("2019-09-30"), "bk2", "sk2"),
      Row(3L, 58.937f, Date.valueOf("2020-09-30"), "bk3", "sk3")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob.
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("bucket_key",
                      StringType,
                      metadata = new MetadataBuilder().putBoolean("is_bucket_key", value = true).build()),
          StructField("sort_key",
                      StringType,
                      metadata = new MetadataBuilder().putBoolean("is_sort_key", value = true).build())
        )),
      "blobdata"
    )

    val blobDatasetPath = filePath + "-blobdataset"

    dfTestDataSet.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
      .bucketBy(2, "bucket_key") // bucket it
      .sortBy("sort_key") // sort it
      .option("path", blobDatasetPath)
      .saveAsTable("test_table")

    val dfTestDataSetRawIonRead = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("region_id", LongType),
          StructField("bucket_key", StringType),
          StructField("sort_key", StringType)
        )),
      "blobdata"
    )

    // deserialse the blob
    val dfDeserialized = dfTestDataSetRawIonRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema))
      .select("fields.*")

    dfDeserialized.count() shouldBe 3
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.filter("bucket_key = 'bk1'").count shouldBe 1
    dfDeserialized.filter("sort_key = 'sk1'").count shouldBe 1

    tableSchema.simpleString shouldEqual dfDeserialized.schema.simpleString

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)
  }

  it should "write bucket/sort column inside blob only once when it’s the same column but has both metadata markers" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("column_b", DateType)
      ))

    val testData = Seq(
      Row(1L, 1.23f, Date.valueOf("2018-09-30")),
      Row(2L, 3.343f, Date.valueOf("2019-09-30")),
      Row(3L, 58.937f, Date.valueOf("2020-09-30"))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob.
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("region_id",
                      LongType,
                      metadata = new MetadataBuilder()
                        .putBoolean("is_bucket_key", value = true)
                        .putBoolean("is_sort_key", value = true)
                        .build())
        )),
      "blobdata"
    )
    val blobDatasetPath = filePath + "-blobdataset"

    dfTestDataSet.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
      .bucketBy(2, "region_id") // bucket it
      .sortBy("region_id") // sort it
      .option("path", blobDatasetPath)
      .saveAsTable("test_table")

    val dfTestDataSetRawIonRead = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("region_id", LongType)
        )),
      "blobdata"
    )

    val dfDeserialized = dfTestDataSetRawIonRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema))
      .select("fields.*")

    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.schema.simpleString shouldEqual tableSchema.simpleString

    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)

  }

  it should "overwrite partition column that is present inside blob when metadata is marked with column type" in {

    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("snapshot_date", StringType)
      ))

    val testData = Seq(Row(1L, 1.23f, "2018-09-30"), Row(2L, 3.343f, "2019-09-30"), Row(3L, 58.937f, "2020-09-30"))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    // write a ion test dataset. This is not partitioned.
    IonOperations.saveAsText(df, Seq(), Map(), filePath, SaveMode.Overwrite)

    // read it back as blob.
    val dfTestDataSet = IonOperations.loadWithRawIonData(
      spark,
      Seq(filePath),
      StructType(
        Seq(
          StructField("snapshot_date",
                      StringType,
                      metadata = new MetadataBuilder().putBoolean("is_partition_key", value = true).build())
        )),
      "blobdata"
    )

    // write only the blob back
    val blobDatasetPath = filePath + "-blobdataset"
    // Creating a copy of partition col to keep the column in schema
    // withColumn drops the metadata, so we need to explicitly copy the metdata as well
    val originalMetadata = dfTestDataSet.schema("snapshot_date").metadata
    val dfTestDataSet_updated =
      dfTestDataSet
        .withColumn("snapshot_date", lit("2024-09-30").as("snapshot_date", originalMetadata))
        .withColumn("snapshot_date_temp",
                    coalesce(col("snapshot_date"), lit("2024-09-30")).as("snapshot_date", originalMetadata))

    val writer = dfTestDataSet_updated.write
      .format("com.amazon.spark.ion.datasource.IonFileFormat")
      .option("recordAsByteArray", value = true) // this indicates that the blob is stored as is
      .mode(SaveMode.Overwrite)
    writer.partitionBy("snapshot_date_temp")
    writer.save(blobDatasetPath)

    val tableSchema_withDate = StructType(
      Seq(
        StructField("region_id", LongType),
        StructField("column_a", FloatType, metadata = new MetadataBuilder().putLong("byte_length", 4).build()),
        StructField("snapshot_date", StringType)
      ))

    // read it back and validate as blob
    val dfTestDataSetRead = IonOperations.loadWithRawIonData(spark,
                                                             Seq(blobDatasetPath),
                                                             StructType(
                                                               Seq(
                                                                 StructField("snapshot_date", StringType)
                                                               )),
                                                             "blobdata")

    val dfDeserialized = dfTestDataSetRead
      .withColumn("fields", from_ion(col("blobdata"), tableSchema_withDate))
      .select("fields.*")

    //expect original values to be updated to only 2024-09-30 for all rows
    dfDeserialized.filter("snapshot_date = '2024-09-30'").count shouldBe 3
    cleanUpFolder(filePath)
    cleanUpFolder(blobDatasetPath)

  }

  behavior of "Ion represented as String"

  it should "read Ion String using Ion functions" in {
    val f = fixture
    import f._

    val tableSchema = StructType(
      Seq(
        StructField("data", StringType)
      ))

    val dataSchema = StructType(
      Seq(
        StructField("region_id", IntegerType),
        StructField("column_a", DoubleType, metadata = new MetadataBuilder().putLong("byte_length", 8).build()),
        StructField("column_b", TimestampType)
      ))

    val filterSchema = StructType(Seq(dataSchema.apply("column_b")))

    val testData = Seq(
      Row("{region_id:1, column_a:1.23, column_b:2018-09-30T00:00:00.000-07:00}"),
      Row("{region_id:2, column_a:3.343, column_b:2019-09-30T00:00:00.000-07:00}"),
      Row("{region_id:3, column_a:58.937, column_b:2020-09-30T00:00:00.000-07:00}"),
      Row("null.struct")
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(testData), tableSchema)

    val dfRead = df
      .withColumn("fields", from_ion(col("data"), filterSchema))
      .select("fields.*")

    dfRead.count() shouldBe 4
    dfRead.filter("column_b = cast(\"2020-09-30 00:00:00-07:00\" as Timestamp)").count shouldBe 1
    dfRead.schema.simpleString shouldEqual StructType(
      Seq(
        StructField("column_b", TimestampType)
      )).simpleString

    // map blob data to actual schema rows, and validate
    val dfDeserialized = df
      .withColumn("fields", from_ion(col("data"), dataSchema))
      .select("fields.*")
    dfDeserialized.filter("region_id = 1").count shouldBe 1
    dfDeserialized.filter("column_a = 58.937").count shouldBe 1
    dfDeserialized.filter("column_b = cast(\"2020-09-30 00:00:00-07:00\" as Timestamp)").count shouldBe 1
    dataSchema.simpleString shouldEqual dfDeserialized.schema.simpleString

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
}
