// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.spark.ion.optimizer

import com.amazon.spark.ion.datasource.IonOptions.ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY
import com.amazon.spark.ion.parser.IonFunctions.from_ion
import com.amazon.spark.ion.parser.IonToStructs
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, GetStructField}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
  * The behavior of IonColumnUnzipUtils methods should be the same for various input combination (DataType + Encoding),
  * when the PruneIonToStructs optimization is enabled
  * This trait allows testing the common behavior for these inputs.
  *
  * https://www.scalatest.org/user_guide/sharing_tests
  */
trait IonUnzipOptimizerBehavior extends Matchers with BeforeAndAfterEach {
  this: AnyFlatSpec =>

  private val testTempDir = "/tmp/testUnzip"
  private implicit var spark: SparkSession = _
  private implicit var sparkContext: SparkContext = _
  private val fromIonOptions = Map[String, String]("ignoreIonConversionErrors" -> "true")

  override def afterEach(): Unit = {
    val directory = new File(testTempDir)
    FileUtils.deleteDirectory(directory)
    cleanupAnyExistingSession()
    spark = null
  }

  private def cleanupAnyExistingSession(): Unit = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (session.isDefined) {
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def initSpark(staticConfToSet: Option[Map[String, Any]]): Unit = {
    if (spark === null) {
      cleanupAnyExistingSession()

      val sparkBuilder =
        SparkSession
          .builder()
          .master("local[2]")
          .appName("SparkFunSuite")
          .config("spark.driver.host", "127.0.0.1")

      val confMap = staticConfToSet.getOrElse(Map.empty[String, Any])

      // Check for the enableIonUnzipOptimizer flag
      val enableIonUnzipOptimizer = confMap.get(ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY).exists {
        case value: Boolean => value
        case value: String  => value.toBoolean
        case _              => false
      }

      // Temp. Remove the custom flag from the map to avoid setting it directly in SparkConf
      val filteredConfMap = confMap - ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY

      filteredConfMap.foreach {
        case (key, value: Boolean) => sparkBuilder.config(key, value)
        case (key, value: Long)    => sparkBuilder.config(key, value)
        case (key, value: Double)  => sparkBuilder.config(key, value)
        case (key, value: String)  => sparkBuilder.config(key, value)
        case (key, value)          => println(s"Unsupported type ${value.getClass.getName} for key $key")
      }

      // Add the IonUnzipOptimizerRuleSqlExtension depending on if IonUnzipOptimizer Flag is enabled.
      if (enableIonUnzipOptimizer) {
        sparkBuilder.config("spark.sql.extensions", classOf[IonUnzipOptimizerRuleSqlExtension].getName)
      }

      spark = sparkBuilder.getOrCreate()
      sparkContext = spark.sparkContext
    }
  }

  def initDataFrame(originalData: Seq[Row],
                    originalDataSchema: StructType,
                    zippedSchema: StructType,
                    zippedColumn: String): DataFrame = {
    val originalDf: Dataset[Row] = spark.createDataFrame(sparkContext.parallelize(originalData), originalDataSchema)
    val zippedFields = zippedSchema.fieldNames.toSet
    val inputCols: Set[String] = originalDf.schema.fieldNames.toSet
    val otherAttrs: Set[String] = zippedFields.diff(inputCols)
    val otherSchema: StructType = zippedSchema.apply(otherAttrs)
    originalDf
      .withColumn("other_attrs", from_ion(originalDf(zippedColumn), otherSchema, fromIonOptions))
      .select(expr("*"), expr("other_attrs.*"))
      .drop("other_attrs")
  }

  def assertAndGetFilterExecs(df: DataFrame, expectedLength: Int): Seq[FilterExec] = {
    val filterExecs = df.queryExecution.executedPlan.collect {
      case filter: FilterExec => filter
    }
    require(filterExecs.length == expectedLength,
            s"Expected $expectedLength FilterExecs, but found ${filterExecs.length}")
    filterExecs
  }

  def getFilterExecSchema(df: DataFrame, conditionExtractor: FilterExec => StructType): StructType = {
    val filterExecs = assertAndGetFilterExecs(df, 1)
    conditionExtractor(filterExecs.head)
  }

  def assertFilterLeftConditionChildSchemaLength(df: DataFrame, expectedLength: Int): Unit = {
    val schema = getFilterExecSchema(df,
                                     exec =>
                                       exec.condition
                                         .asInstanceOf[EqualTo]
                                         .left
                                         .asInstanceOf[GetStructField]
                                         .child
                                         .asInstanceOf[IonToStructs]
                                         .schema)
    require(schema.length == expectedLength, s"Expected schema length $expectedLength, but found ${schema.length}")
  }

  def assertFilterLeftAndRightConditionChildSchemaLength(df: DataFrame, expectedLength: Int): Unit = {
    val schemaLeft = getFilterExecSchema(df,
                                         exec =>
                                           exec.condition
                                             .asInstanceOf[And]
                                             .left
                                             .asInstanceOf[EqualTo]
                                             .left
                                             .asInstanceOf[GetStructField]
                                             .child
                                             .asInstanceOf[IonToStructs]
                                             .schema)
    val schemaRight = getFilterExecSchema(df,
                                          exec =>
                                            exec.condition
                                              .asInstanceOf[And]
                                              .right
                                              .asInstanceOf[EqualTo]
                                              .left
                                              .asInstanceOf[GetStructField]
                                              .child
                                              .asInstanceOf[IonToStructs]
                                              .schema)

    require(schemaLeft.length == expectedLength,
            s"Expected left schema length $expectedLength, but found ${schemaLeft.length}")
    require(schemaRight.length == expectedLength,
            s"Expected right schema length $expectedLength, but found ${schemaRight.length}")
  }

  def unzip(originalData: Seq[Row],
            originalDataSchema: StructType,
            zippedSchema: StructType,
            zippedColumn: String): Unit = {

    it should "unzip only required Ion column when filtering on zipped Ion attribute with optimization enabled" in {
      initSpark(Option(Map(ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY -> true)))
      initDataFrame(originalData, originalDataSchema, zippedSchema, zippedColumn)
        .createOrReplaceTempView("table")
      val filteredDf = spark.sql("select marketplace_id from table where version == 2")
      assertFilterLeftConditionChildSchemaLength(filteredDf, 1)
      filteredDf.show()
    }

    it should "unzip only required Ion columns, when filtering on more than one zipped Ion attributes, with optimization enabled" in {
      initSpark(Option(Map(ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY -> true)))
      initDataFrame(originalData, originalDataSchema, zippedSchema, zippedColumn)
        .createOrReplaceTempView("table")
      val filteredDf = spark.sql("select marketplace_id from table where version == 2 and child_customer_id == 1")
      assertFilterLeftAndRightConditionChildSchemaLength(filteredDf, 1)
      filteredDf.show()
    }

    it should "unzip all Ion columns when filtering on zipped Ion attribute with optimization disabled" in {

      // ION_UNZIP_OPTIMIZER_SPARK_SQL_EXTENSION_CONF_KEY is defaulted to false
      initSpark(Option.empty)
      initDataFrame(originalData, originalDataSchema, zippedSchema, zippedColumn)
        .createOrReplaceTempView("table")
      val filteredDf = spark.sql("select marketplace_id from table where version == 2")
      assertFilterLeftConditionChildSchemaLength(filteredDf, 4)
      filteredDf.show()
    }
  }
}
